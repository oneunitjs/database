# Failover Layer Implementation Plan

> **Status:** Draft | **Priority:** P0/P1 | **Target:** Weeks 1-12

Detailed implementation plans for 9 unimplemented Failover Layer features from working-issues.md section 10.

---

## 📋 Overview

9 features to implement:
1. Persistent failover state storage interface (P0, 3-5d)
2. Failover event history (P0, 3-4d)
3. Configurable failure detection thresholds (P0, 2-3d)
4. Health-check failure classification (P0, 2-3d)
5. Failover state recovery after restart (P0, 3-5d)
6. Failover hooks and callbacks (P1, 3-4d)
7. Connection invalidation after failover (P0, 3-5d)
8. Connection pool reconnection (P1, 3-4d)
9. Fencing integration hooks (P1, 5-7d)

**Total Effort:** ~32-42 days

---

## 1. Persistent Failover State Storage Interface

**Priority:** P0 | **Effort:** 3-5 days | **Files:** `src/failover/failover-state-store.ts`

### Architecture
```
FailoverStateStore Interface
├── PgFailoverStateStore (PostgreSQL)
└── MemoryFailoverStateStore (Testing)

Database Tables:
├── _database_ha_state (current state per shard)
├── _database_ha_state_versions (audit trail)
└── _database_ha_locks (distributed locks)
```

### Implementation

#### Interface
```typescript
// src/failover/failover-state-store.ts
export interface FailoverStateStore {
    loadState(shardId: string): Promise<ShardFailoverState | null>;
    saveState(shardId: string, state: ShardFailoverState, expectedVersion: number): Promise<boolean>;
    transitionState(shardId: string, fromState: FailoverState, toState: FailoverState, reason: string, context: FailoverContext): Promise<boolean>;
    listStates(): Promise<Map<string, ShardFailoverState>>;
    acquireLock(shardId: string, ttlMs: number): Promise<boolean>;
    releaseLock(shardId: string): Promise<void>;
    getStateHistory(shardId: string, limit?: number): Promise<FailoverStateVersion[]>;
}
```

#### PostgreSQL Schema
```sql
CREATE TABLE _database_ha_state (
    shard_id TEXT NOT NULL PRIMARY KEY,
    state_json JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT NOT NULL
);

CREATE TABLE _database_ha_state_versions (
    id BIGSERIAL PRIMARY KEY,
    shard_id TEXT NOT NULL,
    state_json JSONB NOT NULL,
    version BIGINT NOT NULL,
    previous_version BIGINT NOT NULL,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT
);

CREATE TABLE _database_ha_locks (
    lock_name TEXT NOT NULL PRIMARY KEY,
    locked_by TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lock_expiry TIMESTAMPTZ NOT NULL
);
```

#### PostgreSQL Implementation
```typescript
// src/failover/pg-failover-state-store.ts
export class PgFailoverStateStore implements FailoverStateStore {
    constructor(private readonly pool: Pool, private readonly logger: Logger) {}

    async loadState(shardId: string): Promise<ShardFailoverState | null> {
        const result = await this.pool.query(
            `SELECT state_json, version FROM _database_ha_state WHERE shard_id = $1`,
            [shardId]
        );
        if (result.rows.length === 0) return null;
        return { ...result.rows[0].state_json, _version: result.rows[0].version };
    }

    async saveState(shardId: string, state: ShardFailoverState, expectedVersion: number): Promise<boolean> {
        const result = await this.pool.query(
            `UPDATE _database_ha_state SET state_json = $1, version = version + 1, updated_at = NOW()
             WHERE shard_id = $2 AND version = $3 RETURNING version`,
            [state, shardId, expectedVersion]
        );
        if (result.rowCount === 0) return false;
        await this.logVersionChange(shardId, state, result.rows[0].version - 1, result.rows[0].version);
        return true;
    }

    async transitionState(shardId: string, fromState: FailoverState, toState: FailoverState, reason: string, context: FailoverContext): Promise<boolean> {
        const result = await this.pool.query(
            `UPDATE _database_ha_state SET state_json = state_json || $1::jsonb, version = version + 1, updated_at = NOW()
             WHERE shard_id = $2 AND (state_json->>'state') = $3 RETURNING version`,
            [JSON.stringify({ state: toState, updatedAt: context.timestamp }), shardId, fromState]
        );
        if (result.rowCount === 0) return false;
        await this.logVersionChange(shardId, { ...(await this.loadState(shardId))!, state: toState },
                                      result.rows[0].version - 1, result.rows[0].version, reason, context);
        return true;
    }

    async acquireLock(shardId: string, ttlMs: number): Promise<boolean> {
        const lockName = `ha_lock_${shardId}`;
        const expiry = new Date(Date.now() + ttlMs);
        const result = await this.pool.query(
            `INSERT INTO _database_ha_locks (lock_name, locked_by, locked_at, lock_expiry)
             VALUES ($1, $2, NOW(), $3)
             ON CONFLICT (lock_name) DO UPDATE SET locked_by = EXCLUDED.locked_by, locked_at = EXCLUDED.locked_at, lock_expiry = EXCLUDED.lock_expiry
             WHERE _database_ha_locks.lock_expiry < NOW() RETURNING *`,
            [lockName, `worker_${process.pid}`, expiry]
        );
        return result.rowCount > 0;
    }

    async releaseLock(shardId: string): Promise<void> {
        const lockName = `ha_lock_${shardId}`;
        await this.pool.query('DELETE FROM _database_ha_locks WHERE lock_name = $1', [lockName]);
    }

    private async logVersionChange(shardId: string, state: ShardFailoverState, prevVersion: number, newVersion: number, reason?: string, context?: FailoverContext): Promise<void> {
        await this.pool.query(
            `INSERT INTO _database_ha_state_versions (shard_id, state_json, version, previous_version, changed_by, reason)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [shardId, state, newVersion, prevVersion, context?.initiatedBy || 'system', reason]
        );
    }
}
```

### Integration with FailoverManager
```typescript
// src/failover/failover-manager.ts - Add stateStore support
constructor(options: FailoverManagerOptions) {
    this.stateStore = options.stateStore || new PgFailoverStateStore(options.primaryPool, options.logger);
}

async initialize(): Promise<void> {
    const savedState = await this.stateStore.loadState(this.shardId);
    this.state = savedState || createInitialFailoverState(this.config);
    this.startHealthChecks();
}

async setState(newState: ShardFailoverState, reason?: string): Promise<void> {
    const oldState = this.state;
    const success = await this.stateStore.transitionState(this.shardId, oldState.state, newState.state, reason || 'state transition', { timestamp: new Date(), initiatedBy: 'failover-manager' });
    if (!success) this.logger.warn({ shardId: this.shardId, from: oldState.state, to: newState.state }, 'State transition version conflict');
    this.state = newState;
    this.onStateChange?.(newState);
}
```

### Acceptance Criteria
- [ ] `FailoverStateStore` interface defined
- [ ] PostgreSQL implementation with optimistic concurrency
- [ ] Memory implementation for testing
- [ ] Integration with `FailoverManager`
- [ ] Database schema and migrations
- [ ] Lock mechanism prevents concurrent modifications
- [ ] Version history maintained
- [ ] Unit tests (100% coverage)

---

## 2. Failover Event History

**Priority:** P0 | **Effort:** 3-4 days | **Files:** `src/failover/failover-event-store.ts`

### Architecture
```
FailoverEventStore Interface
├── PgFailoverEventStore (PostgreSQL)
└── Memory implementation (Testing)

Database Table: _database_ha_events
```

### Implementation

#### Interface
```typescript
// src/failover/failover-event-store.ts
export type FailoverEventType = 'state_change' | 'health_check_started' | 'health_check_failed' | 
    'health_check_succeeded' | 'failover_started' | 'failover_completed' | 'failover_failed' | 
    'promotion_started' | 'promotion_completed' | 'connection_invalidated' | 'lock_acquired' | 
    'lock_released' | 'replica_added' | 'replica_removed';

export interface FailoverEvent {
    id: string;
    timestamp: Date;
    type: FailoverEventType;
    shardId: string;
    nodeId?: string;
    details: Record<string, unknown>;
    severity: 'debug' | 'info' | 'warning' | 'error';
    previousState?: FailoverState;
    relatedState?: FailoverState;
}

export interface FailoverEventStore {
    recordEvent(event: Omit<FailoverEvent, 'id' | 'timestamp'>): Promise<FailoverEvent>;
    queryEvents(filter: FailoverEventFilter): Promise<{ events: FailoverEvent[]; total: number }>;
    getEventsForShard(shardId: string, limit?: number): Promise<FailoverEvent[]>;
    getLatestEvents(limit: number): Promise<FailoverEvent[]>;
    cleanupEvents(retentionDays: number): Promise<number>;
    getStatistics(filter?: FailoverEventFilter): Promise<FailoverEventStatistics>;
}
```

#### PostgreSQL Schema
```sql
CREATE TABLE _database_ha_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    type TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    node_id TEXT,
    details JSONB NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    previous_state TEXT,
    new_state TEXT
);

CREATE INDEX idx_ha_events_shard_id ON _database_ha_events(shard_id);
CREATE INDEX idx_ha_events_type ON _database_ha_events(type);
CREATE INDEX idx_ha_events_timestamp ON _database_ha_events(timestamp);
CREATE INDEX idx_ha_events_severity ON _database_ha_events(severity);
```

#### PostgreSQL Implementation
```typescript
// src/failover/pg-failover-event-store.ts
export class PgFailoverEventStore implements FailoverEventStore {
    constructor(private readonly pool: Pool) {}

    async recordEvent(event: Omit<FailoverEvent, 'id' | 'timestamp'>): Promise<FailoverEvent> {
        const fullEvent: FailoverEvent = { ...event, id: crypto.randomUUID(), timestamp: new Date() };
        await this.pool.query(
            `INSERT INTO _database_ha_events (event_id, timestamp, type, shard_id, node_id, details, severity, previous_state, new_state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
            [fullEvent.id, fullEvent.timestamp, fullEvent.type, fullEvent.shardId, fullEvent.nodeId,
             fullEvent.details, fullEvent.severity, fullEvent.previousState, fullEvent.relatedState]
        );
        return fullEvent;
    }

    async queryEvents(filter: FailoverEventFilter): Promise<{ events: FailoverEvent[]; total: number }> {
        const { shardId, type, severity, startTime, endTime, limit = 100, offset = 0 } = filter;
        let query = `SELECT id, event_id as "eventId", timestamp, type, shard_id as "shardId", node_id as "nodeId", 
                       details, severity, previous_state as "previousState", new_state as "relatedState"
                    FROM _database_ha_events`;
        const conditions: string[] = []; const params: unknown[] = [];
        if (shardId) { conditions.push(`shard_id = $${params.length + 1}`); params.push(shardId); }
        if (type) { conditions.push(`type = $${params.length + 1}`); params.push(type); }
        if (severity) { conditions.push(`severity = $${params.length + 1}`); params.push(severity); }
        if (startTime) { conditions.push(`timestamp >= $${params.length + 1}`); params.push(startTime); }
        if (endTime) { conditions.push(`timestamp <= $${params.length + 1}`); params.push(endTime); }
        if (conditions.length > 0) query += ` WHERE ${conditions.join(' AND ')}`;
        query += ` ORDER BY timestamp DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        params.push(limit, offset);
        let countQuery = `SELECT COUNT(*) FROM _database_ha_events`;
        if (conditions.length > 0) countQuery += ` WHERE ${conditions.join(' AND ')}`;
        const [countResult, eventsResult] = await Promise.all([
            this.pool.query(countQuery, params.slice(0, -2)),
            this.pool.query(query, params)
        ]);
        return { events: eventsResult.rows as FailoverEvent[], total: parseInt(countResult.rows[0].count, 10) };
    }

    async cleanupEvents(retentionDays: number): Promise<number> {
        const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
        const result = await this.pool.query('DELETE FROM _database_ha_events WHERE timestamp < $1 RETURNING id', [cutoff]);
        return result.rowCount;
    }
}
```

### Integration
```typescript
// In FailoverManager constructor
this.eventStore = options.eventStore;

// Record events at key points
private async recordEvent(type: FailoverEventType, details: Record<string, unknown> = {}, 
                          severity: 'debug' | 'info' | 'warning' | 'error' = 'info',
                          previousState?: FailoverState, newState?: FailoverState): Promise<void> {
    if (!this.eventStore) return;
    await this.eventStore.recordEvent({ type, shardId: this.shardId, nodeId: this.config.nodeId, 
                                         details, severity, previousState, relatedState: newState });
}

// In setState
await this.recordEvent('state_change', { reason, ...newState }, 'info', oldState.state, newState.state);
```

### Acceptance Criteria
- [ ] Interface and PostgreSQL implementation
- [ ] All query methods working
- [ ] Statistics aggregation
- [ ] Integration with FailoverManager
- [ ] Events recorded for all state changes
- [ ] Unit tests (100% coverage)

---

## 3. Configurable Failure Detection Thresholds

**Priority:** P0 | **Effort:** 2-3 days | **Files:** `src/failover/health-check-tracker.ts`

### Implementation

#### Configuration
```typescript
// src/failover/failover-types.ts
export interface FailoverConfig {
    // ...existing
    consecutiveFailureThreshold: number;          // Default: 3
    unhealthyDurationThresholdMs: number;        // Default: 30_000 (30s)
    consecutiveSuccessThreshold: number;          // Default: 2
    healthCheckRetryIntervalMs: number;           // Default: 1_000
    unhealthyHealthCheckMultiplier: number;       // Default: 0.5
    nodeThresholds: Record<string, NodeFailureThresholds>;
}

export interface NodeFailureThresholds {
    consecutiveFailures?: number;
    unhealthyDurationMs?: number;
    consecutiveSuccesses?: number;
    healthCheckIntervalMs?: number;
}
```

#### HealthCheckTracker
```typescript
// src/failover/health-check-tracker.ts
export interface NodeHealthStatus {
    nodeId: string;
    consecutiveFailures: number;
    consecutiveSuccesses: number;
    lastFailureTime: Date | null;
    lastSuccessTime: Date | null;
    currentStatus: 'healthy' | 'degraded' | 'unhealthy';
    statusSince: Date;
}

export class HealthCheckTracker {
    private statuses = new Map<string, NodeHealthStatus>();

    constructor(private config: FailoverConfig, private logger: RequiredFailoverLogger) {}

    recordFailure(nodeId: string): NodeHealthStatus {
        let status = this.statuses.get(nodeId) || this.createStatus(nodeId);
        const nodeThresholds = this.config.nodeThresholds[nodeId] || {};
        const threshold = nodeThresholds.consecutiveFailures || this.config.consecutiveFailureThreshold;
        
        status.consecutiveFailures++;
        status.consecutiveSuccesses = 0;
        status.lastFailureTime = new Date();

        if (status.consecutiveFailures >= threshold) {
            status.currentStatus = 'unhealthy';
            status.statusSince = new Date();
            this.logger.info({ nodeId, consecutiveFailures: status.consecutiveFailures }, 'Node marked unhealthy');
        } else if (status.consecutiveFailures >= threshold / 2) {
            if (status.currentStatus === 'healthy') {
                status.currentStatus = 'degraded';
                status.statusSince = new Date();
                this.logger.warn({ nodeId, consecutiveFailures: status.consecutiveFailures }, 'Node degraded');
            }
        }
        this.statuses.set(nodeId, status);
        return status;
    }

    recordSuccess(nodeId: string): NodeHealthStatus {
        let status = this.statuses.get(nodeId) || this.createStatus(nodeId);
        const nodeThresholds = this.config.nodeThresholds[nodeId] || {};
        const successThreshold = nodeThresholds.consecutiveSuccesses || this.config.consecutiveSuccessThreshold;
        const durationThreshold = nodeThresholds.unhealthyDurationMs || this.config.unhealthyDurationThresholdMs;

        status.consecutiveSuccesses++;
        status.consecutiveFailures = 0;
        status.lastSuccessTime = new Date();

        if (status.currentStatus === 'degraded' && status.consecutiveSuccesses >= successThreshold) {
            status.currentStatus = 'healthy';
            status.statusSince = new Date();
            this.logger.info({ nodeId, consecutiveSuccesses: status.consecutiveSuccesses }, 'Node recovered to healthy');
        } else if (status.currentStatus === 'unhealthy' && status.consecutiveSuccesses >= successThreshold) {
            if (!status.lastFailureTime || (Date.now() - status.lastFailureTime.getTime()) >= durationThreshold) {
                status.currentStatus = 'healthy';
                status.statusSince = new Date();
                this.logger.info({ nodeId, consecutiveSuccesses: status.consecutiveSuccesses }, 'Node recovered');
            }
        }
        this.statuses.set(nodeId, status);
        return status;
    }

    shouldInitiateFailover(nodeId: string): boolean {
        const status = this.statuses.get(nodeId);
        if (!status) return false;
        const nodeThresholds = this.config.nodeThresholds[nodeId] || {};
        const failureThreshold = nodeThresholds.consecutiveFailures || this.config.consecutiveFailureThreshold;
        const durationThreshold = nodeThresholds.unhealthyDurationMs || this.config.unhealthyDurationThresholdMs;
        return status.consecutiveFailures >= failureThreshold &&
               status.lastFailureTime &&
               (Date.now() - status.lastFailureTime.getTime()) >= durationThreshold;
    }

    getEffectiveHealthCheckInterval(nodeId: string): number {
        const status = this.statuses.get(nodeId);
        if (!status) return this.config.healthCheckIntervalMs;
        const nodeThresholds = this.config.nodeThresholds[nodeId] || {};
        const baseInterval = nodeThresholds.healthCheckIntervalMs || this.config.healthCheckIntervalMs;
        if (status.currentStatus === 'unhealthy') {
            return Math.round(baseInterval * (nodeThresholds.unhealthyHealthCheckMultiplier || this.config.unhealthyHealthCheckMultiplier));
        }
        return baseInterval;
    }

    private createStatus(nodeId: string): NodeHealthStatus {
        return { nodeId, consecutiveFailures: 0, consecutiveSuccesses: 0, lastFailureTime: null,
                 lastSuccessTime: null, currentStatus: 'healthy', statusSince: new Date() };
    }
}
```

### Integration
```typescript
// In FailoverManager
private healthTracker: HealthCheckTracker;
constructor(options: FailoverManagerOptions) {
    this.healthTracker = new HealthCheckTracker(options.config, options.logger);
}

// In performHealthCheck
for (const node of nodes) {
    try {
        const health = await this.checkNodeHealth(node);
        if (health.healthy) {
            this.healthTracker.recordSuccess(node.id);
        } else {
            this.healthTracker.recordFailure(node.id);
        }
        if (this.healthTracker.shouldInitiateFailover(node.id)) {
            await this.initiateFailover();
        }
    } catch (error) {
        this.healthTracker.recordFailure(node.id);
    }
}
```

### Acceptance Criteria
- [ ] Threshold configuration added
- [ ] Per-node threshold overrides
- [ ] Dynamic health check intervals
- [ ] State transitions based on thresholds
- [ ] Integration with FailoverManager
- [ ] Unit tests (100% coverage)

---

## 4. Health-Check Failure Classification

**Priority:** P0 | **Effort:** 2-3 days | **Files:** `src/failover/health-classifier.ts`

### Implementation

#### Classifier
```typescript
// src/failover/health-classifier.ts
export type HealthCheckFailureCategory = 'connection_refused' | 'connection_timeout' | 'connection_reset' | 
    'authentication_failed' | 'query_timeout' | 'query_error' | 'network_unreachable' | 
    'dns_resolution_failed' | 'server_error' | 'unknown';

export interface ClassifiedFailure {
    category: HealthCheckFailureCategory;
    severity: 'transient' | 'persistent' | 'critical';
    retryable: boolean;
    failoverImmediately: boolean;
    description: string;
}

export class HealthCheckClassifier {
    private categoryMap: Map<HealthCheckFailureCategory, Omit<ClassifiedFailure, 'description'>> = new Map([
        ['connection_refused', { category: 'connection_refused', severity: 'critical', retryable: true, failoverImmediately: true }],
        ['connection_timeout', { category: 'connection_timeout', severity: 'transient', retryable: true, failoverImmediately: false }],
        ['authentication_failed', { category: 'authentication_failed', severity: 'critical', retryable: false, failoverImmediately: false }],
        ['server_error', { category: 'server_error', severity: 'critical', retryable: false, failoverImmediately: true }],
        ['unknown', { category: 'unknown', severity: 'transient', retryable: true, failoverImmediately: false }]
    ]);

    classify(failure: HealthCheckFailure): ClassifiedFailure {
        const category = this.categorizeError(failure.error);
        const base = this.categoryMap.get(category)!;
        return { ...base, category, description: this.describeFailure(failure, category) };
    }

    private categorizeError(error: Error): HealthCheckFailureCategory {
        const message = error.message.toLowerCase();
        const code = (error as NodeJS.ErrnoException).code;
        if (code === 'ECONNREFUSED') return 'connection_refused';
        if (code === 'ETIMEDOUT') return 'connection_timeout';
        if (code === 'ECONNRESET') return 'connection_reset';
        if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') return 'dns_resolution_failed';
        if (code === 'ENETUNREACH' || code === 'EHOSTUNREACH') return 'network_unreachable';
        if (message.includes('authentication failed') || message.includes('28p01') || message.includes('28000')) return 'authentication_failed';
        if (message.includes('connection refused')) return 'connection_refused';
        if (message.includes('connection reset')) return 'connection_reset';
        if (message.includes('connection timed out') || message.includes('timeout')) return 'connection_timeout';
        if (message.includes('query timeout') || message.includes('statement timeout')) return 'query_timeout';
        if (message.includes('server error') || message.includes('server closed')) return 'server_error';
        return 'unknown';
    }

    shouldFailoverImmediately(classification: ClassifiedFailure): boolean {
        return classification.failoverImmediately;
    }
}
```

### Integration
```typescript
// In FailoverManager
private classifier: HealthCheckClassifier;
constructor(options: FailoverManagerOptions) {
    this.classifier = new HealthCheckClassifier();
}

// In performHealthCheck
try {
    await this.checkNodeHealth(node);
    this.healthTracker.recordSuccess(node.id);
} catch (error) {
    const failure: HealthCheckFailure = { error: error instanceof Error ? error : new Error(String(error)),
                                          nodeId: node.id, shardId: this.shardId, timestamp: new Date(), durationMs: Date.now() - startTime };
    const classification = this.classifier.classify(failure);
    this.healthTracker.recordFailure(node.id, error, failure.durationMs);
    this.logger[classification.severity]({ nodeId: node.id, classification }, classification.description);
    if (classification.failoverImmediately) {
        await this.initiateFailover();
    }
}
```

### Acceptance Criteria
- [ ] Classifier implemented for all error types
- [ ] Severity, retryable, failoverImmediately flags
- [ ] Integration with health check tracker
- [ ] Integration with FailoverManager
- [ ] Unit tests for all error patterns
- [ ] Unit tests (100% coverage)

---

## 5. Failover State Recovery After Restart

**Priority:** P0 | **Effort:** 3-5 days | **Files:** `src/failover/failover-recovery.ts`

### Architecture
```
FailoverRecoveryManager
├── Recover state from persistent store
├── Validate state structure and version
├── Handle concurrent recovery attempts (locking)
└── Reconcile state mismatches
```

### Implementation

#### Recovery Manager
```typescript
// src/failover/failover-recovery.ts
export interface RecoveryResult {
    success: boolean;
    state?: ShardFailoverState;
    version: number;
    actions: RecoveryAction[];
    warnings: string[];
    errors: string[];
    durationMs: number;
}

export class FailoverRecoveryManager {
    constructor(private stateStore: FailoverStateStore, private logger: RequiredFailoverLogger,
                private options: Partial<FailoverRecoveryOptions> = {}) {}

    async recoverState(shardId: string): Promise<RecoveryResult> {
        const startTime = Date.now(); const actions: RecoveryAction[] = []; const warnings: string[] = []; const errors: string[] = [];
        try {
            // Acquire recovery lock
            actions.push({ type: 'lock_acquired', timestamp: new Date(), description: 'Acquiring recovery lock', details: { shardId } });
            const lockAcquired = await this.acquireRecoveryLock(shardId);
            if (!lockAcquired) throw new Error('Failed to acquire recovery lock');

            // Load state
            actions.push({ type: 'state_loaded', timestamp: new Date(), description: 'Loading failover state', details: { shardId } });
            const state = await this.stateStore.loadState(shardId);
            if (!state) return { success: true, version: 0, actions, warnings: ['No existing state found'], errors, durationMs: Date.now() - startTime };

            // Validate
            const validation = await this.validateState(state, shardId);
            for (const issue of validation.issues) {
                if (issue.severity === 'error') errors.push(issue.description);
                else warnings.push(issue.description);
            }
            if (!this.options.forceRecovery && !validation.valid) throw new Error(`State validation failed: ${errors.join(', ')}`);

            // Reconcile if needed
            if (this.options.autoReconcile && validation.issues.length > 0) {
                const reconciled = await this.reconcileState(state, validation, shardId);
                if (reconciled) {
                    await this.stateStore.saveState(shardId, reconciled, state._version || 0);
                    return { success: true, state: reconciled, version: (state._version || 0) + 1, actions,
                             warnings: [...warnings, 'State was automatically reconciled'], errors, durationMs: Date.now() - startTime };
                }
            }

            return { success: true, state, version: state._version || 0, actions, warnings, errors, durationMs: Date.now() - startTime };
        } catch (error) {
            errors.push(error instanceof Error ? error.message : String(error));
            this.logger.error({ error, shardId }, 'Recovery failed');
            return { success: false, version: 0, actions, warnings, errors, durationMs: Date.now() - startTime };
        } finally {
            await this.releaseRecoveryLock(shardId);
            actions.push({ type: 'lock_released', timestamp: new Date(), description: 'Recovery lock released', details: { shardId } });
        }
    }

    private async validateState(state: ShardFailoverState, shardId: string): Promise<ValidationResult> {
        const issues: ValidationIssue[] = [];
        if (!state.state || !state.primary) {
            issues.push({ type: 'corrupted_state', severity: 'error', description: 'State missing required fields',
                          details: { missing: ['state', 'primary'].filter(k => !(k in state)) } });
        }
        if (typeof state._version !== 'number' || state._version < 0) {
            issues.push({ type: 'corrupted_state', severity: 'error', description: 'Invalid state version', details: { version: state._version } });
        }
        if (state.updatedAt) {
            const ageMs = Date.now() - new Date(state.updatedAt).getTime();
            if (ageMs > this.options.maxStateAgeMs) {
                issues.push({ type: 'stale_lock', severity: 'warning', description: 'State is older than maxStateAgeMs',
                              details: { ageMs, maxStateAgeMs: this.options.maxStateAgeMs } });
            }
        }
        const validStates: FailoverState[] = ['primary_healthy', 'primary_unhealthy', 'standby_healthy', 
            'standby_unhealthy', 'failover_in_progress', 'promoting', 'demoting', 'recovering', 'fenced'];
        if (state.state && !validStates.includes(state.state)) {
            issues.push({ type: 'corrupted_state', severity: 'error', description: 'Invalid state value', details: { state: state.state, validStates } });
        }
        return { valid: issues.every(i => i.severity !== 'error'), issues };
    }

    private async reconcileState(state: ShardFailoverState, validation: ValidationResult, shardId: string): Promise<ShardFailoverState | null> {
        const reconciled: Partial<ShardFailoverState> = { ...state }; let changed = false;
        if (!reconciled.primary) return null;
        if (state.state && !['primary_healthy', 'primary_unhealthy', 'standby_healthy', 'standby_unhealthy'].includes(state.state)) {
            if (reconciled.primary && reconciled.primary.id) {
                reconciled.state = 'primary_healthy'; changed = true;
            }
        }
        if (typeof reconciled._version !== 'number') { reconciled._version = 0; changed = true; }
        if (!reconciled.updatedAt) { reconciled.updatedAt = new Date().toISOString(); changed = true; }
        return changed ? reconciled as ShardFailoverState : null;
    }
}
```

### Integration
```typescript
// In FailoverManager
private recoveryManager?: FailoverRecoveryManager;
constructor(options: FailoverManagerOptions) {
    if (options.stateStore) this.recoveryManager = new FailoverRecoveryManager(options.stateStore, options.logger, options.recoveryOptions);
}

async initialize(): Promise<void> {
    if (this.recoveryManager) {
        const result = await this.recoveryManager.recoverState(this.shardId);
        if (result.success && result.state) {
            this.state = result.state;
            this.logger.info({ shardId: this.shardId, state: result.state.state, version: result.version }, 'Recovered failover state');
            this.onStateChange?.(this.state);
        } else {
            this.logger.warn({ shardId: this.shardId, errors: result.errors }, 'Failed to recover state');
            this.state = createInitialFailoverState(this.config);
        }
    } else {
        this.state = createInitialFailoverState(this.config);
    }
    this.startHealthChecks();
}
```

### Acceptance Criteria
- [ ] Recovery manager implemented
- [ ] State validation and reconciliation
- [ ] Recovery lock mechanism
- [ ] Integration with FailoverManager
- [ ] State persistence on shutdown
- [ ] Recovery on restart
- [ ] Unit tests (100% coverage)

---

## 6. Failover Hooks and Callbacks

**Priority:** P1 | **Effort:** 3-4 days | **Files:** `src/failover/failover-hooks.ts`

### Implementation

#### Hook Types
```typescript
// src/failover/failover-hooks.ts
export type FailoverHookType = 'beforeStateChange' | 'afterStateChange' | 'beforeFailover' | 'afterFailover' | 
    'failoverFailed' | 'beforePromotion' | 'afterPromotion' | 'promotionFailed' | 'beforeDemotion' | 
    'afterDemotion' | 'demotionFailed' | 'beforeHealthCheck' | 'afterHealthCheck' | 
    'onPrimaryChange' | 'onTopologyChange' | 'onFencingActivated' | 'onFencingReleased';

export type FailoverHook<T extends FailoverHookType = FailoverHookType> = (context: FailoverHookContext<T>) => Promise<void> | void;

export interface FailoverHookContext<T extends FailoverHookType = FailoverHookType> {
    type: T; shardId: string; timestamp: Date;
    manager: FailoverManagerHookAPI;
    data: FailoverHookData[T];
}
```

#### Hook Runner
```typescript
export class FailoverHookRunner {
    constructor(private hooks: FailoverHooks, private logger: RequiredFailoverLogger) {}

    async invoke<T extends FailoverHookType>(type: T, context: Omit<FailoverHookContext<T>, 'type' | 'timestamp'>): Promise<void> {
        const fullContext: FailoverHookContext<T> = { ...context, type, timestamp: new Date() };
        const typeHooks = this.hooks[type] || [];
        if (typeHooks.length === 0) return;
        this.logger.debug({ hookType: type, hookCount: typeHooks.length }, 'Invoking failover hooks');
        const errors: Error[] = [];
        for (const hook of typeHooks) {
            try { await Promise.resolve(hook(fullContext)); }
            catch (error) { errors.push(error instanceof Error ? error : new Error(String(error))); }
        }
        if (errors.length > 0) this.logger.warn({ hookType: type, errorCount: errors.length }, 'Some hooks failed');
    }

    registerHook<T extends FailoverHookType>(type: T, hook: FailoverHook<T>): void {
        if (!this.hooks[type]) this.hooks[type] = []; this.hooks[type].push(hook);
    }
}
```

### Integration
```typescript
// In FailoverManager
private hookRunner: FailoverHookRunner;
constructor(options: FailoverManagerOptions) {
    this.hookRunner = new FailoverHookRunner(options.hooks || {}, options.logger);
    if (options.onStateChange) this.registerHook('afterStateChange', async (c) => options.onStateChange!(c.data.toState));
    if (options.onFailoverComplete) this.registerHook('afterFailover', async (c) => options.onFailoverComplete!(c.data.newPrimary));
}

// In setState
async setState(newState: ShardFailoverState, reason?: string): Promise<void> {
    const oldState = this.state;
    await this.hookRunner.invoke('beforeStateChange', { shardId: this.shardId, manager: this.hookAPI, data: { fromState: oldState, toState: newState, reason } });
    this.state = newState;
    await this.hookRunner.invoke('afterStateChange', { shardId: this.shardId, manager: this.hookAPI, data: { fromState: oldState, toState: newState, reason } });
    this.onStateChange?.(newState);
}
```

### Acceptance Criteria
- [ ] Hook types and interfaces defined
- [ ] Hook runner implemented
- [ ] Hooks invoked at all lifecycle points
- [ ] Error handling (errors don't break operations)
- [ ] Registration/unregistration working
- [ ] Unit tests (100% coverage)

---

## 7. Connection Invalidation After Failover

**Priority:** P0 | **Effort:** 3-5 days | **Files:** `src/failover/connection-tracker.ts`

### Architecture
```
ConnectionTracker
├── Track all active connections
├── Mark connections per node
├── Invalidate by node
├── Handle transaction state
└── Cleanup idle connections
```

### Implementation

#### Connection Tracker
```typescript
// src/failover/connection-tracker.ts
export interface TrackedConnection {
    id: string; client: PoolClient; nodeId: string; shardId: string;
    createdAt: Date; lastUsedAt: Date; inTransaction: boolean; transactionId?: string;
}

export class ConnectionTracker {
    private connections = new Map<string, Set<TrackedConnection>>();
    private clientToConnection = new Map<PoolClient, TrackedConnection>();
    private cleanupTimer?: ReturnType<typeof setInterval>;

    constructor(private logger: RequiredFailoverLogger, private options: Required<ConnectionTrackerOptions> = DEFAULT_CONNECTION_TRACKER_OPTIONS) {
        this.startCleanup();
    }

    registerConnection(nodeId: string, shardId: string, client: PoolClient): TrackedConnection {
        const conn: TrackedConnection = { id: `conn_${++this.nextConnectionId}_${Date.now()}`, client, nodeId, shardId,
            createdAt: new Date(), lastUsedAt: new Date(), inTransaction: false };
        if (!this.connections.has(nodeId)) this.connections.set(nodeId, new Set());
        this.connections.get(nodeId)!.add(conn);
        this.clientToConnection.set(client, conn);
        return conn;
    }

    unregisterConnection(client: PoolClient): boolean {
        const conn = this.clientToConnection.get(client);
        if (!conn) return false;
        const nodeConns = this.connections.get(conn.nodeId);
        if (nodeConns) { nodeConns.delete(conn); if (nodeConns.size === 0) this.connections.delete(conn.nodeId); }
        this.clientToConnection.delete(client);
        return true;
    }

    async invalidateConnections(nodeId: string): Promise<ConnectionInvalidationResult> {
        const nodeConns = this.connections.get(nodeId);
        const result: ConnectionInvalidationResult = { nodeId, invalidatedCount: 0, activeTransactionCount: 0, errors: [] };
        if (!nodeConns || nodeConns.size === 0) return result;
        for (const conn of nodeConns) {
            if (conn.inTransaction) { result.activeTransactionCount++; }
            else {
                try {
                    if (!conn.client.release) { this.unregisterConnection(conn.client); result.invalidatedCount++; }
                    else { this.markForInvalidation(conn); this.unregisterConnection(conn.client); result.invalidatedCount++; }
                } catch (error) { result.errors.push(error instanceof Error ? error : new Error(String(error))); }
            }
        }
        return result;
    }

    async invalidateConnectionsExcept(excludeNodeId: string): Promise<ConnectionInvalidationResult[]> {
        const results: ConnectionInvalidationResult[] = [];
        for (const nodeId of this.connections.keys()) {
            if (nodeId !== excludeNodeId) results.push(await this.invalidateConnections(nodeId));
        }
        return results;
    }
}
```

#### Integration
```typescript
// In FailoverManager
private connectionTracker: ConnectionTracker;
constructor(options: FailoverManagerOptions) {
    this.connectionTracker = new ConnectionTracker(options.logger, options.trackerOptions);
}

// In initiateFailover
private async initiateFailover(reason?: string): Promise<void> {
    const currentPrimary = this.state.primary;
    if (!currentPrimary) return;
    this.logger.info({ nodeId: currentPrimary.id, shardId: this.shardId }, 'Invalidating connections to old primary');
    const result = await this.connectionTracker.invalidateConnections(currentPrimary.id);
    if (result.activeTransactionCount > 0) {
        this.logger.warn({ nodeId: currentPrimary.id, activeTransactions: result.activeTransactionCount }, 'Active transactions remain');
    }
    await this.performFailover();
}
```

### Acceptance Criteria
- [ ] Connection tracker implemented
- [ ] Connection registration/unregistration
- [ ] Connection invalidation by node
- [ ] Transaction tracking
- [ ] Integration with FailoverManager
- [ ] Unit tests (100% coverage)

---

## 8. Connection Pool Reconnection

**Priority:** P1 | **Effort:** 3-4 days | **Files:** `src/failover/pool-reconnector.ts`

### Implementation

#### PoolReconnector
```typescript
// src/failover/pool-reconnector.ts
export class PoolReconnector {
    constructor(private poolMap: Map<string, Pool>, private configProvider: PoolConfigProvider,
                private logger: RequiredFailoverLogger, private options: Required<ReconnectionOptions> = DEFAULT_RECONNECTION_OPTIONS) {}

    async reconnectPool(nodeId: string, newConfig?: PoolConfig): Promise<ReconnectionResult> {
        const existing = this.pendingReconnections.get(nodeId);
        if (existing) return existing;
        const promise = this.doReconnectPool(nodeId, newConfig);
        this.pendingReconnections.set(nodeId, promise);
        try { return await promise; } finally { this.pendingReconnections.delete(nodeId); }
    }

    private async doReconnectPool(nodeId: string, newConfig?: PoolConfig, retries: number = 0): Promise<ReconnectionResult> {
        const startTime = Date.now();
        try {
            const oldPool = this.poolMap.get(nodeId);
            if (!oldPool) throw new Error(`No pool found for node ${nodeId}`);
            const config = newConfig || await this.configProvider.getConfig(nodeId);
            const newPool = new Pool(config);
            await this.testPool(newPool);
            if (this.options.validatePool) await this.validatePool(newPool);
            this.poolMap.set(nodeId, newPool);
            await oldPool.end();
            return { nodeId, success: true, oldPool, newPool, durationMs: Date.now() - startTime, retries };
        } catch (error) {
            if (retries >= this.options.maxRetries) {
                throw new PoolReconnectionError(`Failed to reconnect pool for ${nodeId} after ${retries} retries`, nodeId, error instanceof Error ? error : undefined);
            }
            const delayMs = Math.min(this.options.retryDelayMs * Math.pow(2, retries), this.options.maxRetryDelayMs);
            await new Promise(resolve => setTimeout(resolve, delayMs));
            return this.doReconnectPool(nodeId, newConfig, retries + 1);
        }
    }

    async reconnectPrimaryPool(primaryNodeId: string): Promise<ReconnectionResult> {
        return this.reconnectPool(primaryNodeId);
    }

    async reconnectReplicaPools(replicaNodeIds: string[]): Promise<ReconnectionResult[]> {
        return Promise.all(replicaNodeIds.map(nodeId => this.reconnectPool(nodeId)));
    }

    private async testPool(pool: Pool): Promise<void> {
        const client = await pool.connect();
        try { await client.query('SELECT 1'); } finally { client.release(); }
    }
}
```

### Integration
```typescript
// In FailoverManager
private poolReconnector?: PoolReconnector;
constructor(options: FailoverManagerOptions) {
    if (options.poolMap && options.configProvider) {
        this.poolReconnector = new PoolReconnector(options.poolMap, options.configProvider, options.logger, options.reconnectionOptions);
    }
}

// Register hook
this.registerHook('afterFailover', async (context) => {
    const { newPrimary } = context.data;
    if (this.poolReconnector) {
        await this.poolReconnector.reconnectPrimaryPool(newPrimary.id);
        const topology = this.provider.getTopology();
        await this.poolReconnector.reconnectReplicaPools(topology.replicas.map(r => r.id));
    }
});
```

### Acceptance Criteria
- [ ] Pool reconnector implemented
- [ ] Retry logic with exponential backoff
- [ ] Primary and replica pool reconnection
- [ ] Status tracking
- [ ] Integration with FailoverManager
- [ ] Unit tests (100% coverage)

---

## 9. Fencing Integration Hooks

**Priority:** P1 | **Effort:** 5-7 days | **Files:** `src/failover/fencing.ts`

### Architecture
```
FencingManager
├── AdvisoryLockFencingProvider
├── TableBasedFencingProvider
└── ExternalFencingProvider

Database Tables:
├── _database_fencing_locks
└── _database_fencing_history
```

### Implementation

#### FencingProvider Interface
```typescript
// src/failover/fencing.ts
export interface FencingProvider {
    activate(nodeId: string, reason: string, options?: FencingOptions): Promise<void>;
    deactivate(nodeId: string): Promise<void>;
    isFenced(nodeId: string): Promise<boolean>;
    check(nodeId: string): Promise<void>;
    getFencedNodes(): Promise<string[]>;
    getStatus(nodeId: string): Promise<FencingStatus>;
    cleanup(nodeId: string): Promise<void>;
}
```

#### AdvisoryLockFencingProvider
```typescript
// src/failover/advisory-lock-fencing.ts
export class AdvisoryLockFencingProvider implements FencingProvider {
    constructor(private pool: Pool, private logger: RequiredFailoverLogger, private hooks: FencingHooks = {}) {}

    async activate(nodeId: string, reason: string, options: FencingOptions = {}): Promise<void> {
        const { ttlMs, priority = 1, metadata = {} } = options;
        const lockId = this.generateLockId(nodeId);
        const client = await this.pool.connect();
        try {
            await client.query('SELECT pg_advisory_lock($1)', [lockId]);
            await client.query(`INSERT INTO _database_fencing_locks (node_id, lock_id, reason, expires_at, priority, metadata, activated_by)
                   VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (node_id) DO UPDATE SET lock_id = EXCLUDED.lock_id, reason = EXCLUDED.reason,
                   expires_at = EXCLUDED.expires_at, priority = EXCLUDED.priority, metadata = EXCLUDED.metadata, activated_at = NOW(), activated_by = EXCLUDED.activated_by`,
                   [nodeId, lockId, reason, ttlMs ? new Date(Date.now() + ttlMs) : null, priority, metadata, 'advisory-lock-provider']);
            await client.query(`INSERT INTO _database_fencing_history (node_id, action, reason, priority, metadata, performed_by)
                   VALUES ($1, $2, $3, $4, $5, $6)`, [nodeId, 'activate', reason, priority, metadata, 'advisory-lock-provider']);
            this.logger.info({ nodeId, reason, lockId }, 'Advisory lock fencing activated');
            await this.hooks.onFencingActivated?.(nodeId, reason);
        } finally { client.release(); }
    }

    async deactivate(nodeId: string): Promise<void> {
        const client = await this.pool.connect();
        try {
            const result = await client.query('SELECT lock_id FROM _database_fencing_locks WHERE node_id = $1', [nodeId]);
            if (result.rows.length > 0) {
                const lockId = result.rows[0].lock_id;
                await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
                await client.query('DELETE FROM _database_fencing_locks WHERE node_id = $1', [nodeId]);
                await client.query(`INSERT INTO _database_fencing_history (node_id, action, performed_by) VALUES ($1, $2, $3)`,
                    [nodeId, 'deactivate', 'advisory-lock-provider']);
            }
            this.logger.info({ nodeId }, 'Advisory lock fencing deactivated');
            await this.hooks.onFencingDeactivated?.(nodeId);
        } finally { client.release(); }
    }

    async isFenced(nodeId: string): Promise<boolean> {
        const result = await this.pool.query('SELECT 1 FROM _database_fencing_locks WHERE node_id = $1', [nodeId]);
        return result.rows.length > 0;
    }

    async check(nodeId: string): Promise<void> {
        const fenced = await this.isFenced(nodeId);
        if (fenced) { const error = new FencedError(nodeId, 'Node is fenced'); this.logger.warn({ nodeId }, 'Fencing check failed'); 
            await this.hooks.onFencingCheckFailed?.(nodeId, error); throw error; }
    }

    async getFencedNodes(): Promise<string[]> {
        const result = await this.pool.query('SELECT node_id FROM _database_fencing_locks');
        return result.rows.map(row => row.node_id);
    }

    private generateLockId(nodeId: string): number {
        let hash = 0; for (let i = 0; i < nodeId.length; i++) { const char = nodeId.charCodeAt(i); hash = ((hash << 5) - hash) + char; hash = hash & hash; }
        return Math.abs(hash);
    }
}
```

#### FencingManager
```typescript
// src/failover/fencing-manager.ts
export class FencingManager {
    constructor(private defaultProvider: FencingProvider, private logger: RequiredFailoverLogger) {}

    registerProvider(name: string, provider: FencingProvider): void { this.providers.set(name, provider); }
    use(providerName?: string): FencingProvider { return providerName ? this.providers.get(providerName) || this.defaultProvider : this.defaultProvider; }

    async activateAll(nodeId: string, reason: string, options?: FencingOptions): Promise<void> {
        await Promise.all([...this.providers.entries()].map(([name, provider]) =>
            provider.activate(nodeId, reason, options).catch(error => this.logger.error({ provider: name, nodeId, error: error.message }, 'Failed to activate fencing'))
        ));
        await this.defaultProvider.activate(nodeId, reason, options);
    }

    async deactivateAll(nodeId: string): Promise<void> {
        await Promise.all([...this.providers.entries()].map(([name, provider]) =>
            provider.deactivate(nodeId).catch(error => this.logger.error({ provider: name, nodeId, error: error.message }, 'Failed to deactivate fencing'))
        ));
        await this.defaultProvider.deactivate(nodeId);
    }
}
```

### Integration
```typescript
// In FailoverManager
private fencingManager?: FencingManager;
constructor(options: FailoverManagerOptions) {
    if (options.fencingProvider) this.fencingManager = new FencingManager(options.fencingProvider, options.logger);
}

// In beforeFailover hook
this.registerHook('beforeFailover', async (context) => {
    if (this.fencingManager) {
        await this.fencingManager.activateAll(context.data.currentPrimary.id, context.data.reason || 'failover initiated');
        await this.hookRunner.invoke('onFencingActivated', { shardId: this.shardId, manager: this.hookAPI,
            data: { node: context.data.currentPrimary, reason: context.data.reason || 'failover initiated' } });
    }
});

// In checkNodeHealth
if (this.fencingManager) {
    try { await this.fencingManager.checkAll(node.id); } catch (error) {
        if (error instanceof FencedError) { this.logger.debug({ nodeId: node.id }, 'Node is fenced'); return { healthy: false }; }
        throw error; }
}
```

### Acceptance Criteria
- [ ] FencingProvider interface
- [ ] AdvisoryLockFencingProvider
- [ ] TableBasedFencingProvider
- [ ] FencingManager
- [ ] Integration with FailoverManager
- [ ] Integration with query execution
- [ ] Database schema
- [ ] Unit tests (100% coverage)

---

## 📅 Timeline

| Week | Features |
|------|----------|
| 1-2 | #1 Persistent State Storage |
| 2-3 | #2 Event History, #3 Thresholds |
| 3-4 | #4 Classification, #5 Recovery |
| 5-6 | #6 Hooks, #7 Connection Invalidation |
| 7-8 | #8 Pool Reconnection |
| 8-12 | #9 Fencing Integration |
| 13 | Integration & Testing |

---

## ✅ Success Criteria

Each feature must have:
- ✅ Implementation complete
- ✅ Unit tests (100% coverage)
- ✅ Integration tests
- ✅ Documentation
- ✅ Code review
- ✅ No breaking changes

---

## 📚 References

- [working-issues.md](./working-issues.md)
- [PLAN.md](./PLAN.md)
- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [src/failover/](./src/failover/)
