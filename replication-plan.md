# Replication Layer Implementation Plan

> **Status:** Draft | **Priority:** P0/P1 | **Target:** Weeks 9-14

Detailed implementation for 6 unimplemented features from working-issues.md section 9.

---

## 📋 Overview

6 features to implement:
1. Weighted replica selection (P1, 3-4d)
2. Round-robin replica selection (P1, 2-3d)
3. Least-connections selection (P1, 3-4d)
4. Exact WAL LSN tracking (P1, 5-7d)
5. Replica selection statistics (P1, 2-3d)
6. Read routing hooks (P1, 3-4d)

**Total:** ~19-25 days

---

## 1. Weighted Replica Selection

**Priority:** P1 | **Effort:** 3-4 days | **File:** `src/replication/replication-router.ts`

### Overview
Select replicas with probability proportional to their weights. Useful when replicas have different capacities.

### Implementation

```typescript
// src/replication/replication-router.ts
export interface WeightedReplicaSelectionOptions {
    /** Default weight for replicas without explicit weight */
    defaultWeight: number;
    /** Whether to normalize weights */
    normalizeWeights: boolean;
    /** Minimum weight (replicas below this are excluded) */
    minWeight: number;
}

export const DEFAULT_WEIGHTED_OPTIONS: Required<WeightedReplicaSelectionOptions> = {
    defaultWeight: 1,
    normalizeWeights: true,
    minWeight: 0.01
};

export class WeightedReplicaSelector implements ReplicaSelector {
    private readonly options: Required<WeightedReplicaSelectionOptions>;
    private totalWeight = 0;
    private readonly replicaWeights = new Map<string, number>();

    constructor(
        private readonly endpoints: EndpointEntry[],
        private readonly healthManager: ReplicationHealthManager,
        logger: Logger,
        options: Partial<WeightedReplicaSelectionOptions> = {}
    ) {
        this.options = { ...DEFAULT_WEIGHTED_OPTIONS, ...options };
        this.updateWeights();
    }

    /** Update weights when endpoints change */
    updateWeights(): void {
        this.replicaWeights.clear();
        this.totalWeight = 0;

        for (const endpoint of this.endpoints) {
            if (endpoint.role === 'replica') {
                const weight = this.getReplicaWeight(endpoint);
                if (weight > this.options.minWeight) {
                    this.replicaWeights.set(endpoint.endpointId, weight);
                    this.totalWeight += weight;
                }
            }
        }

        if (this.options.normalizeWeights && this.totalWeight > 0) {
            for (const [id, weight] of this.replicaWeights) {
                this.replicaWeights.set(id, weight / this.totalWeight);
            }
            this.totalWeight = 1;
        }
    }

    /** Get weight for a replica */
    private getReplicaWeight(endpoint: EndpointEntry): number {
        // Check for explicit weight in connection config
        if (typeof endpoint.connection.weight === 'number') {
            return endpoint.connection.weight;
        }
        return this.options.defaultWeight;
    }

    /** Select a replica based on weights */
    selectReplica(): EndpointEntry | null {
        if (this.totalWeight <= 0) return null;

        // Filter to healthy replicas only
        const healthyReplicas = this.endpoints.filter(
            e => e.role === 'replica' && this.healthManager.isHealthy(e.endpointId)
        );

        if (healthyReplicas.length === 0) return null;

        // Generate random number between 0 and totalWeight
        const random = Math.random() * this.totalWeight;
        let cumulativeWeight = 0;

        for (const endpoint of healthyReplicas) {
            const weight = this.replicaWeights.get(endpoint.endpointId) || 0;
            cumulativeWeight += weight;
            if (random <= cumulativeWeight) {
                return endpoint;
            }
        }

        // Fallback to first healthy replica if no selection made
        return healthyReplicas[0];
    }

    /** Select multiple replicas proportionally */
    selectReplicas(count: number): EndpointEntry[] {
        const replicas: EndpointEntry[] = [];
        const available = this.endpoints.filter(
            e => e.role === 'replica' && this.healthManager.isHealthy(e.endpointId)
        );

        // For each selection, pick based on weight
        for (let i = 0; i < Math.min(count, available.length); i++) {
            const selected = this.selectReplica();
            if (selected) {
                replicas.push(selected);
            }
        }

        return replicas;
    }
}
```

### Integration

```typescript
// In ReplicationRouter
private readonly selectors: Map<string, ReplicaSelector> = new Map();

// Initialize selectors
this.selectors.set('weighted', new WeightedReplicaSelector(
    this.endpoints,
    this.healthManager,
    this.logger,
    this.config.weightedOptions
));

// Use selector
selectReplica(selectionPolicy?: string): EndpointEntry | null {
    const policy = selectionPolicy || this.config.defaultReplicaSelectionPolicy;
    const selector = this.selectors.get(policy);
    if (!selector) {
        this.logger.warn({ policy }, 'Unknown selection policy, using random');
        return this.selectRandomReplica();
    }
    return selector.selectReplica();
}
```

### Acceptance Criteria
- [ ] WeightedReplicaSelector class
- [ ] Configurable weights
- [ ] Weight normalization
- [ ] Healthy replica filtering
- [ ] Integration with router
- [ ] Unit tests (100%)

---

## 2. Round-Robin Replica Selection

**Priority:** P1 | **Effort:** 2-3 days | **File:** `src/replication/replication-router.ts`

### Implementation

```typescript
// src/replication/replication-router.ts
export class RoundRobinReplicaSelector implements ReplicaSelector {
    private currentIndex = 0;
    private readonly endpoints: EndpointEntry[];
    private readonly healthManager: ReplicationHealthManager;

    constructor(
        endpoints: EndpointEntry[],
        healthManager: ReplicationHealthManager,
        private readonly logger: Logger
    ) {
        this.endpoints = endpoints;
        this.healthManager = healthManager;
    }

    /** Select next replica in round-robin order */
    selectReplica(): EndpointEntry | null {
        const healthyReplicas = this.endpoints.filter(
            e => e.role === 'replica' && this.healthManager.isHealthy(e.endpointId)
        );

        if (healthyReplicas.length === 0) return null;

        const selected = healthyReplicas[this.currentIndex % healthyReplicas.length];
        this.currentIndex++;
        return selected;
    }

    /** Select multiple replicas in round-robin order */
    selectReplicas(count: number): EndpointEntry[] {
        const replicas: EndpointEntry[] = [];
        const healthyReplicas = this.endpoints.filter(
            e => e.role === 'replica' && this.healthManager.isHealthy(e.endpointId)
        );

        for (let i = 0; i < Math.min(count, healthyReplicas.length); i++) {
            const selected = healthyReplicas[this.currentIndex % healthyReplicas.length];
            replicas.push(selected);
            this.currentIndex++;
        }

        return replicas;
    }

    /** Reset to start of rotation */
    reset(): void {
        this.currentIndex = 0;
    }
}
```

### Integration

```typescript
// In ReplicationRouter initialization
this.selectors.set('round-robin', new RoundRobinReplicaSelector(
    this.endpoints,
    this.healthManager,
    this.logger
));
```

### Acceptance Criteria
- [ ] RoundRobinReplicaSelector class
- [ ] Sequential selection
- [ ] Healthy replica filtering
- [ ] Integration with router
- [ ] Reset functionality
- [ ] Unit tests (100%)

---

## 3. Least-Connections Selection

**Priority:** P1 | **Effort:** 3-4 days | **File:** `src/replication/replication-router.ts`

### Implementation

```typescript
// src/replication/replication-router.ts
export class LeastConnectionsReplicaSelector implements ReplicaSelector {
    private readonly connectionTracker: ReplicationConnectionTracker;

    constructor(
        private readonly endpoints: EndpointEntry[],
        private readonly healthManager: ReplicationHealthManager,
        connectionTracker: ReplicationConnectionTracker,
        private readonly logger: Logger
    ) {
        this.connectionTracker = connectionTracker;
    }

    /** Select replica with least connections */
    selectReplica(): EndpointEntry | null {
        const healthyReplicas = this.endpoints.filter(
            e => e.role === 'replica' && this.healthManager.isHealthy(e.endpointId)
        );

        if (healthyReplicas.length === 0) return null;

        // Get connection counts for all healthy replicas
        const withConnections = healthyReplicas.map(endpoint => ({
            endpoint,
            connectionCount: this.connectionTracker.getConnectionCount(endpoint.endpointId)
        }));

        // Sort by connection count (ascending)
        withConnections.sort((a, b) => a.connectionCount - b.connectionCount);

        // Return replica with least connections
        return withConnections[0].endpoint;
    }

    /** Select multiple replicas with least connections */
    selectReplicas(count: number): EndpointEntry[] {
        const healthyReplicas = this.endpoints.filter(
            e => e.role === 'replica' && this.healthManager.isHealthy(e.endpointId)
        );

        if (healthyReplicas.length === 0) return [];

        // Get connection counts
        const withConnections = healthyReplicas.map(endpoint => ({
            endpoint,
            connectionCount: this.connectionTracker.getConnectionCount(endpoint.endpointId)
        }));

        // Sort by connection count
        withConnections.sort((a, b) => a.connectionCount - b.connectionCount);

        // Return top N
        return withConnections.slice(0, Math.min(count, withConnections.length))
            .map(({ endpoint }) => endpoint);
    }
}

// Connection Tracker
export class ReplicationConnectionTracker {
    private readonly connectionCounts = new Map<string, number>();

    /** Record connection acquired */
    acquireConnection(endpointId: string): void {
        this.connectionCounts.set(endpointId, (this.connectionCounts.get(endpointId) || 0) + 1);
    }

    /** Record connection released */
    releaseConnection(endpointId: string): void {
        const current = this.connectionCounts.get(endpointId) || 0;
        this.connectionCounts.set(endpointId, Math.max(0, current - 1));
    }

    /** Get connection count for endpoint */
    getConnectionCount(endpointId: string): number {
        return this.connectionCounts.get(endpointId) || 0;
    }

    /** Get all connection counts */
    getAllConnectionCounts(): Map<string, number> {
        return new Map(this.connectionCounts);
    }

    /** Reset counts */
    reset(): void {
        this.connectionCounts.clear();
    }
}
```

### Integration

```typescript
// In ReplicationManager
private readonly connectionTracker = new ReplicationConnectionTracker();

// When acquiring connection
async getConnection(endpointId: string): Promise<PoolClient> {
    const endpoint = this.getEndpoint(endpointId);
    if (!endpoint) throw new Error(`Endpoint ${endpointId} not found`);
    
    this.connectionTracker.acquireConnection(endpointId);
    const client = await endpoint.pool.connect();
    
    // Wrap release to track
    const originalRelease = client.release.bind(client);
    client.release = () => {
        this.connectionTracker.releaseConnection(endpointId);
        originalRelease();
    };
    
    return client;
}

// In ReplicationRouter initialization
this.selectors.set('least-connections', new LeastConnectionsReplicaSelector(
    this.endpoints,
    this.healthManager,
    this.connectionTracker,
    this.logger
));
```

### Acceptance Criteria
- [ ] LeastConnectionsReplicaSelector class
- [ ] Connection tracking
- [ ] Healthy replica filtering
- [ ] Integration with router
- [ ] Unit tests (100%)

---

## 4. Exact WAL LSN Tracking

**Priority:** P1 | **Effort:** 5-7 days | **File:** `src/replication/replication-manager.ts`

### Overview
Track exact Write-Ahead Log (WAL) positions to measure replication lag precisely in bytes. PostgreSQL's `pg_last_xact_replay_lsn()` gives the last committed LSN, and we can compare this with the primary's LSN.

### Implementation

```typescript
// src/replication/replication-types.ts
export interface ReplicationLagInfo {
    /** Replica endpoint ID */
    endpointId: string;
    
    /** Last reported LSN from replica */
    replicaLsn: string;
    
    /** Primary's current LSN */
    primaryLsn: string;
    
    /** LSN difference in bytes */
    lsnDiffBytes: bigint;
    
    /** LSN difference in human-readable format */
    lsnDiffFormatted: string;
    
    /** Timestamp when lag was measured */
    measuredAt: Date;
    
    /** Lag in milliseconds (from previous method) */
    lagMs?: number;
}

export interface WalTrackingOptions {
    /** How often to check LSN (ms) */
    checkIntervalMs: number;
    
    /** Timeout for LSN query (ms) */
    queryTimeoutMs: number;
    
    /** Maximum allowed LSN lag in bytes */
    maxLsnLagBytes: bigint;
}

export const DEFAULT_WAL_TRACKING_OPTIONS: Required<WalTrackingOptions> = {
    checkIntervalMs: 10_000,  // 10 seconds
    queryTimeoutMs: 2_000,     // 2 seconds
    maxLsnLagBytes: 1024n * 1024n * 1024n  // 1GB
};
```

#### LSN Parser and Comparator

```typescript
// src/replication/lsn-utils.ts
export class LsnUtils {
    /**
     * Parse LSN string to bigint
     * LSN format: X/Y where X and Y are hexadecimal numbers
     * Full LSN: 8 hex digits / 8 hex digits = 64-bit value
     */
    static parseLsn(lsn: string): bigint {
        // Remove any whitespace
        lsn = lsn.trim();
        
        // Split on /
        const parts = lsn.split('/');
        
        if (parts.length === 1) {
            // Single part - might be just the second part
            return BigInt(`0x${parts[0]}`);
        }
        
        if (parts.length !== 2) {
            throw new Error(`Invalid LSN format: ${lsn}`);
        }
        
        // Parse both parts as hex
        const first = BigInt(`0x${parts[0]}`);
        const second = BigInt(`0x${parts[1]}`);
        
        // LSN is a 64-bit value: first part is high 32 bits, second is low 32 bits
        return (first << 32n) | second;
    }

    /**
     * Convert bigint to LSN string
     */
    static formatLsn(value: bigint): string {
        const high = (value >> 32n) & 0xFFFFFFFFn;
        const low = value & 0xFFFFFFFFn;
        return `${high.toString(16)}/${low.toString(16)}`;
    }

    /**
     * Calculate difference between two LSNs
     * Returns positive if lsn2 is ahead of lsn1
     */
    static diffLsn(lsn1: string, lsn2: string): bigint {
        const val1 = this.parseLsn(lsn1);
        const val2 = this.parseLsn(lsn2);
        return val2 - val1;
    }

    /**
     * Compare two LSNs
     * Returns -1 if lsn1 < lsn2, 0 if equal, 1 if lsn1 > lsn2
     */
    static compareLsn(lsn1: string, lsn2: string): number {
        const val1 = this.parseLsn(lsn1);
        const val2 = this.parseLsn(lsn2);
        
        if (val1 < val2) return -1;
        if (val1 > val2) return 1;
        return 0;
    }

    /**
     * Format byte difference in human-readable format
     */
    static formatBytes(bytes: bigint): string {
        if (bytes < 1024n) return `${bytes}B`;
        if (bytes < 1024n * 1024n) return `${(bytes / 1024n).toString()}KB`;
        if (bytes < 1024n * 1024n * 1024n) return `${(bytes / (1024n * 1024n)).toString()}MB`;
        return `${(bytes / (1024n * 1024n * 1024n)).toString()}GB`;
    }
}
```

#### WAL Tracker

```typescript
// src/replication/wal-tracker.ts
export class WalTracker {
    private readonly endpoints: Map<string, EndpointEntry>;
    private readonly logger: Logger;
    private readonly options: Required<WalTrackingOptions>;
    
    private primaryLsn: string | null = null;
    private readonly replicaLsns = new Map<string, { lsn: string; updatedAt: Date }>();
    
    private trackingInterval?: ReturnType<typeof setInterval>;
    private tracking = false;

    constructor(
        endpoints: Map<string, EndpointEntry> = new Map(),
        logger: Logger,
        options: Partial<WalTrackingOptions> = {}
    ) {
        this.endpoints = endpoints;
        this.logger = logger;
        this.options = { ...DEFAULT_WAL_TRACKING_OPTIONS, ...options };
    }

    /** Start WAL tracking */
    start(): void {
        if (this.tracking) return;
        this.tracking = true;
        this.trackingInterval = setInterval(
            () => this.updateLagInfo(),
            this.options.checkIntervalMs
        );
    }

    /** Stop WAL tracking */
    stop(): void {
        if (!this.tracking) return;
        this.tracking = false;
        if (this.trackingInterval) {
            clearInterval(this.trackingInterval);
        }
    }

    /** Update LSN information for all endpoints */
    async updateLagInfo(): Promise<ReplicationLagInfo[]> {
        const results: ReplicationLagInfo[] = [];
        const now = new Date();

        // Get primary LSN
        const primaryEndpoint = Array.from(this.endpoints.values())
            .find(e => e.role === 'primary');
        
        if (primaryEndpoint) {
            try {
                this.primaryLsn = await this.getCurrentLsn(primaryEndpoint);
            } catch (error) {
                this.logger.error(
                    { endpointId: primaryEndpoint.endpointId, error: error.message },
                    'Failed to get primary LSN'
                );
            }
        }

        // Get replica LSNs
        const replicaEndpoints = Array.from(this.endpoints.values())
            .filter(e => e.role === 'replica');

        for (const endpoint of replicaEndpoints) {
            try {
                const lsn = await this.getCurrentLsn(endpoint);
                this.replicaLsns.set(endpoint.endpointId, { lsn, updatedAt: now });

                if (this.primaryLsn) {
                    const lsnDiff = LsnUtils.diffLsn(this.primaryLsn, lsn);
                    results.push({
                        endpointId: endpoint.endpointId,
                        replicaLsn: lsn,
                        primaryLsn: this.primaryLsn,
                        lsnDiffBytes: lsnDiff,
                        lsnDiffFormatted: LsnUtils.formatBytes(lsnDiff),
                        measuredAt: now
                    });
                }
            } catch (error) {
                this.logger.error(
                    { endpointId: endpoint.endpointId, error: error.message },
                    'Failed to get replica LSN'
                );
            }
        }

        return results;
    }

    /** Get current LSN for an endpoint */
    private async getCurrentLsn(endpoint: EndpointEntry): Promise<string> {
        const query = endpoint.role === 'primary' 
            ? 'SELECT pg_current_wal_lsn()' 
            : 'SELECT pg_last_xact_replay_lsn()';

        const result = await Promise.race([
            endpoint.pool.query(query),
            new Promise<never>((_, reject) => {
                setTimeout(
                    () => reject(new Error(`LSN query timeout after ${this.options.queryTimeoutMs}ms`)),
                    this.options.queryTimeoutMs
                );
            })
        ]);

        return result.rows[0][endpoint.role === 'primary' ? 'pg_current_wal_lsn' : 'pg_last_xact_replay_lsn'];
    }

    /** Get LSN for a specific endpoint */
    getLsn(endpointId: string): string | null {
        const info = this.replicaLsns.get(endpointId);
        return info?.lsn || null;
    }

    /** Get primary LSN */
    getPrimaryLsn(): string | null {
        return this.primaryLsn;
    }

    /** Get lag information for a replica */
    getLagInfo(endpointId: string): ReplicationLagInfo | null {
        const info = this.replicaLsns.get(endpointId);
        if (!info || !this.primaryLsn) return null;

        const lsnDiff = LsnUtils.diffLsn(this.primaryLsn, info.lsn);
        return {
            endpointId,
            replicaLsn: info.lsn,
            primaryLsn: this.primaryLsn,
            lsnDiffBytes: lsnDiff,
            lsnDiffFormatted: LsnUtils.formatBytes(lsnDiff),
            measuredAt: info.updatedAt
        };
    }

    /** Get lag information for all replicas */
    getAllLagInfo(): ReplicationLagInfo[] {
        const results: ReplicationLagInfo[] = [];
        
        for (const [endpointId, info] of this.replicaLsns) {
            if (this.primaryLsn) {
                const lsnDiff = LsnUtils.diffLsn(this.primaryLsn, info.lsn);
                results.push({
                    endpointId,
                    replicaLsn: info.lsn,
                    primaryLsn: this.primaryLsn,
                    lsnDiffBytes: lsnDiff,
                    lsnDiffFormatted: LsnUtils.formatBytes(lsnDiff),
                    measuredAt: info.updatedAt
                });
            }
        }

        return results;
    }

    /** Check if any replica exceeds max LSN lag */
    hasExcessiveLag(): boolean {
        for (const info of this.getAllLagInfo()) {
            if (info.lsnDiffBytes > this.options.maxLsnLagBytes) {
                return true;
            }
        }
        return false;
    }

    /** Get replicas with excessive lag */
    getLaggingReplicas(): ReplicationLagInfo[] {
        return this.getAllLagInfo().filter(
            info => info.lsnDiffBytes > this.options.maxLsnLagBytes
        );
    }
}
```

### Integration

```typescript
// In ReplicationManager
private readonly walTracker: WalTracker;

constructor(options: ReplicationConfig) {
    // ... existing code
    this.walTracker = new WalTracker(this.endpoints, this.logger, options.walTrackingOptions);
    
    if (options.trackLsn) {
        this.walTracker.start();
    }
}

// Add to health checks
private async checkReplicaHealth(endpointId: string): Promise<ReplicationEndpointHealth> {
    // ... existing health check logic
    
    // Add LSN lag info
    const lagInfo = this.walTracker.getLagInfo(endpointId);
    if (lagInfo) {
        return {
            ...health,
            lsnLagBytes: lagInfo.lsnDiffBytes,
            lsnLagFormatted: lagInfo.lsnDiffFormatted
        };
    }
    
    return health;
}

// Add to replication stats
getReplicationStats(): ReplicationStats {
    return {
        // ... existing stats
        lagInfo: this.walTracker.getAllLagInfo(),
        hasExcessiveLag: this.walTracker.hasExcessiveLag(),
        laggingReplicas: this.walTracker.getLaggingReplicas().map(r => r.endpointId)
    };
}
```

### Acceptance Criteria
- [ ] LsnUtils class for LSN parsing/formatting
- [ ] WalTracker class
- [ ] LSN tracking for primary and replicas
- [ ] Lag calculation in bytes
- [ ] Excessive lag detection
- [ ] Integration with health checks
- [ ] Unit tests (100%)

---

## 5. Replica Selection Statistics

**Priority:** P1 | **Effort:** 2-3 days | **File:** `src/replication/replication-stats.ts`

### Implementation

```typescript
// src/replication/replication-stats.ts
export interface ReplicaSelectionStats {
    /** Total number of selections */
    totalSelections: number;
    
    /** Selections per replica */
    byReplica: Map<string, number>;
    
    /** Selections per policy */
    byPolicy: Map<string, number>;
    
    /** Selection success rate per replica */
    successRateByReplica: Map<string, { total: number; success: number; rate: number }>;
    
    /** Average latency per replica */
    avgLatencyByReplica: Map<string, { totalMs: number; count: number; avgMs: number }>;
    
    /** Last selection time per replica */
    lastSelectionByReplica: Map<string, Date>;
    
    /** Error count per replica */
    errorCountByReplica: Map<string, number>;
}

export interface ReplicaSelectionEvent {
    timestamp: Date;
    replicaId: string;
    policy: string;
    success: boolean;
    latencyMs?: number;
    error?: string;
}

export class ReplicaSelectionStatsTracker {
    private readonly events: ReplicaSelectionEvent[] = [];
    private readonly maxEvents: number;

    constructor(private readonly logger: Logger, maxEvents: number = 10000) {
        this.maxEvents = maxEvents;
    }

    /** Record a selection event */
    recordSelection(event: ReplicaSelectionEvent): void {
        this.events.push(event);
        
        // Trim old events if exceeding max
        if (this.events.length > this.maxEvents) {
            this.events.shift();
        }
    }

    /** Get statistics */
    getStats(): ReplicaSelectionStats {
        const stats: ReplicaSelectionStats = {
            totalSelections: this.events.length,
            byReplica: new Map(),
            byPolicy: new Map(),
            successRateByReplica: new Map(),
            avgLatencyByReplica: new Map(),
            lastSelectionByReplica: new Map(),
            errorCountByReplica: new Map()
        };

        for (const event of this.events) {
            // Count by replica
            stats.byReplica.set(
                event.replicaId,
                (stats.byReplica.get(event.replicaId) || 0) + 1
            );

            // Count by policy
            stats.byPolicy.set(
                event.policy,
                (stats.byPolicy.get(event.policy) || 0) + 1
            );

            // Track last selection
            stats.lastSelectionByReplica.set(event.replicaId, event.timestamp);

            // Track success rate
            let replicaSuccess = stats.successRateByReplica.get(event.replicaId);
            if (!replicaSuccess) {
                replicaSuccess = { total: 0, success: 0, rate: 0 };
                stats.successRateByReplica.set(event.replicaId, replicaSuccess);
            }
            replicaSuccess.total++;
            if (event.success) {
                replicaSuccess.success++;
            }
            replicaSuccess.rate = replicaSuccess.total > 0 
                ? replicaSuccess.success / replicaSuccess.total 
                : 0;

            // Track latency
            let replicaLatency = stats.avgLatencyByReplica.get(event.replicaId);
            if (!replicaLatency) {
                replicaLatency = { totalMs: 0, count: 0, avgMs: 0 };
                stats.avgLatencyByReplica.set(event.replicaId, replicaLatency);
            }
            if (typeof event.latencyMs === 'number') {
                replicaLatency.totalMs += event.latencyMs;
                replicaLatency.count++;
                replicaLatency.avgMs = replicaLatency.count > 0 
                    ? replicaLatency.totalMs / replicaLatency.count 
                    : 0;
            }

            // Track errors
            if (!event.success) {
                stats.errorCountByReplica.set(
                    event.replicaId,
                    (stats.errorCountByReplica.get(event.replicaId) || 0) + 1
                );
            }
        }

        return stats;
    }

    /** Get recent events */
    getRecentEvents(limit: number = 100): ReplicaSelectionEvent[] {
        return this.events.slice(-limit);
    }

    /** Get events for a specific replica */
    getEventsForReplica(replicaId: string, limit: number = 100): ReplicaSelectionEvent[] {
        return this.events
            .filter(e => e.replicaId === replicaId)
            .slice(-limit);
    }

    /** Get events for a specific policy */
    getEventsForPolicy(policy: string, limit: number = 100): ReplicaSelectionEvent[] {
        return this.events
            .filter(e => e.policy === policy)
            .slice(-limit);
    }

    /** Clear all events */
    clear(): void {
        this.events.length = 0;
    }
}
```

### Integration

```typescript
// In ReplicationRouter
private readonly selectionStats = new ReplicaSelectionStatsTracker(this.logger);

// Track selections
selectReplica(selectionPolicy?: string): EndpointEntry | null {
    const policy = selectionPolicy || this.config.defaultReplicaSelectionPolicy;
    const selector = this.selectors.get(policy);
    if (!selector) {
        this.logger.warn({ policy }, 'Unknown selection policy, using random');
        return this.selectRandomReplica();
    }

    const startTime = Date.now();
    try {
        const replica = selector.selectReplica();
        const latencyMs = Date.now() - startTime;

        if (replica) {
            this.selectionStats.recordSelection({
                timestamp: new Date(),
                replicaId: replica.endpointId,
                policy,
                success: true,
                latencyMs
            });
        }

        return replica;
    } catch (error) {
        this.selectionStats.recordSelection({
            timestamp: new Date(),
            replicaId: 'unknown',
            policy,
            success: false,
            latencyMs: Date.now() - startTime,
            error: error instanceof Error ? error.message : String(error)
        });
        throw error;
    }
}

// Expose stats
getSelectionStats(): ReplicaSelectionStats {
    return this.selectionStats.getStats();
}

getSelectionEvents(limit: number = 100): ReplicaSelectionEvent[] {
    return this.selectionStats.getRecentEvents(limit);
}
```

### Acceptance Criteria
- [ ] ReplicaSelectionStatsTracker class
- [ ] Event recording
- [ ] Statistics aggregation
- [ ] Query methods
- [ ] Integration with router
- [ ] Unit tests (100%)

---

## 6. Read Routing Hooks

**Priority:** P1 | **Effort:** 3-4 days | **File:** `src/replication/replication-hooks.ts`

### Implementation

```typescript
// src/replication/replication-hooks.ts
export type ReadRoutingHookType = 
    | 'beforeSelectReplica'
    | 'afterSelectReplica'
    | 'beforeQuery'
    | 'afterQuery'
    | 'onReplicaFailure'
    | 'onReplicaSuccess';

export interface ReadRoutingHookContext<T extends ReadRoutingHookType = ReadRoutingHookType> {
    type: T;
    query: string;
    params?: unknown[];
    replica?: EndpointEntry;
    timestamp: Date;
    router: ReplicationRouterHookAPI;
    data: ReadRoutingHookData[T];
}

export type ReadRoutingHookData = {
    beforeSelectReplica: { policy?: string; availableReplicas: string[] };
    afterSelectReplica: { policy?: string; selectedReplica: string | null };
    beforeQuery: { replica: EndpointEntry };
    afterQuery: { replica: EndpointEntry; durationMs: number; rowCount: number };
    onReplicaFailure: { replica: EndpointEntry; error: Error; retryCount: number };
    onReplicaSuccess: { replica: EndpointEntry; durationMs: number; rowCount: number };
};

export interface ReplicationRouterHookAPI {
    getAvailableReplicas(): string[];
    getCurrentPolicy(): string;
    getReplicaHealth(replicaId: string): ReplicationEndpointHealth | null;
    getReplicationStats(): ReplicationStats;
}

export type ReadRoutingHook<T extends ReadRoutingHookType = ReadRoutingHookType> = (
    context: ReadRoutingHookContext<T>
) => Promise<void> | void;

export interface ReadRoutingHooks {
    [K in ReadRoutingHookType]?: ReadRoutingHook<K>[];
}

export class ReadRoutingHookRunner {
    private readonly hooks: ReadRoutingHooks;
    private readonly logger: Logger;

    constructor(hooks: ReadRoutingHooks = {}, logger: Logger) {
        this.hooks = hooks;
        this.logger = logger;
    }

    async invoke<T extends ReadRoutingHookType>(
        type: T,
        context: Omit<ReadRoutingHookContext<T>, 'type' | 'timestamp'>
    ): Promise<void> {
        const fullContext: ReadRoutingHookContext<T> = {
            ...context,
            type,
            timestamp: new Date()
        };

        const typeHooks = this.hooks[type] || [];
        if (typeHooks.length === 0) return;

        this.logger.debug(
            { hookType: type, hookCount: typeHooks.length },
            'Invoking read routing hooks'
        );

        const errors: Error[] = [];
        for (const hook of typeHooks) {
            try {
                await Promise.resolve(hook(fullContext));
            } catch (error) {
                errors.push(error instanceof Error ? error : new Error(String(error)));
                this.logger.error(
                    { hookType: type, error: error instanceof Error ? error.message : String(error) },
                    'Read routing hook error'
                );
            }
        }

        if (errors.length > 0) {
            this.logger.warn(
                { hookType: type, errorCount: errors.length },
                'Some read routing hooks failed'
            );
        }
    }

    registerHook<T extends ReadRoutingHookType>(type: T, hook: ReadRoutingHook<T>): void {
        if (!this.hooks[type]) {
            this.hooks[type] = [];
        }
        this.hooks[type].push(hook);
    }

    unregisterHook<T extends ReadRoutingHookType>(type: T, hook: ReadRoutingHook<T>): boolean {
        const typeHooks = this.hooks[type];
        if (!typeHooks) return false;
        const index = typeHooks.indexOf(hook);
        if (index === -1) return false;
        typeHooks.splice(index, 1);
        return true;
    }
}
```

### Integration

```typescript
// In ReplicationRouter
private readonly hookRunner: ReadRoutingHookRunner;
private readonly hookAPI: ReplicationRouterHookAPI;

constructor(config: ReplicationConfig, logger: Logger) {
    // ... existing code
    this.hookRunner = new ReadRoutingHookRunner(config.hooks, logger);
    this.hookAPI = {
        getAvailableReplicas: () => this.getAvailableReplicaIds(),
        getCurrentPolicy: () => this.config.defaultReplicaSelectionPolicy,
        getReplicaHealth: (id) => this.healthManager.getHealth(id),
        getReplicationStats: () => this.getStats()
    };
}

// In selectReplica
selectReplica(selectionPolicy?: string): EndpointEntry | null {
    const policy = selectionPolicy || this.config.defaultReplicaSelectionPolicy;
    
    await this.hookRunner.invoke('beforeSelectReplica', {
        query: '', // Will be set by query method
        router: this.hookAPI,
        data: {
            policy,
            availableReplicas: this.getAvailableReplicaIds()
        }
    });

    const replica = this.doSelectReplica(policy);

    await this.hookRunner.invoke('afterSelectReplica', {
        query: '',
        router: this.hookAPI,
        replica: replica || undefined,
        data: {
            policy,
            selectedReplica: replica?.endpointId || null
        }
    });

    return replica;
}

// In executeQuery
async executeQuery(query: string, params?: unknown[]): Promise<QueryResult> {
    const replica = this.selectReplica();
    if (!replica) {
        throw new NoReplicasAvailableError();
    }

    await this.hookRunner.invoke('beforeQuery', {
        query,
        params,
        replica,
        router: this.hookAPI,
        data: { replica }
    });

    const startTime = Date.now();
    try {
        const result = await this.executeOnReplica(replica, query, params);
        const durationMs = Date.now() - startTime;

        await this.hookRunner.invoke('afterQuery', {
            query,
            params,
            replica,
            router: this.hookAPI,
            data: {
                replica,
                durationMs,
                rowCount: result.rowCount
            }
        });

        await this.hookRunner.invoke('onReplicaSuccess', {
            query,
            params,
            replica,
            router: this.hookAPI,
            data: {
                replica,
                durationMs,
                rowCount: result.rowCount
            }
        });

        return result;
    } catch (error) {
        await this.hookRunner.invoke('onReplicaFailure', {
            query,
            params,
            replica,
            router: this.hookAPI,
            data: {
                replica,
                error: error instanceof Error ? error : new Error(String(error)),
                retryCount: 0 // Would track retries
            }
        });
        throw error;
    }
}
```

### Example Usage

```typescript
// Example: Logging hooks
router.registerHook('beforeQuery', async (context) => {
    console.log(`[${context.timestamp.toISOString()}] ` +
                `Executing query on ${context.replica.endpointId}: ${context.query}`);
});

// Example: Metrics hooks
router.registerHook('afterQuery', async (context) => {
    metrics.increment('replication.query', {
        replica: context.replica.endpointId
    });
    metrics.timing('replication.query_duration', context.data.durationMs, {
        replica: context.replica.endpointId
    });
    metrics.gauge('replication.row_count', context.data.rowCount, {
        replica: context.replica.endpointId
    });
});

// Example: Read consistency hooks
router.registerHook('beforeSelectReplica', async (context) => {
    // Force read from primary for certain queries
    if (context.query.toLowerCase().includes('for update') ||
        context.query.toLowerCase().includes('nowait')) {
        // This would require modifying the selection logic
        // In practice, we'd throw an error or have a special hook
        console.log('Warning: Query contains locking clause but will be routed to replica');
    }
});

// Example: Replica selection modification
router.registerHook('beforeSelectReplica', async (context) => {
    // Implement custom load balancing based on application logic
    // For example, route certain users to specific replicas
    // This would require extending the hook context with user info
});

// Example: Error handling hooks
router.registerHook('onReplicaFailure', async (context) => {
    await sendAlert({
        severity: 'warning',
        message: `Query failed on replica ${context.replica.endpointId}`,
        details: {
            query: context.query,
            error: context.data.error.message,
            replica: context.replica.endpointId
        }
    });
});
```

### Acceptance Criteria
- [ ] Hook types and interfaces defined
- [ ] Hook runner implemented
- [ ] Hooks invoked at all routing points
- [ ] Error handling
- [ ] Registration API
- [ ] Integration with router
- [ ] Unit tests (100%)

---

## 📅 Timeline

| Week | Features |
|------|----------|
| 9-10 | #1 Weighted Selection, #2 Round-Robin |
| 10-11 | #3 Least-Connections, #5 Stats |
| 11-12 | #4 WAL LSN Tracking |
| 12-13 | #6 Read Routing Hooks |
| 14 | Integration & Testing |

---

## ✅ Success Criteria

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
- [src/replication/](./src/replication/)
- [PostgreSQL WAL](https://www.postgresql.org/docs/current/wal.html)
- [PostgreSQL Replication](https://www.postgresql.org/docs/current/warm-standby.html)
