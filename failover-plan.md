# Failover Layer Implementation Plan

> **Status:** Implemented | **Priority:** P0/P1 | **Target:** Weeks 1-12

Detailed implementation documentation for the Failover Layer features implemented in this workspace, per `working-issues.md` section 10.

All 9 failover features are implemented, unit-tested, wired into `FailoverManager`, and verified against `src/failover/index.ts`.

---

## 📋 Overview

9 failover features implemented:

| # | Feature | Priority | Effort | File | Status |
|---|---------|----------|--------|------|--------|
| 1 | Persistent failover state storage interface | P0 | 3-5d | `src/failover/failover-state-store.ts` | ✅ Implemented |
| 2 | Failover event history | P0 | 3-4d | `src/failover/failover-event-store.ts` | ✅ Implemented |
| 3 | Configurable failure detection thresholds | P0 | 2-3d | `src/failover/health-check-tracker.ts` | ✅ Implemented |
| 4 | Health-check failure classification | P0 | 2-3d | `src/failover/health-classifier.ts` | ✅ Implemented |
| 5 | Failover state recovery after restart | P0 | 3-5d | `src/failover/failover-recovery.ts` | ✅ Implemented |
| 6 | Failover hooks and callbacks | P1 | 3-4d | `src/failover/failover-hooks.ts` | ✅ Implemented |
| 7 | Connection invalidation after failover | P0 | 3-5d | `src/failover/connection-tracker.ts` | ✅ Implemented |
| 8 | Connection pool reconnection | P1 | 3-4d | `src/failover/pool-reconnector.ts` | ✅ Implemented |
| 9 | Fencing integration hooks | P1 | 5-7d | `src/failover/fencing.ts` / `fencing-manager.ts` | ✅ Implemented |

**Total Effort:** ~32-42 days

**Module:** `src/failover/` | **Public barrel:** `src/failover/index.ts`

---

## 🏗 Architecture

```
FailoverManager
├── stateStore?          FailoverStateStore (PgFailoverStateStore / MemoryFailoverStateStore)
├── recoveryManager?     FailoverRecoveryManager (locking, validation, reconciliation)
├── connectionTracker?   ConnectionTracker (track + invalidate per node)
├── eventStore?          FailoverEventStore (PgFailoverEventStore / MemoryFailoverEventStore)
├── classifier           HealthCheckClassifier (categorize + severity + retryable + failoverImmediately)
├── healthTracker?       HealthCheckTracker (consecutive failure/success thresholds)
├── hookRunner           FailoverHookRunner (lifecycle hook dispatch)
├── poolReconnector?     PoolReconnector (exponential backoff, atomic pool swap)
├── fencingManager?      FencingManager (multi-provider fencing orchestration)
└── provider             HaProvider (StaticHaProvider / PatroniHaProvider)
```

Database tables created via `FAILOVER_STATE_SCHEMA`:
- `_database_ha_state` — current failover state per shard
- `_database_ha_state_versions` — versioned audit trail of state changes
- `_database_ha_locks` — distributed recovery locks

Database tables created via `FAILOVER_EVENT_SCHEMA`:
- `_database_ha_events` — failover event history with indices

Database tables created via `FENCING_SCHEMA`:
- `_database_fencing_locks` — node fencing lock state
- `_database_fencing_history` — fencing activation/deactivation audit

---

## 🔌 Public API (from `src/failover/index.ts`)

**Classes:**
- `FailoverManager` — orchestrates failover lifecycle (health checks → state transitions → promotion → reconnection)
- `PgFailoverStateStore` — PostgreSQL-backed persistent failover state
- `MemoryFailoverStateStore` — in-memory state store (testing/single-process)
- `PgFailoverEventStore` — PostgreSQL-backed failover event history
- `MemoryFailoverEventStore` — in-memory event store
- `FailoverRecoveryManager` — state recovery with locking, validation, reconciliation
- `ConnectionTracker` — per-node connection tracking + invalidation
- `HealthCheckClassifier` — failure categorization
- `defaultHealthCheckClassifier` — default classifier instance
- `HealthCheckTracker` — consecutive failure/success threshold tracking
- `DEFAULT_FAIL_OVER_CONFIG` — default tracker configuration
- `FailoverHookRunner` — lifecycle hook dispatch
- `defaultHookLogger` — no-op hook logger
- `PgFailoverEventStore` — PostgreSQL-backed failover event history
- `MemoryFailoverEventStore` — in-memory event store
- `PoolReconnector` — pool reconnection with exponential backoff
- `DEFAULT_RECONNECTION_OPTIONS` — default reconnector configuration
- `AdvisoryLockFencingProvider` — PostgreSQL advisory-lock fencing
- `TableBasedFencingProvider` — table-only fencing (no advisory locks)
- `FencedError` — thrown when a node is fenced
- `defaultFencingHooks` — no-op fencing hooks
- `FencingManager` — multi-provider fencing orchestration
- `StaticHaProvider`, `PatroniHaProvider` — HA providers
- `FailoverError`, `FailoverNotSupportedError`, `SplitBrainPreventedError`, `PrimaryNotVerifiedError`, `isFailoverError`

**Types:**
- `FailoverContext`, `FailoverStateVersion`, `PersistedShardState`, `FailoverStateStore`
- `RecoveryResult`, `RecoveryAction`, `ValidationIssue`, `ValidationResult`, `FailoverRecoveryOptions`
- `TrackedConnection`, `ConnectionInvalidationResult`, `ConnectionTrackerOptions`
- `HealthCheckFailureCategory`, `HealthCheckFailureSeverity`, `ClassifiedFailure`, `HealthCheckFailure`
- `FailoverEventType`, `FailoverEvent` (as `FailoverHistoryEvent`), `FailoverEventFilter`, `FailoverEventStatistics`, `FailoverEventQueryResult`, `FailoverEventStore`
- `NodeHealthStatus`, `NodeFailureThresholds`, `RequiredFailoverConfig`
- `FailoverHookType`, `FailoverHookContext`, `FailoverHookData`, `FailoverHook`, `FailoverManagerHookAPI`, `FailoverHooks`
- `ReconnectionResult`, `ReconnectionOptions`, `PoolConfigProvider`
- `FencingProvider`, `FencingStatus`, `FencingOptions`, `FencingHooks`, `FencingActivationResult`
- `HaNodeRole`, `HaNodeStatus`, `FailoverState`, `HaNodeInfo`, `HaTopology`, `FailoverEvent`, `FailoverProviderConfig`, `HaProvider`, `FailoverConfig`, `ShardFailoverState`
- `DistributedTxStatus`, `ParticipantStatus`, `DistributedTxParticipantRecord`, `DistributedTxRecord`, `DistributedTxParticipant`, `DistributedTxOptions`, `DistributedTxResult`

---

## 1. Persistent Failover State Storage Interface ✅

**File:** `src/failover/failover-state-store.ts` | **Priority:** P0

### Interface: `FailoverStateStore`
```typescript
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

### Implementations
- `PgFailoverStateStore(pool, logger)` — PostgreSQL with optimistic concurrency on `version` column
- `MemoryFailoverStateStore()` — in-memory (testing / single-process)

### Schema (`FAILOVER_STATE_SCHEMA`)
- `_database_ha_state(shard_id PK, state_json JSONB, version BIGINT, updated_at TIMESTAMPTZ, updated_by TEXT)`
- `_database_ha_state_versions(id BIGSERIAL PK, shard_id, state_json, version, previous_version, changed_by, changed_at, reason)`
- `_database_ha_locks(lock_name PK, locked_by, locked_at, lock_expiry)`

### Integration with `FailoverManager`
- `FailoverManagerOptions.stateStore?: FailoverStateStore` — optional; when omitted the manager starts with fresh state (backward compatible)
- `initialize()` — recovers persisted state via `recoveryManager` (locking + validation) or a direct `loadState()` fallback
- `transitionTo()` — persists every state transition via `stateStore.transitionState()`; version conflicts are logged as warnings

### Acceptance Criteria ✅
- [x] `FailoverStateStore` interface defined
- [x] PostgreSQL implementation with optimistic concurrency
- [x] Memory implementation for testing
- [x] Integration with `FailoverManager`
- [x] Database schema (`FAILOVER_STATE_SCHEMA`)
- [x] Lock mechanism prevents concurrent modifications
- [x] Version history maintained
- [x] Unit tests in `test/failover/failover-state-store.test.ts`

---

## 2. Failover Event History ✅

**File:** `src/failover/failover-event-store.ts` | **Priority:** P0

### Interface: `FailoverEventStore`
```typescript
export interface FailoverEventStore {
    recordEvent(event: Omit<FailoverEvent, "id" | "timestamp">): Promise<FailoverEvent>;
    queryEvents(filter: FailoverEventFilter): Promise<FailoverEventQueryResult>;
    getEventsForShard(shardId: string, limit?: number): Promise<FailoverEvent[]>;
    getLatestEvents(limit?: number): Promise<FailoverEvent[]>;
    cleanupEvents(retentionDays: number): Promise<number>;
    getStatistics(filter?: FailoverEventFilter): Promise<FailoverEventStatistics>;
}
```

### Implementations
- `PgFailoverEventStore(pool, logger)` — PostgreSQL with shard/type/severity/time indices
- `MemoryFailoverEventStore()` — in-memory

### Schema (`FAILOVER_EVENT_SCHEMA`)
- `_database_ha_events(id, event_id UUID, timestamp, type, shard_id, node_id, details JSONB, severity, previous_state, new_state)`
- Indices: `idx_ha_events_shard_id`, `idx_ha_events_type`, `idx_ha_events_timestamp`, `idx_ha_events_severity`

### Integration with `FailoverManager`
- `FailoverManagerOptions.eventStore?: FailoverEventStore` — optional
- `transitionTo()` records a `state_change` event with previous/related state after every transition

### Acceptance Criteria ✅
- [x] Interface and PostgreSQL implementation
- [x] Memory implementation for testing
- [x] All query methods working (shard/type/severity/time filters, limit/offset)
- [x] Statistics aggregation (`getStatistics`)
- [x] Event retention cleanup (`cleanupEvents`)
- [x] Integration with `FailoverManager`
- [x] Events recorded for all state changes
- [x] Unit tests in `test/failover/failover-event-store.test.ts`

---

## 3. Configurable Failure Detection Thresholds ✅

**File:** `src/failover/health-check-tracker.ts` | **Priority:** P0

### Configuration: `RequiredFailoverConfig`
- `consecutiveFailureThreshold` (default 3)
- `unhealthyDurationThresholdMs` (default 30_000)
- `consecutiveSuccessThreshold` (default 2)
- `healthCheckIntervalMs` (default 5_000)
- `unhealthyHealthCheckMultiplier` (default 0.5)
- `nodeThresholds?: Record<string, NodeFailureThresholds>` — per-node overrides

### Class: `HealthCheckTracker(config, logger)`
- `recordFailure(nodeId)` — increments failures, resets successes; marks `degraded` at ≥ threshold/2, `unhealthy` at ≥ threshold
- `recordSuccess(nodeId)` — increments successes; recovers to `healthy` when threshold met + duration elapsed
- `shouldInitiateFailover(nodeId)` — true when consecutive failures ≥ threshold AND unhealthy duration elapsed
- `getEffectiveHealthCheckInterval(nodeId)` — base interval × unhealthy multiplier when unhealthy
- `getStatus(nodeId)` / `getAllStatuses()` — introspection

### Integration with `FailoverManager`
- `FailoverManagerOptions.healthTracker?: HealthCheckTracker` — optional; defaults constructed from `config`
- `performHealthCheck()` records success/failure on the tracker; consults `shouldInitiateFailover()`

### Acceptance Criteria ✅
- [x] Threshold configuration added
- [x] Per-node threshold overrides
- [x] Dynamic health check intervals
- [x] State transitions based on thresholds
- [x] Integration with `FailoverManager`
- [x] Unit tests in `test/failover/health-check-tracker.test.ts`

---

## 4. Health-Check Failure Classification ✅

**File:** `src/failover/health-classifier.ts` | **Priority:** P0

### Classifier: `HealthCheckClassifier`
Categorizes failures by error code (`ECONNREFUSED`, `ETIMEDOUT`, `ECONNRESET`, `ENOTFOUND`, `EAI_AGAIN`, `ENETUNREACH`, `EHOSTUNREACH`, `28P01`, `28000`, `42*` SQL states) and message content into:

| Category | Severity | Retryable | Failover Immediately |
|----------|----------|-----------|----------------------|
| `connection_refused` | critical | yes | yes |
| `connection_timeout` | transient | yes | no |
| `connection_reset` | persistent | yes | no |
| `authentication_failed` | critical | no | no |
| `query_timeout` | transient | yes | no |
| `query_error` | persistent | no | no |
| `network_unreachable` | persistent | yes | no |
| `dns_resolution_failed` | persistent | no | no |
| `server_error` | critical | no | yes |
| `unknown` | transient | yes | no |

### Integration with `FailoverManager`
- `performHealthCheck()` catch block classifies errors via `classifier.classify()`
- Logs at severity-appropriate level (`critical`→error, `persistent`→warn, else→info)
- Triggers immediate failover transition to `suspect` when `failoverImmediately`

### Acceptance Criteria ✅
- [x] Classifier implemented for all error types
- [x] Severity, retryable, failoverImmediately flags
- [x] Integration with `FailoverManager`
- [x] Unit tests for all error patterns in `test/failover/health-classifier.test.ts`

---

## 5. Failover State Recovery After Restart ✅

**File:** `src/failover/failover-recovery.ts` | **Priority:** P0

### Class: `FailoverRecoveryManager(stateStore, logger, options)`
`FailoverRecoveryOptions: { forceRecovery?, autoReconcile?, maxStateAgeMs? }`

- `recoverState(shardId)` — acquires recovery lock → loads state → validates → (optional auto-reconcile) → returns `RecoveryResult`; releases lock in `finally`
- `validateState(state)` — checks `state`, `shardId`, `_version`, `updatedAt` age, valid `FailoverState` value
- `reconcileState(state, validation)` — fixes missing/invalid fields; returns `null` when nothing to fix

### Integration with `FailoverManager`
- `FailoverManagerOptions.recoveryOptions?: FailoverRecoveryOptions` — optional
- `initialize()` — when `stateStore` configured, builds `FailoverRecoveryManager` and calls `recoverState()`; falls back to direct `loadState()` otherwise

### Acceptance Criteria ✅
- [x] Recovery manager implemented
- [x] State validation and reconciliation
- [x] Recovery lock mechanism (serialize concurrent recoveries)
- [x] Integration with `FailoverManager`
- [x] Recovery on restart
- [x] Unit tests in `test/failover/failover-recovery.test.ts`

---

## 6. Failover Hooks and Callbacks ✅

**File:** `src/failover/failover-hooks.ts` | **Priority:** P1

### Hook Types: `FailoverHookType`
`beforeStateChange`, `afterStateChange`, `beforeFailover`, `afterFailover`, `failoverFailed`, `beforePromotion`, `afterPromotion`, `promotionFailed`, `beforeDemotion`, `afterDemotion`, `demotionFailed`, `beforeHealthCheck`, `afterHealthCheck`, `onPrimaryChange`, `onTopologyChange`, `onFencingActivated`, `onFencingReleased`

### Class: `FailoverHookRunner(logger)`
- `invoke(type, context)` — calls all hooks for a type; hook errors are caught and logged (never break operations)
- `registerHook(type, hook)` / `unregisterHook(type, hook)` / `getHookCount(type)` / `clear()`

### Integration with `FailoverManager`
- `FailoverManagerOptions.hooks?: FailoverHookRunner` — optional; defaults to a new `FailoverHookRunner`
- Hooks invoked at: before/after state change, before/after failover, before/after promotion, before/after health check
- `hookAPI` exposes read-only `getCurrentState`, `getCurrentPrimary`, `getCurrentGeneration`, `getShardId`

### Acceptance Criteria ✅
- [x] Hook types and interfaces defined
- [x] Hook runner implemented
- [x] Hooks invoked at all lifecycle points
- [x] Error handling (errors don't break operations)
- [x] Registration/unregistration working
- [x] Unit tests in `test/failover/failover-hooks.test.ts`

---

## 7. Connection Invalidation After Failover ✅

**File:** `src/failover/connection-tracker.ts` | **Priority:** P0

### Class: `ConnectionTracker(options?)`
- `registerConnection(nodeId, shardId, client)` / `unregisterConnection(client)`
- `markInTransaction(client, transactionId)` / `markOutOfTransaction(client)` / `touch(client)`
- `getConnectionsForNode(nodeId)` / `getConnectionCount()`
- `invalidateConnections(nodeId)` — closes non-transaction connections; counts but skips in-transaction ones
- `invalidateConnectionsExcept(excludeNodeId)` / `invalidateAllConnections()`
- `stop()` — halts idle-cleanup timer

### Integration with `FailoverManager`
- `FailoverManagerOptions.connectionTracker?: ConnectionTracker` — optional
- `requestFailover()` invalidates connections to old primary before promotion; warns on active transactions
- Public API: `registerConnection`, `unregisterConnection`, `markInTransaction`, `markOutOfTransaction`, `touch`, `invalidatePrimaryConnections`, `getConnectionCount`

### Acceptance Criteria ✅
- [x] Connection tracker implemented
- [x] Connection registration/unregistration
- [x] Connection invalidation by node
- [x] Transaction tracking (in-transaction connections preserved)
- [x] Integration with `FailoverManager`
- [x] Unit tests in `test/failover/connection-tracker.test.ts`

---

## 8. Connection Pool Reconnection ✅

**File:** `src/failover/pool-reconnector.ts` | **Priority:** P1

### Class: `PoolReconnector(poolMap, configProvider, logger, options?, poolFactory?)`
- `reconnectPool(nodeId, newConfig?)` — atomic swap: validate new pool before closing old; deduplicates concurrent reconnections per node
- `reconnectPrimaryPool(nodeId)` / `reconnectReplicaPools(nodeIds)`
- Exponential backoff retry with `maxRetries`, `retryDelayMs`, `maxRetryDelayMs`
- Optional pool validation (`validatePool`) via `SELECT 1`
- `getPendingCount()` — in-flight reconnections

### Integration with `FailoverManager`
- `FailoverManagerOptions.poolReconnector?: PoolReconnector` — optional
- `performPromotion()` reconnects primary pool + all replica pools after successful promotion

### Acceptance Criteria ✅
- [x] Pool reconnector implemented
- [x] Retry logic with exponential backoff
- [x] Primary and replica pool reconnection
- [x] Atomic pool swap (validate before close)
- [x] Integration with `FailoverManager`
- [x] Unit tests in `test/failover/pool-reconnector.test.ts`

---

## 9. Fencing Integration Hooks ✅

**Files:** `src/failover/fencing.ts`, `src/failover/fencing-manager.ts` | **Priority:** P1

### Interface: `FencingProvider`
`activate`, `deactivate`, `isFenced`, `check`, `getFencedNodes`, `getStatus`, `cleanup`

### Implementations
- `AdvisoryLockFencingProvider(pool, logger, hooks?)` — `pg_advisory_lock` + tracking table
- `TableBasedFencingProvider(pool, logger, hooks?)` — table-only fencing (no advisory locks)
- `FencingManager(defaultProvider, logger)` — multi-provider orchestration (`registerProvider`, `use`, `activateAll`, `deactivateAll`, `check`, `isFenced`, `getFencedNodes`, `getStatus`, `cleanup`)

### Schema (`FENCING_SCHEMA`)
- `_database_fencing_locks(node_id PK, lock_id, reason, expires_at, priority, metadata JSONB, activated_at, activated_by)`
- `_database_fencing_history(id BIGSERIAL PK, node_id, action, reason, priority, metadata, performed_by, performed_at)`

### Integration with `FailoverManager`
- `FailoverManagerOptions.fencingManager?: FencingManager` — optional
- `requestFailover()` activates fencing on old primary before failover proceeds (`activateAll`)
- `performHealthCheck()` calls `fencingManager.check(nodeId)`; fenced nodes are marked down and skipped
- Public API: `isFenced(nodeId)`, `getFencingStatus(nodeId)`

### Acceptance Criteria ✅
- [x] `FencingProvider` interface
- [x] `AdvisoryLockFencingProvider`
- [x] `TableBasedFencingProvider`
- [x] `FencingManager`
- [x] Integration with `FailoverManager`
- [x] Integration with health check / query execution path
- [x] Database schema (`FENCING_SCHEMA`)
- [x] Unit tests in `test/failover/fencing.test.ts` and `test/failover/fencing-manager.test.ts`

---

## 🏗 Failover State Machine (existing)

**File:** `src/failover/failover-state-machine.ts` (pre-existing)

States: `healthy` → `suspect` → `primary_unavailable` → `failover_requested` → `promoting` → `verifying` → `primary_changed` → `recovering` → `failed`

Transitions driven by `nextFailoverState(state, trigger)` and applied via `applyFailoverTransition(state, toState, event)`.

---

## 📡 FailoverManager Public Surface

Configuration (`FailoverManagerOptions`):
- `shardId`, `primaryPool`, `config`, `logger`
- `onStateChange?(state)`, `onFailoverComplete?(newPrimary)`
- `stateStore?`, `recoveryOptions?`, `connectionTracker?`, `eventStore?`, `healthTracker?`, `hooks?`, `poolReconnector?`, `fencingManager?`

Read-only getters: `currentState`, `currentGeneration`, `currentPrimary`, `failoverState`

Lifecycle: `initialize()`, `shutdown()`

Manual operations: `requestManualFailover(reason)`, `reportPrimaryFailure(reason)`, `reportPrimarySuccess()`

Connection management: `registerConnection`, `unregisterConnection`, `markInTransaction`, `markOutOfTransaction`, `touch`, `invalidatePrimaryConnections`, `getConnectionCount`

Fencing introspection: `isFenced(nodeId)`, `getFencingStatus(nodeId)`

Topology: `getTopology()`

---

## 🧪 Testing

All 9 features are unit-tested in `test/failover/`:

| Test file | Coverage |
|-----------|----------|
| `failover-state-machine.test.ts` | state machine transitions (25 tests) |
| `failover-state-store.test.ts` | `PgFailoverStateStore` + `MemoryFailoverStateStore` (21 tests) |
| `failover-event-store.test.ts` | `PgFailoverEventStore` + `MemoryFailoverEventStore` (14 tests) |
| `health-check-tracker.test.ts` | `HealthCheckTracker` thresholds (11 tests) |
| `health-classifier.test.ts` | `HealthCheckClassifier` error patterns (16 tests) |
| `failover-recovery.test.ts` | `FailoverRecoveryManager` lock/validate/reconcile (11 tests) |
| `failover-hooks.test.ts` | `FailoverHookRunner` lifecycle hooks (9 tests) |
| `connection-tracker.test.ts` | `ConnectionTracker` track/invalidate (13 tests) |
| `pool-reconnector.test.ts` | `PoolReconnector` retry/swap (7 tests) |
| `fencing.test.ts` | `AdvisoryLockFencingProvider` + `TableBasedFencingProvider` (12 tests) |
| `fencing-manager.test.ts` | `FencingManager` multi-provider orchestration (13 tests) |

**Total:** 152 tests across 11 files.

Run with: `npx vitest run test/failover/`

Verification:
- `npm run typecheck` — clean (strict TypeScript)
- `npm run build` — clean
- `npx vitest run test/failover/` — 152/152 pass

---

## 🔌 Usage Example

```typescript
import {
    FailoverManager,
    PgFailoverStateStore,
    ConnectionTracker,
    HealthCheckClassifier,
    HealthCheckTracker,
    FailoverHookRunner,
    PoolReconnector,
    AdvisoryLockFencingProvider,
    FencingManager,
    TableBasedFencingProvider,
} from "@oneunit/database";

// Optional durable components
const stateStore = new PgFailoverStateStore(coordinationPool);
const eventStore = new PgFailoverEventStore(coordinationPool);
const connectionTracker = new ConnectionTracker({ logger });
const healthTracker = new HealthCheckTracker(config, logger);
const hookRunner = new FailoverHookRunner(logger);
const poolReconnector = new PoolReconnector(poolMap, configProvider, logger);
const fencingProvider = new TableBasedFencingProvider(coordinationPool, logger);
const fencingManager = new FencingManager(fencingProvider, logger);

const failover = new FailoverManager({
    shardId: "shard-0",
    primaryPool: primaryPool,
    config: failoverConfig,
    logger,
    stateStore,
    recoveryOptions: { autoReconcile: true },
    connectionTracker,
    eventStore,
    healthTracker,
    hooks: hookRunner,
    poolReconnector,
    fencingManager,
});

await failover.initialize();

// Callers register connections when acquiring clients
failover.registerConnection("primary", "shard-0", client);
failover.markInTransaction(client, "tx-1");
failover.touch(client);

// Before executing queries on a node
if (await failover.isFenced(nodeId)) {
    continue; // skip fenced node
}

await failover.shutdown();
```

---

## 🔁 Backward Compatibility

All `FailoverManagerOptions` fields beyond the pre-existing ones (`shardId`, `primaryPool`, `config`, `logger`, `onStateChange`, `onFailoverComplete`) are optional. Existing callers that do not configure the new durable components behave exactly as before — state starts fresh, health checks run, no state store, no event store, no fencing, no pool reconnection. No breaking changes.

---

## 📚 References

- [working-issues.md](./working-issues.md) — Feature tracking (section 10: Failover Layer)
- [PLAN.md](./PLAN.md) — Production readiness implementation plan
- [ARCHITECTURE.md](./ARCHITECTURE.md) — Technical architecture (failover module map)
- [README.md](./README.md) — Usage documentation
- [src/failover/](./src/failover/) — Failover layer source
