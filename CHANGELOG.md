# @oneunit/database

## [Unreleased]

### 🚀 Features

- **Sharding:** Added `ShardRebalanceTracker` class for tracking shard rebalancing progress with support for creating, updating, querying, completing, failing, and canceling rebalance operations
- **Sharding:** Added `registerShardStrategy()` function to register custom shard strategies dynamically
- **Sharding:** Added `ShardRebalanceProgress` type for tracking rebalance state including moved/failed/skipped keys, percentage, status, and timestamps
- **Sharding:** Enhanced exports in `db.sharding` to include `rebalanceTracker` for managing rebalance operations
- **Column Helpers:** Added SQL injection protection in `enumColumn()` helper - now properly escapes single quotes in enum values
- **Replication:** Added comprehensive replication support with `ReplicationManager` and `ReplicationRouter` for primary/read-replica routing
- **Replication:** Added `readPreference` support: `"primary"`, `"replica-preferred"`, `"replica-only"` for flexible read routing
- **Replication:** Added replication lag monitoring via `pg_last_xact_replay_timestamp()` with configurable `lagThresholdMs`
- **Replication:** Added `isReplicaEligible()` helper to check if a replica meets health and lag requirements
- **Replication:** Added `validateReplicationConfig()` for configuration validation
- **Replication:** Exported error types: `ReplicationConfigError`, `ReplicaUnavailableError`, `PrimaryUnavailableError`, `WriteToReplicaError`, `ReadPreferenceError`
- **Failover:** Added `FailoverManager` for high-availability management with support for primary/replica failover
- **Failover:** Added `StaticHaProvider` and `PatroniHaProvider` for different HA provider implementations
- **Failover:** Added failover state machine with `createInitialFailoverState()`, `applyFailoverTransition()`, `nextFailoverState()` functions
- **Failover:** Added state helpers: `isTerminalFailoverState()`, `isTransientFailoverState()`
- **Failover:** Added event creation with `createFailoverEvent()`
- **Failover:** Exported HA types: `HaNodeRole`, `HaNodeStatus`, `FailoverState`, `HaNodeInfo`, `HaTopology`, `FailoverEvent`, `FailoverProviderConfig`, `HaProvider`, `FailoverConfig`, `ShardFailoverState`
- **Failover:** Exported error types: `FailoverError`, `FailoverNotSupportedError`, `SplitBrainPreventedError`, `PrimaryNotVerifiedError`
- **Distributed Transactions:** Added comprehensive 2PC support with `DistributedTransactionCoordinator` and `DistributedTransactionContext`
- **Distributed Transactions:** Added `DistributedTransactionError`, `DistributedTxNotFoundError`, `DistributedTxAlreadyResolvedError`, `MaxPreparedTransactionsError` error types
- **Distributed Transactions:** Added type exports: `ParticipantContext`, `DistributedTxCoordinatorOptions`, `DistributedTransactionExecuteOptions`
- **Distributed Transactions:** Added configurable timeouts (`prepareTimeoutMs`, `commitTimeoutMs`, `abortTimeoutMs`, `globalTimeoutMs`) via `DistributedTxTimeoutConfig` with `DEFAULT_DISTRIBUTED_TX_TIMEOUTS`
- **Distributed Transactions:** Added `DistributedTxTimeoutError` with phase, shardId, timeoutMs metadata; automatic abort on prepare timeout
- **Distributed Transactions:** Added `RetryPolicy` with exponential backoff + jitter, retryable error classification, `execute()` and `wrap()` patterns
- **Distributed Transactions:** Added `DistributedTxHookRunner` with 16 lifecycle hook types (before/after prepare/commit/abort, prepareFailed/commitFailed/abortFailed, onParticipantJoin/Leave, onStateChange, onTimeout)
- **Distributed Transactions:** Added `ParticipantHealthChecker` with per-node health status, consecutive failure/success tracking, health check execution
- **Distributed Transactions:** Added `PreparedTransactionScanner` for scanning pg_prepared_xacts and `TransactionCleanupManager` for diagnosing/cleaning up orphaned/old transactions
- **Distributed Transactions:** Exported new types: `DistributedTxTimeoutConfig`, `RetryPolicyOptions`, `DistributedTxHookType`, `DistributedTxHookContext`, `DistributedTxHookData`, `DistributedTxHook`, `DistributedTxCoordinatorHookAPI`, `DistributedTxHooks`, `DistributedTxLogger`, `ParticipantHealthStatus`, `ParticipantHealthOptions`, `ParticipantHealthLogger`, `PreparedTransactionInfo`, `PreparedTransactionFilter`, `PreparedTransactionScannerOptions`, `PreparedTransactionScannerLogger`, `CleanupResult`, `CleanupDiagnosticsOptions`, `CleanupDiagnosticsLogger`
- **QueryBuilder:** Added comprehensive enhancements including CTE (WITH clause) support via `with()` and `withRecursive()`
- **QueryBuilder:** Added Window Function support via `window()` method and helper functions `sql.rowNumber()`, `sql.rank()`, `sql.denseRank()`
- **QueryBuilder:** Added Complex Join support (RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN, LEFT OUTER JOIN, RIGHT OUTER JOIN)
- **QueryBuilder:** Added Subquery support in FROM, WHERE, SELECT via `from()`, `selectSubquery()`, `whereExists()`, `whereNotExists()`, `whereInSubquery()`, `whereNotInSubquery()`
- **QueryBuilder:** Added Nested WHERE groups via `whereGroup()` and `orWhereGroup()` methods
- **QueryBuilder:** Added Query Combination (UNION, UNION ALL, INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL)
- **QueryBuilder:** Added CASE expression support via `selectCase()` and `sql.caseWhen()`
- **QueryBuilder:** Added DISTINCT ON support via `distinctOn()`
- **QueryBuilder:** Added Row Locking support (FOR UPDATE, FOR NO KEY UPDATE, SKIP LOCKED) via `forUpdate()`, `forNoKeyUpdate()`, `skipLocked()`
- **QueryBuilder:** Added JSON operator support via `whereJson()`, `whereJsonContains()`, `whereJsonContained()` and SQL helpers
- **QueryBuilder:** Added Query Logging Context via `withQueryContext()` and `timeout()`
- **Batch Operations:** Added `selectMany()`, `updateMany()`, `deleteMany()` batch methods
- **Model Layer:** Added type coercion (`coerce` + `coerceTypes` options) for automatic type conversion on insert/update
- **Model Layer:** Added SQL JOIN support via `join`/`innerJoin`/`leftJoin`/`rightJoin`/`fullJoin`/`crossJoin` methods + `{ queryBuilder }` option
- **Model Layer:** Added automatic transaction support for single ops via `{ atomic: true }` option
- **Model Layer:** Added `restoreById` returning single row

### 🐛 Bug Fixes

- **Security:** Fixed SQL injection vulnerability in `enumColumn()` helper — now properly escapes single quotes in enum values by replacing `'` with `''`
- **Connection Management:** Fixed connection leak detection with automatic warnings after 5 minutes of unreleased connections
- **Pool Health:** Enhanced health checks to include `pendingAcquire` and `max` pool statistics
- **Retry Logic:** Prevented retry storms by excluding pool exhaustion errors (`POOL_EXHAUSTED`, `ECONNREFUSED`) from automatic retries
- **Cursor Management:** Added automatic cleanup and leak detection for server-side cursors
- **Manual Client Tracking:** Added connection leak detection for manually acquired clients via `getClient()`
- **Sharding:** Fixed shard routing to properly handle custom shard strategy names

### 🔧 Improvements

- Added `validateConnections` option to `createPool()` for connection validation on checkout (disabled by default for backward compatibility)
- Added `getActiveManualClientCount()` method to `ClientApi` for monitoring manually acquired connections
- Enhanced pool event logging with connection statistics
- Added timeout for cursor operations to prevent hanging

### 📚 Documentation

- Added comprehensive use case guidance in README.md
- Added "When to Use Each Layer" section in ARCHITECTURE.md
- Documented capabilities and appropriate use cases for all features

---

## [1.0.0] - 2026-10-31

### 🚀 Initial Release

First stable release of @oneunit/database with:

- Core PostgreSQL client with connection pooling
- SQL query builder and tagged template
- Table models with CRUD operations
- Schema management and migrations
- Connection pooling with health checks
- Streaming and cursor support
- Transaction management with savepoints
- Sharding with 9 routing strategies
- Read replica support with lag-aware routing
- Online resharding with durable state
- Cross-shard distributed transactions (2PC)
- High-availability failover support
- Comprehensive error handling and classification
- Full TypeScript support
- Metrics and instrumentation hooks
