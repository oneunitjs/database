# @oneunit/database

## [Unreleased]

### 🚀 Features

- Default sharding strategy changed from `"hash"` to `"consistent-hash"` (minimizes key remapping when shards are added/removed)

### 🐛 Bug Fixes

#### P0 — Broken on Live Server

- **`pool.connect` callback incompatibility (P0-1)**: `pool.connect` override now supports both callback form (for internal `Pool.query()` use) and promise form (for `getClient()`). Previously the callback was dropped, causing `pool.query()`, `db.health()`, `db.check()`, and `db.schema.*` DDL helpers to deadlock indefinitely and leak pool slots.
- **`db.query(sql\`...\`)` drops fragment values (P0-2)**: Query config objects / fragments passed as the first argument now preserve their `values`. The second argument is treated strictly as options, never as a values override.
- **Optimistic locking breaks `updateById`/`deleteById` (P0-3)**: `updateById` and `deleteById` now fetch the current row via `findById` and inject the current `version` into the WHERE clause, fixing `TypeError: Optimistic locking requires "version" in the WHERE clause`.

#### P1 — Wrong Results / Data Risk

- **`queryAllShards` merge `"application"` returns `{}` (P1-1)**: `fanOutApi` now leaves results unmerged when `merge === "application"`, returning raw per-shard result objects instead of empty objects.
- **Failed 2PC poisons next query with `25P02` (P1-2)**: Distributed transaction execution context now issues `ROLLBACK` on the participant client when a statement fails, clearing the aborted transaction state before returning the connection to the pool.

#### P2 — Operational

- **Sharded `transaction()` without `shardKey` silently uses default shard (P2-1)**: Now throws `CrossShardTransactionError` when sharding is enabled and no `shardKey` is provided.
- **Missing `defaultShardId` only warns instead of throwing (P2-2)**: `normalizeShardingConfig` now throws `ShardConfigError` when sharding is enabled but `defaultShardId` is not configured.
- **Schema auto-sync undefined unless configured (P2-3)**: When sharding is enabled and no explicit `schemaSync` config is provided, schema auto-sync is auto-enabled using the primary pool as the coordination store.
- **Hardcoded leak timers not `.unref()`'d (P2-6)**: Leak-detection timers now call `.unref()` so they do not prevent Node.js process exit when idle.
- **Module-level `Set`s retain clients on shutdown (P2-7)**: `clearActiveConnections()` and `clearManualClients()` are called during `combinedShutdown`, clearing `activeConnections` (pool.ts) and `manualClients` (client.ts).
- **Query retry opt-in (P2-9)**: Default retry is now enabled for transient errors (serialization failures `40001`, deadlocks `40P01`, connection loss) with `retries: 3`, `baseDelay: 50ms`, `maxDelay: 2000ms`. Non-transient errors are never retried.

### 📚 Documentation

- Added `docs/limitation.md` — comprehensive catalog of all package limitations with severity ratings, impact analysis, and mitigation strategies
- Updated README.md Production Considerations and Known Limitations sections to link to `docs/limitation.md`
- Updated ARCHITECTURE.md Production Limitations section to link to `docs/limitation.md`

---

## [2.0.0-beta.1] - 2026-11-02

### 🚀 Features

#### Schema Auto-Sync
- **SchemaSyncService:** Orchestrates schema synchronization across all shards (lock → prepare → check → apply → validate → complete/rollback)
- **SchemaVersionTracker:** Per-shard schema version and checksum tracking
- **SchemaHashCalculator:** Deterministic SHA-256 schema checksums
- **MigrationApplier:** Applies and rolls back migrations on individual shards
- **SchemaValidator:** Cross-shard schema consistency validation
- **SyncLockManager:** Distributed sync locking to prevent concurrent sync operations
- **SyncStateStore:** Durable sync operation state persistence
- **SchemaRollbackCoordinator:** Coordinated rollback across shards
- **SchemaSyncHooks:** Lifecycle callbacks (sync start/complete/fail, shard start/complete/fail, validation start/complete, rollback start/complete)
- **SchemaSyncWorker:** Background periodic consistency checks with optional auto-repair
- **SchemaSyncReporter:** Sync progress and status reporting
- **SchemaSyncCLI:** CLI commands for manual sync operations
- **SchemaSyncSchema:** SQL schema for coordination DB tables (`schema_sync_state`, `shard_schema_versions`, `schema_sync_locks`, `migration_checksums`)
- Exported types: `SchemaSyncStatus`, `SchemaSyncOptions`, `SchemaSyncResult`, `SchemaConsistencyReport`, `ShardSchemaInfo`, `ShardSyncResult`, `SchemaSyncOperation`, `SchemaDiffType`, `SyncOperationId`, `SchemaSyncHooks`

#### Distributed Tx Connection Management
- **TransactionSemaphore:** Limits concurrent distributed transactions with queueing and timeout
- **PoolMonitor:** Per-pool connection utilization tracking with warning/error thresholds and listeners
- **CircuitBreaker:** Blocks new transactions when pools near exhaustion (CLOSED → OPEN → HALF_OPEN → CLOSED)
- **ConnectionThrottler:** Per-transaction connection limits to prevent pool monopolization
- **DistributedTransactionManager:** Managed entry point combining coordinator, semaphore, circuit breaker, and pool monitor

### 🐛 Bug Fixes

- **Security:** Fixed SQL injection vulnerability in `enumColumn()` helper — now properly escapes single quotes in enum values by replacing `'` with `''`
- **Security:** Sanitized prepared transaction IDs in `PREPARE TRANSACTION` — escaped single quotes to prevent SQL injection via `preparedId`
- **Connection Management:** Leaked pool connections are now auto-released (destroyed) after the 5-minute leak detection timeout instead of warn-only
- **Connection Management:** Leaked manual clients (from `getClient()`) are now auto-released after the 5-minute leak detection timeout instead of warn-only
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
- Added Schema Auto-Sync and Distributed Tx Connection Management sections to README.md
- Documented capabilities and appropriate use cases for all features in ARCHITECTURE.md

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
