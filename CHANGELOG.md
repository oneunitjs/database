# @oneunit/database

## [Unreleased]

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
