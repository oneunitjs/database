# @oneunit/database

## [Unreleased]

### 🚀 Features

### 🐛 Bug Fixes

- **Connection Management:** Fixed connection leak detection with automatic warnings after 5 minutes of unreleased connections
- **Pool Health:** Enhanced health checks to include `pendingAcquire` and `max` pool statistics
- **Retry Logic:** Prevented retry storms by excluding pool exhaustion errors (`POOL_EXHAUSTED`, `ECONNREFUSED`) from automatic retries
- **Cursor Management:** Added automatic cleanup and leak detection for server-side cursors
- **Manual Client Tracking:** Added connection leak detection for manually acquired clients via `getClient()`

### 🔧 Improvements

- Added `validateConnections` option to `createPool()` for connection validation on checkout (disabled by default for backward compatibility)
- Added `getActiveManualClientCount()` method to `ClientApi` for monitoring manually acquired connections
- Enhanced pool event logging with connection statistics
- Added timeout for cursor operations to prevent hanging

### 📚 Documentation

- Added comprehensive use case guidance in README.md
- Added "When to Use Each Layer" section in ARCHITECTURE.md
- Documented limitations and appropriate use cases for all features

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
