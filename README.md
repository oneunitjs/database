<p align="center">
  <strong>@oneunit/database</strong>
  <br/>
  A batteries-included PostgreSQL client for Node.js
</p>

<p align="center">
  <a href="https://www.npmjs.com/@oneunit/database"><img src="https://img.shields.io/npm/v/@oneunit/database?color=0969da&label=npm" alt="npm version"></a>
  <a href="https://github.com/oneunitjs/database/blob/main/packages/database/LICENSE"><img src="https://img.shields.io/npm/l/@oneunit/database?color=22863a" alt="license"></a>
  <img src="https://img.shields.io/badge/node-%3E%3D20-417e38" alt="node version">
  <img src="https://img.shields.io/badge/types-included-3178c6" alt="types included">
</p>

<p align="center">
  Connection pooling · Parameterized queries · Transactions &amp; savepoints · Streaming &amp; cursors<br/>
  Query builder · Models · Schema helpers · Migrations · Health checks · Metrics<br/>
  Sharding · Replication · Online resharding · Distributed transactions · Failover
</p>

---

Built on [`pg`](https://node-postgres.com), with optional [`pg-cursor`](https://github.com/brianc/node-pg-cursor) and [`pg-query-stream`](https://github.com/brianc/node-pg-query-stream) for streaming workloads. Ships as a standalone ESM package — no sibling-package runtime dependencies.

> **Monorepo** — [github.com/oneunitjs/database](https://github.com/oneunitjs/database)

> **Latest Release:** See [CHANGELOG.md](CHANGELOG.md) for recent changes.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [When to Use Which Features](#when-to-use-which-features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Idle Transaction Timeout](#idle-transaction-timeout)
- [Queries](#queries)
  - [Prepared Statements](#prepared-statements)
  - [Manual Checkout](#manual-checkout)
  - [Timeouts & Retries](#timeouts--retries)
- [Transactions](#transactions)
- [SQL Fragments & Query Builder](#sql-fragments--query-builder)
- [Models](#models)
- [Batch Inserts](#batch-inserts)
- [Streaming & Cursors](#streaming--cursors)
- [Schema Helpers](#schema-helpers)
- [Migrations](#migrations)
- [Health Checks & Shutdown](#health-checks--shutdown)
- [Sharding](#sharding)
- [Replication](#replication)
- [Online Resharding (Data Migration)](#online-resharding-data-migration)
- [Distributed Transactions](#distributed-transactions)
- [Failover](#failover)
- [Error Handling](#error-handling)
- [Logging & Metrics](#logging--metrics)
- [TypeScript](#typescript)
- [Sub-path Exports](#sub-path-exports)
- [Examples](#examples)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture Overview

`@oneunit/database` is built around a **composition model** where `createDatabase()` wires together independent factories for each concern:

| Subsystem | Responsibility |
| :--- | :--- |
| `client/` | Connection pooling, query execution, retry, health, shutdown, streaming, cursors, batch, hooks, metrics |
| `query/` | SQL tagged template and fluent query builder |
| `model/` | Table-level CRUD abstraction |
| `schema/` | DDL helpers and versioned migration runner |
| `sharding/` | Multi-shard routing (9 strategies), shard health, metrics, migration tracking |
| `replication/` | Primary / read-replica routing with lag-aware health checks |
| `resharding/` | Durable online data migration (plan → copy → validate → cutover → finalize) |
| `distributed-tx/` | Cross-shard two-phase commit (2PC) coordinator |
| `failover/` | High-availability provider and state machine |

**Key characteristics**:

* **Composition over inheritance** — no class hierarchies; each subsystem is a pure factory function.
* **Explicit configuration** — `loadDatabaseConfig()` resolves environment variables, defaults, and explicit overrides.
* **Parameterized SQL** — all queries use `$1, $2…` placeholders; the `sql` tag and `QueryBuilder` enforce safety.
* **Retry-aware** — optional retry logic via `createRetry()` for transient errors only.
* **Idempotent lifecycle** — `shutdown()` and `migrate.run()` are safe to call repeatedly.
* **Atomic write fencing** — shared advisory locks + durable write gate table prevent TOCTOU races during resharding cutover.
* **Observability** — hooks and metrics collectors provide before/after hooks and per-shard counters.

The full architectural diagram and module interaction details are in **ARCHITECTURE.md**, which explains data flow, module map, error architecture, instrumentation, and the dependency graph.

---

## When to Use Which Features

`@oneunit/database` provides features for applications of all scales — from simple CRUD apps to distributed systems. Use this guide to determine which features your application needs.

### Quick Decision Matrix

| Use Case | Core Client | Query Builder | Models | Schema/Migrations | Sharding | Replication | Resharding | Distributed TX | Failover |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Simple App** — Single DB, basic CRUD | ✅ Required | ✅ Recommended | ✅ Optional | ✅ Recommended | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Growing App** — Single DB, needs abstractions | ✅ Required | ✅ Recommended | ✅ Recommended | ✅ Required | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Read-Heavy App** — Single primary, multiple replicas | ✅ Required | ✅ Recommended | ✅ Optional | ✅ Recommended | ❌ | ✅ Required | ❌ | ❌ | ❌ |
| **Scaled App** — Multiple shards, partitioned data | ✅ Required | ✅ Recommended | ✅ Optional | ✅ Required | ✅ Required | ✅ Recommended | ✅ Optional | ❌ | ❌ |
| **Enterprise App** — Multi-shard, HA, zero-downtime changes | ✅ Required | ✅ Recommended | ✅ Optional | ✅ Required | ✅ Required | ✅ Required | ✅ Required | ✅ Required | ✅ Required |

### Feature Descriptions by Use Case

#### 🟢 Core Features (Always Use)

These are the foundation of the package and should be used by all applications:

- **`client/`** — Connection pooling, query execution, transactions, health checks
  - Use for: All database operations
  - Example: `db.query()`, `db.transaction()`, `db.health()`

- **`query/`** — SQL tagged template and query builder
  - Use for: Safe SQL construction, preventing injection
  - Example: `sql\`SELECT * FROM users WHERE id = ${id}\` `

#### 🟡 Common Features (Most Applications)

For typical applications that need data modeling and schema management:

- **`model/`** — Table-level CRUD abstraction
  - Use for: Reducing boilerplate for common operations
  - When: Your app has multiple tables with similar patterns
  - Example: `db.model("users").findById(id)`

- **`schema/` & `migrate/`** — DDL helpers and versioned migrations
  - Use for: Managing database schema evolution
  - When: You need to deploy schema changes over time
  - Example: `db.schema.create.table("users", {...})`

- **`batch/`** — Chunked multi-row inserts
  - Use for: Bulk data loading
  - When: Inserting large datasets efficiently
  - Example: `db.batch.insertMany("events", rows)`

- **`stream/` & `cursor/`** — Streaming large result sets
  - Use for: Processing large datasets without loading into memory
  - When: Query results exceed available memory
  - Requires: `pg-cursor` and/or `pg-query-stream` peer dependencies

#### 🟠 Scale Features (Multi-Database Applications)

For applications that have outgrown a single database:

- **`replication/`** — Primary / read-replica routing
  - Use for: Scaling read operations across multiple replicas
  - When: Your read load exceeds what a single DB can handle
  - Solves: Read scalability, read-heavy workloads
  - Example: `db.query(sql, values, { readPreference: "replica-preferred" })`

- **`sharding/`** — Multi-shard routing with 9 strategies
  - Use for: Distributing data across multiple databases (shards)
  - When: Your data volume or write throughput exceeds single DB limits
  - Solves: Write scalability, data partitioning
  - Choose strategy based on:
    - **`hash`** — Simple, even distribution (remaps on shard count change)
    - **`consistent-hash`** — Minimal key remapping when adding/removing shards
    - **`range`** — Ordered data (time-series, IDs)
    - **`directory`** — Explicit key-to-shard mapping
    - **`tenant`** — Multi-tenant SaaS (tenant ID to shard)
    - **`composite`** — Multi-component keys (tenant + user)
    - **`geographic`** — Data residency requirements
    - **`rendezvous`** — Highest random weight distribution
    - **`weighted`** — Uneven shard capacities

#### 🔴 Advanced Features (Distributed Systems)

For sophisticated distributed applications requiring high availability and zero-downtime operations:

- **`resharding/`** — Online data migration
  - Use for: Moving data between shards without downtime
  - When: You need to add/remove shards or rebalance data
  - Solves: Shard rebalancing, capacity changes
  - Lifecycle: plan → copy → validate → cutover → finalize
  - Example: `db.resharding.plan()`, `db.resharding.start()`, `db.resharding.cutover()`

- **`distributed-tx/`** — Cross-shard two-phase commit (2PC)
  - Use for: Atomic operations across multiple shards
  - When: You need ACID transactions spanning multiple databases
  - Solves: Cross-shard data consistency
  - Requires: `max_prepared_transactions > 0` on all PostgreSQL servers
  - Example: `db.sharding.distributedTransaction(async (tx) => {...})`

- **`failover/`** — High-availability provider and state machine
  - Use for: Automatic failover when primary database fails
  - When: You need high availability with minimal downtime
  - Solves: Primary database failure recovery
  - States: ACTIVE → DRAINING → DRAINED → REMOVED
  - Example: Configure with replication for automatic primary/replica switching

---

## Installation

```bash
npm install @oneunit/database
```

For streaming support, add the optional peer dependencies:

```bash
npm install pg-cursor pg-query-stream
```

---

## Quick Start

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase();

// Simple query
const { rows } = await db.query(
    "SELECT * FROM users WHERE status = $1",
    ["active"],
);

// Transaction with savepoints
await db.transaction(async (client) => {
    await client.query(
        "INSERT INTO users (email) VALUES ($1)",
        ["ada@example.com"],
    );
});

// Graceful shutdown
await db.shutdown();
```

### Quick Start for Different Use Cases

#### Simple Application (Single Database)

Most applications start here — a single PostgreSQL database with basic CRUD:

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    connectionString: "postgresql://user:pass@localhost:5432/app",
    max: 10, // connection pool size
});

// Query
const users = await db.query("SELECT * FROM users WHERE status = $1", ["active"]);

// Transaction
await db.transaction(async (client) => {
    await client.query("INSERT INTO users (email) VALUES ($1)", ["test@example.com"]);
    await client.query("UPDATE stats SET count = count + 1");
});

// Shutdown
await db.shutdown();
```

#### Read-Heavy Application (Primary + Replicas)

Scale reads by routing queries to replicas:

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    replication: {
        enabled: true,
        mode: "replica-preferred", // Read from replicas when available
        primary: {
            id: "primary",
            connection: { connectionString: "postgresql://user:pass@localhost:5432/primary" },
        },
        replicas: [
            { id: "replica-1", connection: { connectionString: "postgresql://user:pass@localhost:5433/replica1" } },
            { id: "replica-2", connection: { connectionString: "postgresql://user:pass@localhost:5434/replica2" } },
        ],
        lagThresholdMs: 5000, // Exclude replicas lagging more than 5 seconds
    },
});

// Writes always go to primary
await db.query("INSERT INTO users (email) VALUES ($1)", ["test@example.com"]);

// Reads prefer replicas, fall back to primary if needed
const users = await db.query(
    "SELECT * FROM users WHERE status = $1",
    ["active"],
    { readPreference: "replica-preferred" }
);

// Force read from primary (for strong consistency)
const freshData = await db.query(
    "SELECT * FROM users WHERE id = $1",
    [userId],
    { readPreference: "primary" }
);
```

#### Multi-Tenant Application (Sharded by Tenant)

Partition data by tenant for isolation and scalability:

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    sharding: {
        enabled: true,
        strategy: "tenant", // Route by tenant ID
        defaultShardId: "shard-0",
        shards: [
            { id: "shard-0", connection: { connectionString: "postgresql://user:pass@localhost:5432/tenant_a" } },
            { id: "shard-1", connection: { connectionString: "postgresql://user:pass@localhost:5432/tenant_b" } },
        ],
    },
});

// Route queries to the correct shard using tenant ID as shard key
await db.query(
    "INSERT INTO users (tenant_id, email) VALUES ($1, $2)",
    [tenantId, "user@example.com"],
    { shardKey: tenantId }
);

// Transaction stays on one shard
const user = await db.transaction(
    async (client) => {
        await client.query(
            "INSERT INTO users (tenant_id, email) VALUES ($1, $2) RETURNING *",
            [tenantId, "user@example.com"]
        );
    },
    { shardKey: tenantId }
);

// Fan out to all shards for cross-tenant queries
const allUsers = await db.sharding.fanOut({
    query: "SELECT COUNT(*) FROM users",
    merge: "sum",
});
```

#### Enterprise Application (Sharded + Replicated + HA)

Combine all features for a robust, scalable, highly available system:

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    // Primary/replica for each shard
    sharding: {
        enabled: true,
        strategy: "hash",
        defaultShardId: "shard-0",
        shards: [
            {
                id: "shard-0",
                connection: { connectionString: "postgresql://user:pass@localhost:5432/shard0_primary" },
                replication: {
                    enabled: true,
                    mode: "replica-preferred",
                    replicas: [
                        { id: "shard-0-replica-1", connection: { connectionString: "..." } },
                    ],
                },
            },
            {
                id: "shard-1",
                connection: { connectionString: "postgresql://user:pass@localhost:5432/shard1_primary" },
                replication: {
                    enabled: true,
                    mode: "replica-preferred",
                    replicas: [
                        { id: "shard-1-replica-1", connection: { connectionString: "..." } },
                    ],
                },
            },
        ],
        // Coordination database for distributed operations
        coordination: {
            connectionString: "postgresql://user:pass@localhost:5432/coordinator",
        },
    },
    // HA provider for automatic failover
    failover: {
        enabled: true,
        provider: "postgres", // or custom provider
    },
});

// Cross-shard transaction with 2PC
const result = await db.sharding.distributedTransaction(async (tx) => {
    await tx.execute("shard-0", "UPDATE accounts SET balance = balance - $1 WHERE user_id = $2", [100, "user-1"]);
    await tx.execute("shard-1", "UPDATE accounts SET balance = balance + $1 WHERE user_id = $2", [100, "user-2"]);
});

// Online resharding to add a new shard
const rsharding = db.resharding;
await rsharding.init();
await db.sharding.addShard({
    id: "shard-2",
    connection: { connectionString: "postgresql://user:pass@localhost:5432/shard2_primary" },
});
await rsharding.plan(/* migration descriptor */);
await rsharding.start();
await rsharding.copyUnits();
await rsharding.validate();
await rsharding.cutover();
await rsharding.finalize();
```

## Recent Improvements

The package has undergone significant stability and reliability improvements:

### 🚀 New Features

- **🎯 Shard Rebalance Tracker** — Track shard rebalancing progress with automatic percentage calculation and state management
- **🔐 SQL Injection Fix** — `enumColumn()` now properly escapes single quotes in enum values
- **📡 Replication Support** — Complete primary/replica routing with lag-aware read preference
- **⚡ Failover Support** — High-availability with Static and Patroni providers
- **🔗 Distributed Transactions** — Two-phase commit across multiple shards
- **📝 Custom Shard Strategies** — Register custom strategies with `registerShardStrategy()`

### 🛡️ Production Readiness

- **Connection Leak Detection**: Automatic warnings after 5 minutes for unreleased connections (both manual and cursor-based)
- **Connection Validation**: Optional validation of connections on checkout to prevent stale connection errors
- **Enhanced Health Checks**: Pool statistics now include `pendingAcquire` and `max` for better monitoring
- **Smart Retry Logic**: Retries no longer trigger on pool exhaustion errors, preventing cascading failures
- **Cursor Cleanup**: Automatic cleanup and leak detection for server-side cursors

### 📊 Monitoring & Observability

```javascript
// Get active manually acquired clients (for leak detection)
const activeManualClients = db.getActiveManualClientCount();

// Enhanced health checks with pool statistics
const health = await db.check();
console.log(health.pool);
// {
//   total: 10,
//   idle: 5,
//   waiting: 2,
//   pendingAcquire: 0,
//   max: 20
// }
```

### 🔧 Configuration Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `validateConnections` | `boolean` | `false` | Validate connections on checkout (prevents stale connection errors) |
| `logger` | `object` | `undefined` | Logger with `info`, `warn`, `error`, `debug` methods for connection lifecycle events |
| `queryTimeout` | `number` | `0` | Default query timeout in milliseconds (application-level) |
| `timeout` | `number` | `0` | Alias for queryTimeout |
| `connectTimeout` | `number` | `0` | Connection acquisition timeout in milliseconds |

Connection settings are read from the environment by default, or pass them explicitly:

```javascript
const db = createDatabase({
    connectionString: "postgresql://user:pass@localhost:5432/app",
    max: 10,
    application_name: "billing-api",
});
```

---

## Configuration

All environment access is centralized in `loadDatabaseConfig()`. Explicit options always take precedence over environment values.

| Variable | Alias | Default | Description |
| :--- | :--- | :--- | :--- |
| `DATABASE_URL` | | — | PostgreSQL connection string |
| `DATABASE_HOST` | `PGHOST` | — | Hostname |
| `DATABASE_PORT` | `PGPORT` | — | Port |
| `DATABASE_NAME` | `PGDATABASE` | — | Database name |
| `DATABASE_USER` | `PGUSER` | — | User |
| `DATABASE_PASSWORD` | `PGPASSWORD` | — | Password |
| `DATABASE_POOL_MAX` | `DB_MAX_CONN`, `DB_POOL_SIZE` | `20` | Maximum pool size |
| `DATABASE_POOL_MIN` | `DB_MIN_CONN` | `0` | Minimum pool size |
| `DATABASE_CONNECTION_TIMEOUT` | `DB_CONN_TIMEOUT` | `5000` | Connect timeout (ms) |
| `DATABASE_IDLE_TIMEOUT` | `DB_IDLE_TIMEOUT` | `30000` | Idle client timeout (ms) |
| `DATABASE_QUERY_TIMEOUT` | `DB_QUERY_TIMEOUT` | — | Default query timeout (ms) |
| `DATABASE_STATEMENT_TIMEOUT` | `DB_STATEMENT_TIMEOUT` | — | PostgreSQL `statement_timeout` (ms) |
| `DATABASE_SSL` | `DB_SSL` | `false` | Enable TLS |
| `DATABASE_SSL_CA` | `DB_SSL_CA` | — | Path to CA certificate |
| `DATABASE_APP_NAME` | `DB_APP_NAME` | `Unknown App` | `application_name` sent to Postgres |

> **Tip** — Copy [`.env.example`](.env.example) and fill in values for local development.

```javascript
import { loadDatabaseConfig, createDatabase } from "@oneunit/database";

const config = loadDatabaseConfig({ max: 8 });
const db = createDatabase(config);
```

Credentials are **never** written to logs.

---

## Idle Transaction Timeout

PostgreSQL can terminate sessions that remain idle inside an open transaction. Enable this via the `idleTransactionTimeout` option on `createDatabase()` or `loadDatabaseConfig()`:

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    connectionString: "postgresql://user:pass@localhost:5432/app",
    idleTransactionTimeout: {
        enabled: true,
        timeout: 60_000, // milliseconds
    },
});
```

To disable:

```javascript
const db = createDatabase({
    idleTransactionTimeout: {
        enabled: false,
    },
});
```

| Property | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | Yes | Whether `idle_in_transaction_session_timeout` is applied to new connections |
| `timeout` | `number` | Yes when `enabled: true` | Idle-in-transaction timeout in milliseconds (1 to 2,147,483,647) |

**Behavior:**

- The timeout is applied as a PostgreSQL startup parameter (`-c idle_in_transaction_session_timeout=<ms>`) on every newly created physical connection
- It only terminates sessions that are idle inside an open transaction — it is **not** a general query execution timeout
- When PostgreSQL terminates the session, uncommitted changes are rolled back
- Configuration changes only take effect for new physical connections; existing sessions are unaffected
- This feature does **not** provide transaction resumption or automatic retries

**Important:** Disabling this package feature does not override or remove server-level, database-level, role-level, or session-level timeouts configured independently in PostgreSQL.

**Invalid values** (negative, zero when enabled, `NaN`, `Infinity`, or exceeding 2,147,483,647 ms) throw a `TypeError` or `RangeError` at configuration time.

---

## Queries

Every query is parameterized — use `$1, $2, ...` placeholders. Clients are acquired and released automatically.

```javascript
const result = await db.query(
    "SELECT id, email FROM users WHERE id = $1",
    [userId],
);

// Returns the first row, or null
const user = await db.queryOne(
    "SELECT * FROM users WHERE email = $1",
    ["ada@example.com"],
);
```

### Prepared Statements

```javascript
const findUser = db.prepare(
    "find_user",
    "SELECT * FROM users WHERE id = $1",
);

const { rows } = await findUser.execute([userId]);
```

### Manual Checkout

```javascript
const client = await db.getClient();
try {
    await client.query("SELECT 1");
} finally {
    db.releaseClient(client);
}
```

### Timeouts & Retries

Retries are **off by default**. Enable them only for idempotent statements. Only transient PostgreSQL errors (serialization failures, deadlocks, connection loss) are retried.

```javascript
await db.query("SELECT pg_advisory_lock($1)", [42], {
    timeout: 2_000,
    retry: { retries: 3, baseDelay: 50 },
});
```

---

## Transactions

Transactions always `BEGIN` / `COMMIT` / `ROLLBACK` and always release the client.

```javascript
const created = await db.transaction(async (client) => {
    const { rows } = await client.query(
        "INSERT INTO users (email) VALUES ($1) RETURNING *",
        ["ada@example.com"],
    );

    // Nested savepoint
    await db.savepoint(client, "profile", async (tx) => {
        await tx.query(
            "INSERT INTO profiles (user_id) VALUES ($1)",
            [rows[0].id],
        );
    });

    return rows[0];
});
```

**Options:**

```javascript
await db.transaction(work, {
    isolation: "SERIALIZABLE",
    readOnly: false,
    timeout: 5_000,
    retry: { retries: 2 },
});
```

Supported isolation levels: `READ UNCOMMITTED` · `READ COMMITTED` · `REPEATABLE READ` · `SERIALIZABLE`

Transactions are pinned to exactly one connection (or one shard when sharding is enabled). For cross-shard operations, use `distributedTransaction()` for 2PC across independent database servers.

---

## SQL Fragments & Query Builder

The QueryBuilder and SQL helpers provide a safe, expressive way to construct parameterized SQL queries.

### Tagged Template

The `sql` tagged template produces parameterized SQL with `.text` and `.values`. All interpolated values become `$1, $2, ...` parameters — never interpolated into SQL text.

```javascript
import { createDatabase, sql } from "@oneunit/database";

const db = createDatabase();
const statuses = ["active", "pending"];

const fragment = sql`
    SELECT * FROM users
    WHERE status IN (${sql.join(statuses)})
    AND created_at > ${since}
`;

await db.query(fragment.text, fragment.values);
```

**Safe identifier quoting:**

```javascript
// SAFE - identifier() properly quotes table/column names
sql`SELECT * FROM ${sql.identifier('users')}`

// SAFE - parameterized value
sql`SELECT * FROM users WHERE id = ${userId}`
```

**New SQL Helper Functions:**

- **Window Functions:** `sql.rowNumber()`, `sql.rank()`, `sql.denseRank()`
- **JSON Operators:** `sql.jsonGetText()`, `sql.jsonGet()`, `sql.jsonContains()`, `sql.jsonContained()`, `sql.jsonArrayLength()`, `sql.jsonObjectKeys()`, `sql.jsonPath()`
- **Aggregations:** `sql.countDistinct()`, `sql.stringAgg()`, `sql.arrayAgg()`, `sql.jsonAgg()`, `sql.jsonObjectAgg()`
- **CASE Expressions:** `sql.caseWhen()`
- **Utility:** `sql.quoteString()`

### Fluent Query Builder

The fluent QueryBuilder supports SELECT, INSERT, UPDATE, DELETE with comprehensive query construction.

**Basic SELECT:**

```javascript
const { rows } = await db.from("users")
    .select(["id", "email"])
    .where({ status: "active" })
    .orderBy("created_at", "desc")
    .limit(20);
```

**Common Table Expressions (CTEs):**

```javascript
const { rows } = await db.with('active_users',
    db.from('users').select('id', 'name').where({ active: true })
  )
  .with('recent_orders',
    db.from('orders').select('user_id', 'amount').where({ created_at: '> 2023-01-01' })
  )
  .select('au.name', 'ro.amount')
  .from('active_users au')
  .join('recent_orders ro', 'au.id = ro.user_id')
  .execute();

// Recursive CTE
db.withRecursive('numbers',
    db.from('series_generate_integer(1, 10)').select('n')
  )
  .select('*').from('numbers');
```

**Window Functions:**

```javascript
import { sql } from "@oneunit/database";

// Using helper functions
db.from('employees')
  .select('name', 'salary', sql.rowNumber(['department'], ['salary DESC']))
  .execute();

// Using window() method
db.window('ranking', {
    partitionBy: ['department'],
    orderBy: [{ column: 'salary', direction: 'DESC' }]
  })
  .select('name', 'salary', sql`RANK() OVER ranking`)
  .from('employees')
  .execute();
```

**Complex Joins:**

```javascript
db.from('users')
  .innerJoin('orders', 'users.id = orders.user_id')
  .leftJoin('profiles', 'users.id = profiles.user_id')
  .rightJoin('audit_logs', 'users.id = audit_logs.user_id')
  .fullOuterJoin('metadata', 'users.id = metadata.user_id')
  .crossJoin('categories')
  .select('users.name', 'orders.total', 'profiles.bio')
  .execute();
```

**Subqueries:**

```javascript
// Subquery in FROM
db.select('name', 'order_count')
  .from(
    db.from('orders').select('user_id', sql`COUNT(*)`).groupBy('user_id'),
    'user_orders'
  )
  .execute();

// Subquery in WHERE (EXISTS)
db.from('users')
  .whereExists(
    db.from('orders').where('orders.user_id = users.id')
  )
  .execute();

// Subquery in SELECT
db.from('users')
  .select(
    'name',
    db.from('orders').select(sql`COUNT(*)`).where('orders.user_id = users.id'), 'order_count'
  )
  .execute();
```

**Nested WHERE Groups:**

```javascript
db.from('users')
  .whereGroup(qb => qb
    .where({ status: "active" })
    .orWhere({ role: "admin" })
  )
  .orWhereGroup(qb => qb
    .where({ department: "engineering" })
    .where({ level: "> 5" })
  )
  .execute();
```

**Query Combination (UNION, INTERSECT, EXCEPT):**

```javascript
const activeUsers = db.from('users').select('id').where({ active: true });
const admins = db.from('users').select('id').where({ role: "admin" });

// UNION
activeUsers.union(admins).execute();

// UNION ALL
activeUsers.unionAll(admins).execute();

// INTERSECT
activeUsers.intersect(admins).execute();

// EXCEPT
activeUsers.except(admins).execute();
```

**CASE Expressions:**

```javascript
db.from('orders')
  .select(
    'id',
    'amount',
    db.selectCase('discount_level', cb => cb
      .when(sql`amount > 1000`, 'gold')
      .when(sql`amount > 500`, 'silver')
      .else('bronze')
    )
  )
  .execute();

// Or using sql.caseWhen()
db.from('orders')
  .select('id', sql.caseWhen()
    .when(sql`amount > 1000`, 'gold')
    .when(sql`amount > 500`, 'silver')
    .else('bronze'), 'discount_level'
  )
  .execute();
```

**DISTINCT ON:**

```javascript
db.from('messages')
  .distinctOn(['conversation_id'])
  .select('conversation_id', 'content', 'created_at')
  .orderBy('conversation_id', 'created_at DESC')
  .execute();
```

**Row Locking:**

```javascript
db.from('accounts')
  .where({ id: accountId })
  .forUpdate()
  .execute();

// FOR NO KEY UPDATE
db.from('accounts')
  .where({ id: accountId })
  .forNoKeyUpdate()
  .execute();

// SKIP LOCKED
db.from('jobs')
  .where({ status: "pending" })
  .forUpdate()
  .skipLocked()
  .execute();
```

**JSON Operators:**

```javascript
// Query JSON fields
db.from('users')
  .whereJson('metadata', '@>', { premium: true })
  .execute();

// JSON contains
db.from('users')
  .whereJsonContains('metadata', { tags: ['vip'] })
  .execute();

// Using sql helpers
db.from('users')
  .where(sql.jsonGetText('metadata', 'name'), 'John')
  .execute();
```

**Batch Operations:**

```javascript
// Batch SELECT
await db.batch.selectMany("users", [1, 2, 3, 4, 5], { chunkSize: 100 });

// Batch UPDATE
await db.batch.updateMany("users", updates, { chunkSize: 100 });

// Batch DELETE
await db.batch.deleteMany("users", ids, { chunkSize: 100 });
```

**Query Logging Context:**

```javascript
db.from('users')
  .withQueryContext({ requestId: 'abc-123', userId: '42' })
  .execute();

// Access context (for logging)
const context = db.from('users').withQueryContext({ traceId: 'xyz' }).getQueryContext();
```

**Transaction Timeout:**

```javascript
db.from('users')
  .timeout(5000) // 5 second timeout
  .execute();
```

> **Identifiers are quoted.** Values are never interpolated into SQL text.

**Shard Pinning:**

```javascript
await db.from("orders")
    .where({ user_id })
    .withShard(user_id)
    .execute();
```

> **See Also:** [QUERY_BUILDER_IMPROVEMENTS.md](QUERY_BUILDER_IMPROVEMENTS.md) for comprehensive documentation of all QueryBuilder features, including advanced use cases, examples, and migration guides.

---

## Models

CRUD helpers for table-level operations:

```javascript
const users = db.model("users");

await users.insert({ email: "ada@example.com" });
await users.findOne({ email: "ada@example.com" });
await users.updateById(id, { status: "active" });
await users.deleteById(id);
await users.count({ status: "active" });
```

**Features:**

- **Table-level operations** - Full CRUD support via `join()`/`innerJoin()`/`leftJoin()` methods and `{ queryBuilder }` option
- **Complex conditions** - `find()` supports operators (`$gt`, `$lt`, `$in`, `$like`, `$ne`, `$null`, AND/OR groups)
- **Automatic transactions** - Use `{ atomic: true }` for single operations, or `{ client }` to join an existing transaction
- **Validation** - Built-in via `validate` option (e.g., Zod, TypeBox)
- **Soft deletes** - `softDelete` option with `withDeleted`/`forceDelete`/`restore`
- **Automatic timestamps** - `timestamps` option with customizable field names
- **Lifecycle hooks** - `hooks` option for before/after Create/Update/Delete
- **Relationships** - `relationships` option with eager loading via `findWithIncludes`
- **Cascade delete** - `deleteWithCascade`/`deleteByIdWithCascade`
- **Type coercion** - `coerce` + `coerceTypes` options for automatic type conversion

```javascript
// Transaction with automatic BEGIN/COMMIT/ROLLBACK
await users.insert({ email: "test@example.com" }, { atomic: true });

// Join an existing transaction
await db.transaction(async (client) => {
    const user = await users.insert({ email: "test@example.com" }, { client });
});

// JOIN query across tables
const qb = users.innerJoin("orders", "users.id", "orders.user_id");
const results = await users.find({ "orders.total": { $gt: 100 } }, { queryBuilder: qb });

**Available methods:** `find`, `findOne`, `findById`, `findWithIncludes`, `findOneWithIncludes`, `findByIdWithIncludes`, `findWithDeleted`, `insert`, `insertMany`, `update`, `updateById`, `delete`, `deleteById`, `remove`, `removeById`, `forceDelete`, `forceDeleteById`, `restore`, `restoreById`, `count`, `exists`, `deleteWithCascade`, `deleteByIdWithCascade`, `join`, `innerJoin`, `leftJoin`, `rightJoin`, `fullJoin`, `crossJoin`. Configuration getters: `softDeleteEnabled`, `timestampsEnabled`, `coerceEnabled`.

> **See Also:** [MODEL_LAYER_LIMITATIONS.md](MODEL_LAYER_LIMITATIONS.md) for detailed Model layer features and capabilities.

---

## Batch Inserts

```javascript
await db.batch.insertMany("events", rows, {
    chunkSize: 500,
    onConflict: { columns: ["id"], do: "nothing" },
    returning: ["id"],
});
```

Rows are parameterized and chunked so the statement stays under PostgreSQL's 65 535 parameter limit.

For sharded batch inserts, route each group to its target shard:

```javascript
await db.sharding.batchInsertMany([
    { table: "events", rows: shard0Rows, shardKey: "user-1" },
    { table: "events", rows: shard1Rows, shardKey: "user-2" },
]);
```

---

## Streaming & Cursors

Use streams or cursors for large result sets.

```javascript
// Node.js readable stream
const stream = await db.stream(
    "SELECT * FROM audit_logs WHERE created_at < $1",
    [cutoff],
);
stream.on("data", (row) => processRow(row));
stream.on("error", console.error);

// Async iterator (cursor-based)
const cursor = await db.cursor("SELECT * FROM analytics_events");
for await (const row of cursor) {
    processRow(row);
}
```

> The pooled client is released automatically on `end`, `error`, `close`, or iterator completion.

Requires optional peer dependencies: `pg-query-stream` (streams), `pg-cursor` (cursors).

Shard-aware streaming:

```javascript
const stream = await db.stream(
    "SELECT * FROM events WHERE user_id = $1",
    [userId],
    { shardKey: userId },
);
```

---

## Schema Helpers

```javascript
import { createDatabase, id, timestamp } from "@oneunit/database";

const db = createDatabase();

await db.schema.create.table("users", {
    ...id(),
    email: "VARCHAR(255) NOT NULL",
    status: "VARCHAR(50) DEFAULT 'active'",
    ...timestamp("created_at", "updated_at"),
});

await db.schema.create.index("idx_users_email", "users", ["email"]);
await db.schema.constrain.unique("users", "uq_users_email", ["email"]);
await db.schema.alter.addColumns("users", { bio: "TEXT" });
await db.schema.drop.table("users", { cascade: true });
```

---

## Migrations

```javascript
await db.migrate.run({
    migrations: [
        {
            id: "001_users",
            up: "CREATE TABLE users (id uuid PRIMARY KEY DEFAULT gen_random_uuid())",
            down: "DROP TABLE users",
        },
    ],
});

await db.migrate.status({ directory: "./migrations" });
await db.migrate.rollback({ directory: "./migrations", steps: 1 });
```

SQL files and ESM/CJS modules in a directory are loaded in filename order. Each migration runs inside its own transaction under an advisory lock.

---

## Health Checks & Shutdown

```javascript
// Detailed health info for readiness probes
const health = await db.check();
// { status: "up", latency: { value: 2, unit: "ms" }, pool: { total, idle, waiting } }

// Simple boolean health check
const ready = await db.health();
if (!ready.healthy) {
    throw new Error(ready.error);
}

// Graceful shutdown (idempotent, safe to call concurrently)
process.on("SIGTERM", async () => {
    await db.shutdown();
    process.exit(0);
});
```

Shutdown is idempotent — safe to call multiple times; concurrent callers share one in-flight close. It drains active operations before closing pools.

---

## Sharding

Sharding routes queries to different PostgreSQL databases (shards) based on a shard key. Each shard is an independent database with its own connection pool.

### Configuration

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    sharding: {
        enabled: true,
        strategy: "hash", // or "consistent-hash", "rendezvous", "range", "directory", "tenant", "composite", "geographic", "weighted"
        shards: [
            {
                id: "shard-0",
                connection: {
                    connectionString: "postgresql://localhost:5432/shard0",
                },
            },
            {
                id: "shard-1",
                connection: {
                    connectionString: "postgresql://localhost:5432/shard1",
                },
            },
        ],
        defaultShardId: "shard-0",
    },
});
```

To disable sharding (preserves existing single-database behavior):

```javascript
const db = createDatabase({
    sharding: {
        enabled: false,
        strategy: "hash",
        shards: [],
    },
});
```

| Property | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | Yes | Whether sharding is active |
| `strategy` | `ShardStrategyName` | No | Routing strategy (default: `"hash"`) |
| `shards` | `ShardConfig[]` | Yes when enabled | List of shard configurations |
| `shards[].id` | `string` | Yes | Unique stable shard identifier |
| `shards[].connection` | `PoolConfig` | Yes | pg connection config for the shard |
| `shards[].weight` | `number` | No | Weight for `weighted` strategy |
| `defaultShardId` | `string` | No | Fallback shard for directory/tenant default policy |
| `coordination` | `PoolConfig` | No | Dedicated coordination DB for distributed drain state |
| `coordinationMode` | `"strict" \| "local-only" \| "fail-closed"` | No | Coordination mode (default: `"local-only"`) |
| `abandonedOperationTimeoutMs` | `number` | No | Timeout for abandoned ops (default: `300000`) |

### Routing Strategies

Nine strategies are available. Choose based on your data access pattern:

| Strategy | Name | Best For | Key Characteristic |
| :--- | :--- | :--- | :--- |
| Hash | `hash` | General purpose | Deterministic modulo; remaps most keys when shard count changes |
| Consistent Hash | `consistent-hash` | Cache-like workloads | Ring with virtual nodes; minimal remapping on membership change |
| Rendezvous | `rendezvous` | General purpose | Highest random weight; deterministic ranking per key |
| Range | `range` | Ordered keys (IDs, dates) | Explicit numeric/string ranges; no silent fallback |
| Directory | `directory` | Known key→shard mapping | Exact lookup; configurable unknown-key policy |
| Tenant | `tenant` | Multi-tenant SaaS | Tenant ID → shard mapping; optional default shard |
| Composite | `composite` | Multi-component keys | {tenantId, userId} → collision-resistant serialization |
| Geographic | `geographic` | Data residency | Region → eligible shards; deterministic selection |
| Weighted | `weighted` | Uneven capacity | Weighted random-walk; higher weight = more keys |

#### Hash Routing

Uses FNV-1a 32-bit hash of the shard key, modulo shard count. Identical keys always resolve to the same shard.

Uses FNV-1a 32-bit hash of the shard key, modulo shard count. Identical keys always resolve to the same shard. Adding or removing shards remaps keys — plan for data migration when changing shard membership.

#### Consistent Hash Routing

Uses a hash ring with configurable virtual nodes. Adding or removing a shard remaps only a fraction of keys (approximately `1 / (N+1)` where N is the new shard count).

#### Rendezvous Hash Routing

Ranks eligible shards deterministically for each key using a hash of `key:shardId`. The highest-ranked shard is selected. Supports an optional `hashSeed` to vary routing.

#### Range Routing

Routes ordered numeric keys to explicitly configured shard ranges. Boundaries can be inclusive or exclusive. Keys outside all ranges throw `ShardNotFoundError` — no silent fallback.

#### Directory Routing

Uses an explicit key-to-shard mapping. Unknown keys throw `ShardNotFoundError` unless `unknownKeyPolicy: "default"` is set with a `defaultShardId`.

Legacy `routing.directory` format is also supported for backward compatibility.

#### Tenant Routing

Routes using an explicit tenant identifier. Large tenants can be assigned to dedicated shards. Unknown tenants throw `ShardNotFoundError` unless `defaultTenantShardId` is configured.

#### Composite-Key Routing

Routes based on multiple key components (e.g., `tenantId` and `userId`). Uses length-prefixed serialization to prevent collisions from naive concatenation.

#### Geographic Routing

Routes using a configured region. Maps regions to eligible shards and deterministically selects from them. Unknown regions throw `ShardNotFoundError` unless a `defaultRegion` is configured with `unknownRegionPolicy: "default"`.

#### Weighted Routing

Supports shards with different configured capacities. Uses weighted highest-random-weight: `score = hash^(1/weight)`. Higher weight shards receive more keys on average.

**Weights are static** — they do not measure load or balance query latency automatically.

### Query Execution

Route queries to the correct shard using `db.query()` with a `shardKey` option:

```javascript
const result = await db.query(
    "SELECT * FROM users WHERE id = $1",
    [userId],
    { shardKey: userId },
);
```

When sharding is disabled, `shardKey` is ignored and the query runs against the single pool. When sharding is enabled and `shardKey` is provided, the query is routed to the correct shard based on the configured strategy.

When sharding is enabled but no `shardKey` is provided, the query runs against the primary pool (the default shard or first shard). Callers that need explicit shard routing must supply `shardKey`.

Parameterized SQL is preserved — values are never interpolated into SQL text.

### Transactions

Transactions stay on their original shard for their entire lifetime. Pass `shardKey` in the transaction options:

```javascript
await db.transaction(
    async (client) => {
        await client.query("INSERT INTO orders (user_id) VALUES ($1)", [userId]);
        // All operations happen on the same shard
    },
    { shardKey: userId },
);
```

Cross-shard transactions use `db.sharding.distributedTransaction()` for two-phase commit across database servers.

### Direct Shard Access

```javascript
db.sharding.enabled;           // true | false
db.sharding.strategy;          // "hash" | "consistent-hash" | ... | undefined
db.sharding.routingVersion;    // increments on each shard map change
db.sharding.shards;            // [{ id, connected, pool }, ...]
db.sharding.getShard("shard-0"); // { id, connected, pool } | undefined
db.sharding.getShardForKey("user-42"); // { id, connected, pool }

import { getRegisteredStrategyNames } from "@oneunit/database";
getRegisteredStrategyNames(); // ["hash", "consistent-hash", ...]
```

### Health & Observability

```javascript
// Health of all shards — aggregate result
const health = await db.sharding.health();
console.log(health.healthy);           // true | false
console.log(health.downShards);        // ["shard-1"] — shards that are down
console.log(health.upShards);          // ["shard-0", "shard-2"]

for (const shard of health.shards) {
    console.log(shard.shardId, shard.status, shard.latency.value, "ms");
    console.log(shard.pool);           // { total, idle, waiting }
}

// Health of a single shard
const shardHealth = await db.sharding.shardHealth("shard-0");
console.log(shardHealth?.status);      // "up" | "down" | undefined
```

### Resharding (Adding/Removing Shards)

Shards can be added or removed at runtime. Each change bumps the `routingVersion`, and optional hooks run before/after the change:

```javascript
// Add a new shard
await db.sharding.addShard({
    id: "shard-1",
    connection: { connectionString: "postgresql://user@db1:5432/db1" },
});

// Remove a shard
await db.sharding.removeShard("shard-0");

// Check the current routing version
console.log(db.sharding.routingVersion); // increments on each change
```

Resharding hooks allow you to coordinate data migration:

```javascript
await db.sharding.addShard(
    { id: "shard-2", connection: { connectionString: "..." } },
    {
        onBeforeReshard: (oldVersion, newVersion, added, removed) => {
            // Pause writes, start data migration
        },
        onAfterReshard: (oldVersion, newVersion, added, removed) => {
            // Resume writes, verify migration complete
        },
    },
);
```

**Important:** Changing shard membership changes routing but does **not** automatically move existing data. You must coordinate data migration separately.

### Migration Coordination

Run schema migrations across all shards and track their status:

```javascript
const result = await db.sharding.migrate.runMigrationOnAllShards(
    "001_add_users_table",
    "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)",
);

console.log(result.allApplied);    // true if all shards succeeded
console.log(result.anyFailed);     // true if any shard failed
console.log(result.shards);        // { "shard-0": "applied", "shard-1": "applied" }
```

### Topology Management

```javascript
// Read-only snapshot of shard topology
const snap = db.sharding.topology.snapshot();
console.log(snap.generation);        // routing version
console.log(snap.shards);            // [{ shardId, state, weight, activeOps, connected, ... }]

// Validate topology consistency
const validation = db.sharding.topology.validate();
console.log(validation.valid);       // boolean
console.log(validation.issues);      // string[]

// Plan rebalancing (does not execute moves)
const proposal = db.sharding.topology.planRebalance({ strategy: "least-loaded" });
console.log(proposal.proposal);      // [{ fromShardId, toShardId, reason, estimatedRows? }]
```

### Shard Rebalancing

Track progress of shard rebalance operations with the built-in `ShardRebalanceTracker`. This in-memory tracker helps monitor and manage data migration between shards.

**Basic Usage**:

```javascript
const db = createDatabase({ /* sharding config */ });

// Access the rebalance tracker
const tracker = db.sharding.rebalanceTracker;

// Create a new rebalance operation
const progress = tracker.createRebalance(
  "shard-0",  // source shard
  "shard-1",  // destination shard
  10000       // total keys to migrate
);

console.log(progress.rebalanceId);      // "rebal-abc123..."
console.log(progress.status);          // "planning"
console.log(progress.percentage);      // 0

// Update progress as keys are migrated
const updated = tracker.updateProgress(progress.rebalanceId, {
  movedKeys: 2500,
  failedKeys: 50,
});

console.log(updated.percentage);       // 25.5
console.log(updated.status);           // "copying" (auto-transitioned)

// Mark as completed (or let it auto-complete when all keys are processed)
tracker.completeRebalance(progress.rebalanceId);

// Or mark as failed
if (errorOccurred) {
  tracker.failRebalance(progress.rebalanceId, error.message);
}

// Or cancel
if (userCancelled) {
  tracker.cancelRebalance(progress.rebalanceId);
}

// Query rebalance operations
const allRebalances = tracker.listRebalances();
const specificRebalance = tracker.getRebalance(progress.rebalanceId);

// Clean up completed operations
tracker.clearCompleted();
```

**ShardRebalanceProgress Properties**:

| Property | Type | Description |
| :--- | :--- | :--- |
| `rebalanceId` | `string` | Unique identifier for the rebalance operation |
| `sourceShardId` | `string` | Source shard identifier |
| `destinationShardId` | `string` | Destination shard identifier |
| `totalKeys` | `number` | Total number of keys to migrate |
| `movedKeys` | `number` | Number of keys successfully migrated |
| `failedKeys` | `number` | Number of keys that failed to migrate |
| `skippedKeys` | `number` | Number of keys skipped |
| `percentage` | `number` | Completion percentage (0-100) |
| `status` | `string` | Current status: `"planning"`, `"copying"`, `"validating"`, `"cutting-over"`, `"completed"`, `"failed"`, `"cancelled"` |
| `startedAt` | `string` | ISO timestamp when rebalance started |
| `updatedAt` | `string` | ISO timestamp of last update |
| `errors` | `string[]` | Array of error messages |

**Automatic State Transitions**:

- When `movedKeys + failedKeys + skippedKeys >= totalKeys`, status automatically transitions to `"completed"`
- Percentage is automatically recalculated when `movedKeys` or `totalKeys` changes
- Terminal states (`completed`, `failed`, `cancelled`) cannot be changed

**Note**: This is an in-memory tracker. For production use with process restarts, persist the state to a database or external store.

---

## Replication

Replication routes queries between one PostgreSQL primary and zero or more read replicas that the operator has already provisioned (e.g. via physical streaming replication). This package configures **client connections** to those replicas — it does not create, promote, or fail over replicas.

### Configuration

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    connectionString: "postgresql://localhost:5432/app",
    replication: {
        enabled: true,
        mode: "replica-preferred", // "primary" | "replica-preferred" | "replica-only"
        primary: {
            id: "primary",
            connection: {
                connectionString: "postgresql://localhost:5432/app",
            },
        },
        replicas: [
            {
                id: "replica-0",
                connection: {
                    connectionString: "postgresql://localhost:5433/app",
                    max: 5,
                },
            },
        ],
        lagThresholdMs: 1000,
        healthCheck: {
            enabled: true,
            intervalMs: 30_000,
            timeoutMs: 5_000,
        },
    },
});
```

When sharding is enabled, use `ShardConfig.replication` instead (each shard's `connection` is its primary).

### Read Preference Modes

| Mode | Behavior | Use Case |
| :--- | :--- | :--- |
| `"primary"` | Always read from primary | Strong consistency required, write-after-read patterns |
| `"replica-preferred"` | Prefer replicas, fall back to primary if none eligible | Read scaling, slightly stale data acceptable |
| `"replica-only"` | Read from replicas only, error if none eligible | Analytics, reporting, read-only workloads |

| Property | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | Yes | Whether replication is active |
| `mode` | `"primary" \| "replica-preferred" \| "replica-only"` | Yes | Default read preference for queries |
| `primary` | `ReplicationEndpointConfig` | No | Primary endpoint config (defaults to main connection with id `"primary"`) |
| `replicas` | `ReplicationEndpointConfig[]` | Yes | List of read replica endpoint configurations |
| `lagThresholdMs` | `number` | No | Exclude replicas whose lag exceeds this threshold (default: 1000ms) |
| `healthCheck` | `object` | No | `{ enabled, intervalMs, timeoutMs, lagQuery? }` |

**ReplicationEndpointConfig**:
```typescript
{
  id: string;                    // Unique endpoint identifier
  connection: PoolConfig;        // PostgreSQL connection config
  role?: "primary" | "replica"; // Explicit role (auto-detected if omitted)
  weight?: number;               // Weight for load balancing (default: 1)
}
```

### Routing Intent

Routing is driven entirely by an explicit `readPreference` on `db.query()` — **never** by parsing SQL:

```javascript
// Write (or any operation needing the primary): always the primary
await db.query("INSERT INTO users (email) VALUES ($1)", ["ada@example.com"]);

// Strong read-after-write: force the primary
await db.query("SELECT * FROM users WHERE id = $1", [id], {
    readPreference: "primary",
});

// Read that may be slightly stale: prefer a replica, fall back to primary
await db.query("SELECT * FROM analytics WHERE ...", [], {
    readPreference: "replica-preferred",
});

// Read that must come from a replica; error if none is eligible
await db.query("SELECT * FROM analytics WHERE ...", [], {
    readPreference: "replica-only",
});
```

Default `readPreference` is `"primary"`, so existing code reads from the primary and there are no accidental stale reads or writes to replicas.

### Consistency & Lag

- All writes go to the primary. Transactions remain pinned to one primary connection for their entire lifetime.
- Read replicas may lag the primary. `lagThresholdMs` excludes replicas whose measured lag exceeds the threshold.
- Unknown lag (not yet measured) is distinct from zero lag and is reported as `lagUnknown: true` in health results.
- A successful health check does **not** imply a replica is consistent — health only confirms connectivity.
- This package does not implement distributed consensus, automatic primary promotion, or any zero-data-loss guarantee.

### Health & Observability

```javascript
const health = await db.replication.health();
// { healthy, primary: {...}, replicas: [{ endpointId, role, status, latencyMs, lagMs, lagUnknown, consecutiveFailures, error }], ... }
```

Per-endpoint health reports role, status, latency, known replication lag, consecutive failures, and sanitized error details. A failed replica may be excluded from read routing; a failed primary never redirects writes to a replica or another shard.

### Public API

`db.replication` exposes `enabled`, `mode`, `primaryEndpointId`, `replicaEndpointIds`, `health()`, `endpointHealth(id)`, `primaryPool`, `readEndpoint(preference)`, and `shutdown()`. When replication is disabled, `enabled` is `false` and all operations target the primary.

---

## Online Resharding (Data Migration)

`@oneunit/database` includes a durable, crash-recoverable online resharding subsystem that redistributes existing records when shard membership or routing configuration changes. Adding or removing a shard changes routing but does **not** automatically move data — you must explicitly plan and execute a migration.

### Lifecycle

```
planned → copying → catching_up → validating → ready_for_cutover → cutting_over → completed → finalized
```

Operator-visible states: `paused`, `failed`, `cancelled`, `rollback_required`.

### Usage

```javascript
const db = createDatabase({
    sharding: {
        enabled: true,
        strategy: "hash",
        shards: [
            { id: "src", connection: { connectionString: "..." } },
            { id: "dst", connection: { connectionString: "..." } },
        ],
    },
});

const rsharding = db.resharding;
await rsharding.init();
await rsharding.initShard("src", srcPool);
await rsharding.initShard("dst", dstPool);

const plan = await rsharding.plan(descriptor, targetTopology);
await rsharding.start();
await rsharding.copyUnits(descriptor);          // bounded, resumable batches
const { passed } = await rsharding.validate();  // never cut over after failure
await rsharding.cutover();                      // coordinated, recoverable
await rsharding.finalize();
```

The application supplies a `MigrationDescriptor` describing each table's primary key, shard key, batch reader, idempotent upsert, delete propagation, and optional validation — the package does not guess schemas from SQL.

### Key Guarantees

- **Durable state**: migration lifecycle, per-unit checkpoints, change-log offsets, and published routing versions are persisted on a coordination database (versioned schema, idempotent init)
- **Bounded, resumable copy**: keyset-paginated batches (never offset pagination), configurable batch size/concurrency/throttle/retries. Checkpoints advance only after the destination write commits
- **Idempotent destination writes**: adapters use upsert (`ON CONFLICT`) semantics; repeated delivery does not duplicate logical records
- **Change capture**: a transactional outlog (`resharding_changes`) records source mutations atomically with the source mutation, with stable sequence ordering
- **Coordinated cutover**: verifies the copy is complete, drains the change log to a consistency boundary, fences writes for moved keys, then publishes the new routing version atomically. Writes are briefly paused during the drain/fence window
- **Crash recovery**: all operations are durable and idempotent. A restart during cutover inspects durable state to roll forward or require operator intervention

### Supported Planning Strategies

| Strategy | Supported planning | Notes |
| :--- | :--- | :--- |
| `hash` | Yes | Whole-shard units; membership change remaps most keys |
| `consistent-hash` | Yes | Bucket (virtual-node range) units |
| `rendezvous` | Yes | Whole-shard units for added/removed shards |
| `range` | Yes | Exact range-boundary deltas |
| `directory` | Yes | Exact per-key deltas from the directory map |
| `tenant` | Yes | Whole-tenant units only |
| `composite` | Yes | Whole-shard units; exact composite-key mapping delegated to adapter |
| `geographic` | Yes | Per-region units |
| `weighted` | Yes | Whole-shard units for membership/weight changes |

### Limitations

- **No cross-shard distributed transactions.** A migration touches multiple shards but each shard's work is committed independently; partial failure across shards is reconciled through the coordination authority, not an atomic transaction.
- **No zero-downtime claim.** Writes are briefly paused during the cutover drain/fence window.
- **Writes bypassing package-managed APIs** (e.g. raw `psql`, other applications writing directly to PostgreSQL) are NOT captured by the transactional outlog.
- **The package does not infer schemas or shard keys.** Every participating table must be described through a `MigrationTableAdapter`.

---

## Failover

High-availability support for PostgreSQL clusters with primary/replica failover. The failover layer is a standalone `FailoverManager` that runs health checks, detects primary failures, and orchestrates automatic or manual failover by promoting a replica.

### Providers

A `HaProvider` discovers the current topology and performs promotion/verification. Two implementations are bundled:

**StaticHaProvider** — simple deployments with a statically configured primary. No automatic promotion; failover requires operator intervention.

```javascript
import { FailoverManager, StaticHaProvider } from "@oneunit/database";

const provider = new StaticHaProvider(new Map([
  ["shard-0", {
    id: "shard-0",
    host: "primary-host",
    port: 5432,
    database: "mydb",
    user: "postgres",
    connectionString: "postgresql://postgres@primary-host:5432/mydb",
  }],
]));

const config = {
  enabled: true,
  provider,
  healthCheckIntervalMs: 5_000,
  failureThreshold: 3,
  recoveryThreshold: 2,
  suspectTimeoutMs: 30_000,
  promotionTimeoutMs: 30_000,
  verificationTimeoutMs: 15_000,
};

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config,
  logger,
});

await failover.initialize();
```

**PatroniHaProvider** — production Patroni-managed clusters. Detects the primary and replicas from the Patroni REST API and supports automatic promotion.

```javascript
import { FailoverManager, PatroniHaProvider } from "@oneunit/database";

const provider = new PatroniHaProvider("http://patroni-cluster:8008");

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider, healthCheckIntervalMs: 5_000 },
  logger,
});

await failover.initialize();
```

### Failover States

| State | Description |
| :--- | :--- |
| `healthy` | Primary available and responding to health checks |
| `suspect` | Primary failing health checks; failover may be initiated |
| `primary_unavailable` | Primary unavailable for longer than `suspectTimeoutMs`; failover is requested |
| `failover_requested` | Failover approved; waiting for promotion |
| `promoting` | A replica is being promoted to primary |
| `verifying` | The new primary is being verified as writable |
| `primary_changed` | New primary confirmed; failover complete |
| `recovering` | Connections and pools being re-established |
| `failed` | Failover could not complete; operator intervention required |

Transitions follow `healthy → suspect → primary_unavailable → failover_requested → promoting → verifying → primary_changed → recovering → healthy`. `failed` is terminal and can only move to `recovering`.

### Failover API

```javascript
// Get current topology from the HA provider
const topology = await failover.getTopology();
console.log("Primary:", topology.primary);
console.log("Replicas:", topology.replicas);

// Current failover state
console.log("State:", failover.currentState);          // FailoverState
console.log("Primary:", failover.currentPrimary);        // string | undefined
console.log("Generation:", failover.currentGeneration);  // number

// Report a primary failure detected by the query/transaction path
await failover.reportPrimaryFailure("connection lost");

// Report a successful operation on the primary
await failover.reportPrimarySuccess();

// Request a manual failover (operator-initiated)
await failover.requestManualFailover("planned maintenance");

// Register a connection so it is invalidated on failover
failover.registerConnection("primary", "shard-0", client);
failover.markInTransaction(client, "tx-123");

// Invalidate all connections to the current primary
const result = await failover.invalidatePrimaryConnections();

// Check whether a node is fenced
const fenced = await failover.isFenced("node-1");
```

### Failover Events

State transitions are recorded as failover events. The state machine tracks `fromState`, `toState`, `reason`, and `triggeredBy` on each event, available via `failover.failoverState.failoverEvents`.

When an `eventStore` is configured, every state change is also persisted for auditing (see "Failover Event History" below).

### Error Handling

```javascript
import {
  FailoverError,
  FailoverNotSupportedError,
  SplitBrainPreventedError,
  PrimaryNotVerifiedError,
  isFailoverError,
} from "@oneunit/database";

try {
  await failover.requestManualFailover("maintenance");
} catch (error) {
  if (isFailoverError(error)) {
    console.error(`Failover error on shard ${error.shardId}:`, error.message);
  }
}
```

### Production Best Practices

1. **Use Patroni for production** — automatic primary detection and promotion
2. **Configure timeouts to match your SLA** — `healthCheckIntervalMs`, `failureThreshold`, `suspectTimeoutMs`, `promotionTimeoutMs`, `verificationTimeoutMs`
3. **Persist state across restarts** — configure a `stateStore` so failover state survives manager restarts
4. **Test failover procedures** — regularly in staging
5. **Monitor health checks and fencing** — ensure all nodes are checked and fenced nodes are blocked

### Advanced Failover Configuration

All advanced options are opt-in and passed to the `FailoverManager` constructor. Existing configurations continue to work unchanged.

#### Persistent State Storage

Persist failover state across restarts using a coordination database:

```javascript
import { FailoverManager, PgFailoverStateStore, FAILOVER_STATE_SCHEMA } from "@oneunit/database";

// Execute the schema against your coordination database first
await coordinationPool.query(FAILOVER_STATE_SCHEMA);

const stateStore = new PgFailoverStateStore(coordinationPool, logger);

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  stateStore,
  recoveryOptions: {
    autoReconcile: true,
    forceRecovery: false,
    maxStateAgeMs: 300_000, // State older than 5 min is considered stale
  },
});

await failover.initialize();
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS _database_ha_state (
    shard_id TEXT NOT NULL PRIMARY KEY,
    state_json JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _database_ha_state_versions (
    id BIGSERIAL PRIMARY KEY,
    shard_id TEXT NOT NULL,
    state_json JSONB NOT NULL,
    version BIGINT NOT NULL,
    previous_version BIGINT NOT NULL,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT
);

CREATE TABLE IF NOT EXISTS _database_ha_locks (
    lock_name TEXT NOT NULL PRIMARY KEY,
    locked_by TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lock_expiry TIMESTAMPTZ NOT NULL
);
```

#### Connection Invalidation

Track connections per node so they are invalidated when the primary fails. Connections in active transactions are reported but not forcibly closed.

```javascript
import { ConnectionTracker } from "@oneunit/database";

const connectionTracker = new ConnectionTracker({
  cleanupIntervalMs: 60_000,
  maxIdleMs: 300_000,
});

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  connectionTracker,
});

// Callers register connections when acquiring clients
failover.registerConnection("primary", "shard-0", client);

// Connections are invalidated automatically before primary promotion
```

#### Failure Detection Thresholds

Configure per-node failure detection with `HealthCheckTracker`:

```javascript
import { HealthCheckTracker } from "@oneunit/database";

const healthTracker = new HealthCheckTracker({
  consecutiveFailureThreshold: 3,
  unhealthyDurationThresholdMs: 30_000,
  consecutiveSuccessThreshold: 2,
  healthCheckIntervalMs: 5_000,
  unhealthyHealthCheckMultiplier: 0.5,
  nodeThresholds: {
    "primary-1": {
      consecutiveFailures: 5,
      unhealthyDurationMs: 60_000,
    },
  },
}, logger);

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  healthTracker,
});
```

#### Failover Hooks

Register lifecycle hooks via `FailoverHookRunner`. Hook errors are caught and logged so a failing hook never breaks failover.

```javascript
import { FailoverHookRunner } from "@oneunit/database";

const hookRunner = new FailoverHookRunner(logger);
hookRunner.registerHook("beforeStateChange", (context) => {
  console.log(`Transitioning ${context.data.fromState.state} -> ${context.data.toState.state}`);
});
hookRunner.registerHook("afterFailover", (context) => {
  console.log(`Failover complete. New primary: ${context.data.newPrimary?.id}`);
});

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  hooks: hookRunner,
});
```

Available hook types: `beforeStateChange`, `afterStateChange`, `beforeFailover`, `afterFailover`, `failoverFailed`, `beforePromotion`, `afterPromotion`, `promotionFailed`, `beforeDemotion`, `afterDemotion`, `demotionFailed`, `beforeHealthCheck`, `afterHealthCheck`, `onPrimaryChange`, `onTopologyChange`, `onFencingActivated`, `onFencingReleased`.

#### Pool Reconnection

Reconnect primary and replica pools after failover with exponential backoff:

```javascript
import { PoolReconnector } from "@oneunit/database";

const poolReconnector = new PoolReconnector(
  poolMap,           // Map<nodeId, Pool>
  configProvider,    // (nodeId) => Promise<PoolConfig>
  logger,
  {
    maxRetries: 5,
    retryDelayMs: 1_000,
    maxRetryDelayMs: 30_000,
    validatePool: true,
  }
);

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  poolReconnector,
});
```

Pools are swapped atomically — the new pool is validated before the old pool is closed.

#### Fencing Integration

Prevent a node from assuming the primary role during failover:

```javascript
import {
  AdvisoryLockFencingProvider,
  TableBasedFencingProvider,
  FencingManager,
  FENCING_SCHEMA,
} from "@oneunit/database";

// Execute the schema against your coordination database first
await coordinationPool.query(FENCING_SCHEMA);

const fencingProvider = new TableBasedFencingProvider(coordinationPool, logger);
// Or, for advisory-lock-based fencing:
// const fencingProvider = new AdvisoryLockFencingProvider(coordinationPool, logger);

const fencingManager = new FencingManager(fencingProvider, logger);

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  fencingManager,
});
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS _database_fencing_locks (
    node_id TEXT NOT NULL PRIMARY KEY,
    lock_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    priority INTEGER NOT NULL DEFAULT 1,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _database_fencing_history (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT,
    priority INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    performed_by TEXT NOT NULL,
    performed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Fenced nodes are blocked during health checks and failover.

#### Failover Event History

Record and query failover events for auditing:

```javascript
import { PgFailoverEventStore, FAILOVER_EVENT_SCHEMA } from "@oneunit/database";

// Execute the schema against your coordination database first
await coordinationPool.query(FAILOVER_EVENT_SCHEMA);

const eventStore = new PgFailoverEventStore(coordinationPool, logger);

const failover = new FailoverManager({
  shardId: "shard-0",
  primaryPool,
  config: { enabled: true, provider },
  logger,
  eventStore,
});

// Query events
const { events, total } = await eventStore.queryEvents({
  shardId: "shard-0",
  type: "state_change",
  severity: "warning",
  startTime: new Date(Date.now() - 86_400_000), // last 24h
});

// Statistics
const stats = await eventStore.getStatistics();
// { totalEvents, eventsByType, eventsBySeverity, eventsByShard, firstEvent, lastEvent }

// Cleanup old events
await eventStore.cleanupEvents(30); // retain 30 days
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS _database_ha_events (
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

CREATE INDEX IF NOT EXISTS idx_ha_events_shard_id ON _database_ha_events(shard_id);
CREATE INDEX IF NOT EXISTS idx_ha_events_type ON _database_ha_events(type);
CREATE INDEX IF NOT EXISTS idx_ha_events_timestamp ON _database_ha_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_ha_events_severity ON _database_ha_events(severity);
```

---

## Distributed Transactions

Cross-shard two-phase commit (2PC) for atomic operations across multiple shards. Requires `max_prepared_transactions > 0` on all participating PostgreSQL servers. This is exposed both through the sharding layer (`db.sharding.distributedTransaction`) and directly (`db.distributedTx`).

**Quick Start**:

```javascript
// Using the sharding API
const result = await db.sharding.distributedTransaction(async (tx) => {
    await tx.execute("user-1", "INSERT INTO orders (user_id, total) VALUES ($1, $2)", [userId, 99.99]);
    await tx.execute("user-2", "UPDATE accounts SET balance = balance - $1 WHERE user_id = $2", [50, userId]);
}, { isolation: "SERIALIZABLE", timeout: 10_000 });

// result: { transactionId, status: "committed", participants: { "shard-0": "committed", "shard-1": "committed" } }
```

**Using the direct API**:

```javascript
import { DistributedTransactionCoordinator } from "@oneunit/database";

// Create coordinator with participant shards
const coordinator = new DistributedTransactionCoordinator({
  prepareTimeoutMs: 30000,
  commitTimeoutMs: 30000,
});

// Begin a distributed transaction
const { transactionId, context } = await coordinator.begin();

try {
  // Execute on shard A
  await context.execute(
    { shardId: "shard-a" },
    "INSERT INTO orders (user_id, total) VALUES ($1, $2)",
    [userId, 99.99]
  );

  // Execute on shard B
  await context.execute(
    { shardId: "shard-b" },
    "UPDATE accounts SET balance = balance - $1 WHERE user_id = $2",
    [50, userId]
  );

  // Prepare and commit
  await coordinator.prepare(transactionId);
  await coordinator.commit(transactionId);
  
  console.log("Transaction committed successfully");
} catch (error) {
  await coordinator.abort(transactionId);
  throw error;
}
```

### Two-Phase Commit Lifecycle

1. **Begin** → Creates transaction ID and registers participants
2. **Execute** → Runs SQL on participants, tracks statements
3. **Prepare** → `PREPARE TRANSACTION` on all participants
4. **Commit** → `COMMIT PREPARED` on all participants (atomic)
5. **Abort** → `ROLLBACK PREPARED` on all participants (on any failure)

### Configuration

```javascript
const coordinator = new DistributedTransactionCoordinator({
  prepareTimeoutMs: 30000,    // Timeout for PREPARE (default: 30s)
  commitTimeoutMs: 30000,     // Timeout for COMMIT (default: 30s)
  maxRetries: 3,              // Max retries for prepare/commit
});
```

### Execute Options

```typescript
interface DistributedTransactionExecuteOptions {
  timeoutMs?: number;         // Statement timeout
  retryOnDeadlock?: boolean;  // Retry on deadlock (default: true)
}

await context.execute(
  { shardId: "shard-1" },
  "UPDATE users SET balance = balance - $1 WHERE id = $2",
  [100, userId],
  { timeoutMs: 5000, retryOnDeadlock: true }
);
```

### Status and Recovery

```javascript
// Check transaction status
const status = await coordinator.getStatus(transactionId);
// { state: "prepared", participants: { "shard-a": "prepared", "shard-b": "prepared" } }

// Recover from crash - prepare was successful but commit failed
if (status.state === "prepared") {
  await coordinator.commit(transactionId);
}
```

**States**:
- `pending` — Transaction created, not yet prepared
- `prepared` — All participants prepared, waiting for commit
- `committed` — Successfully committed
- `aborted` — Successfully aborted
- `failed` — Failed during prepare or commit

### Error Handling

All distributed transaction errors extend `DistributedTransactionError`:

```javascript
import {
  DistributedTransactionError,
  DistributedTxNotFoundError,
  DistributedTxAlreadyResolvedError,
  MaxPreparedTransactionsError,
} from "@oneunit/database";

try {
  await coordinator.commit(transactionId);
} catch (error) {
  if (error instanceof DistributedTxAlreadyResolvedError) {
    console.log("Transaction already resolved");
  } else if (error instanceof MaxPreparedTransactionsError) {
    console.log("Increase PostgreSQL max_prepared_transactions");
  } else if (error instanceof DistributedTransactionError) {
    console.log("Distributed transaction failed:", error.message);
  }
}
```

### PostgreSQL Configuration

Ensure all participating PostgreSQL servers have:

```sql
-- In postgresql.conf
max_prepared_transactions = 100  -- Or higher based on expected concurrency

-- Check current value
SHOW max_prepared_transactions;
```

All operations are prepared atomically and committed or aborted together. On any failure, `coordinator.abort()` is invoked to roll back all participants.

---

## Error Handling

Driver errors are wrapped as `DatabaseError` with the original preserved on `.cause`.

```javascript
import {
    DatabaseError,
    TimeoutError,
    isUniqueViolation,
    isTransientError,
    isShardingDisabledError,
    isWriteFencedError,
} from "@oneunit/database";

try {
    await db.query("INSERT INTO users (email) VALUES ($1)", [email]);
} catch (error) {
    if (isUniqueViolation(error)) {
        return { conflict: true };
    }
    throw error;
}
```

### Available Error Helpers

| Helper | Catches |
| :--- | :--- |
| `isUniqueViolation` | Duplicate key (`23505`) |
| `isForeignKeyViolation` | FK constraint (`23503`) |
| `isNotNullViolation` | NOT NULL constraint (`23502`) |
| `isCheckViolation` | CHECK constraint (`23514`) |
| `isExclusionViolation` | Exclusion constraint (`23P01`) |
| `isConstraintViolation` | Any constraint violation (`23xxx`) |
| `isSerializationFailure` | Serializable isolation conflict (`40001`) |
| `isDeadlock` | Deadlock detected (`40P01`) |
| `isConnectionError` | Connection lost / refused |
| `isTransientError` | Any retriable error |

### Sharding Errors

| Error | Code | When |
| :--- | :--- | :--- |
| `ShardingDisabledError` | `SHARDING_DISABLED` | Sharding APIs called when disabled |
| `ShardNotFoundError` | `SHARD_NOT_FOUND` | Shard ID or key mapping not found |
| `ShardConfigError` | `SHARD_CONFIG_ERROR` | Invalid sharding configuration |
| `ShardRoutingError` | (none) | Query/transaction routing to a shard |
| `ShardNotReadyError` | `SHARD_NOT_READY` | Shard is draining or removed |
| `WriteFencedError` | `WRITE_FENCED` | Writes blocked by active migration |
| `CrossShardTransactionError` | `CROSS_SHARD_TRANSACTION` | Cross-shard transaction via transaction API |

### Resharding Errors

| Error | Code | When |
| :--- | :--- | :--- |
| `ReshardingError` | (varies) | General resharding failure |
| `UnsupportedStrategyError` | `UNSUPPORTED_STRATEGY` | Strategy planning |
| `WriteFencedError` | `WRITE_FENCED` | Shard write gate is fenced |
| `MigrationLockedError` | `MIGRATION_LOCKED` | Worker lock held by another instance |
| `UnrecoverableCutoverError` | `UNRECOVERABLE_CUTOVER` | Cutover requires intervention |

---

## Logging & Metrics

Pass any logger with `info` / `warn` / `error` / `debug` methods. Parameter values are **not** logged unless `logParameters: true`.

```javascript
const db = createDatabase({
    logger: console,
    instrumentation: {
        logQueries: true,
        slowQueryMs: 200,
        onQuery({ durationMs, rowCount }) {
            metrics.timing("db.query", durationMs, { rowCount });
        },
    },
});

// Snapshot current metrics
db.metrics.snapshot();
```

### MetricsCollector

| Metric | Description |
| :--- | :--- |
| `queries` | Total query count |
| `errors` | Total error count |
| `slowQueries` | Queries exceeding `slowQueryMs` |
| `retries` | Retry attempts |
| `transactions` | Transaction count |
| `rolledBackTransactions` | Rolled-back transactions |
| `totalDurationMs` | Cumulative query time |
| `maxDurationMs` | Slowest single query |
| `lastDurationMs` | Most recent query time |

Call `db.metrics.snapshot()` to get a frozen copy. Call `db.metrics.reset()` to zero all counters.

Per-shard metrics are available via `db.sharding.metrics`.

---

## TypeScript

The package ships with full TypeScript declarations. ESM-only (`"type": "module"`).

```ts
import { createDatabase } from "@oneunit/database";

interface User {
    id: string;
    email: string;
}

const db = createDatabase();
const user = await db.queryOne<User>(
    "SELECT id, email FROM users WHERE id = $1",
    [id],
);
```

---

## Sub-path Exports

For tree-shaking or targeted imports, the package exposes granular entry points:

```javascript
import { createClient }       from "@oneunit/database/client";
import { createModelFactory } from "@oneunit/database/model";
import { createQueryBuilder }  from "@oneunit/database/query";
import { createSchemaManager } from "@oneunit/database/schema";
import { id, timestamp }       from "@oneunit/database/column";
import { encode, decode }      from "@oneunit/database/json-serialization";
```

---

## Examples

Runnable examples live in the [`examples/`](examples) directory:

| File | Description |
| :--- | :--- |
| [`basic.js`](examples/basic.js) | Connect, query, shut down |
| [`transactions.js`](examples/transactions.js) | Transaction + savepoint |
| [`streaming.js`](examples/streaming.js) | Cursor iteration |
| [`models.js`](examples/models.js) | Model CRUD |
| [`migrations.js`](examples/migrations.js) | In-memory migrations |
| [`resharding.js`](examples/resharding.js) | End-to-end online resharding |

```bash
DATABASE_URL=postgresql://localhost:5432/app node examples/basic.js
```

---

## Testing

```bash
npm test
```

Tests use [vitest](https://vitest.dev/). All tests mock `pg.Pool` and `PoolClient` — no live PostgreSQL instance required.

Tests are organized under `test/` to mirror `src/`:

| Directory | Covers |
| :--- | :--- |
| `test/client/` | client, transactions, retry, batch, check, health, shutdown, stream, cursor |
| `test/query/` | SQL fragments & QueryBuilder |
| `test/schema/` | DDL helpers, migration runner |
| `test/model/` | Model CRUD abstraction |
| `test/json-serialization/` | encode/decode utilities |
| `test/errors/` | error normalization & classifiers |
| `test/config/` | env-based config loading |
| `test/database.test.js` | `createDatabase()` composition surface |
| `test/edge-cases.test.js` | edge, extreme, behaviour & real-world scenarios |
| `test/sharding/` | sharding config, routing, pool lifecycle |
| `test/integration/sharding/` | integration tests with Docker Compose PG cluster |

```bash
npm test                    # run all tests
npm run test:watch          # watch mode
npx vitest run test/client  # run one directory
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding standards, and the PR workflow.

---

## Support & Troubleshooting

### Connection Issues

**Problem:** Sporadic `ECONNRESET` or `connection terminated` errors.

**Solution:** Enable connection validation:
```javascript
const db = createDatabase({
    validateConnections: true, // Validate connections on first use
});
```

### Connection Leaks

**Problem:** Pool exhaustion errors (`POOL_EXHAUSTED`).

**Diagnosis:**
```javascript
// Check for leaked manual clients
const activeCount = db.getActiveManualClientCount();
console.log(`Active manual clients: ${activeCount}`);

// Check health for pool statistics
const health = await db.check();
console.log('Pool stats:', health.pool);
```

**Solution:** Ensure all manual clients are released:
```javascript
const client = await db.getClient();
try {
    await client.query('SELECT 1');
} finally {
    db.releaseClient(client); // Always release!
}
```

### Cursor Leaks

**Problem:** PostgreSQL memory usage keeps increasing.

**Diagnosis:** Check for long-running cursors (warnings logged after 5 minutes).

**Solution:** Always close cursors:
```javascript
// Using for/await (recommended)
for await (const row of cursor) {
    process(row);
}
// Cursor automatically closed

// Manual cleanup
try {
    const cursor = await db.cursor(query);
    // process cursor
} finally {
    await cursor.close();
}
```

---

## License

[MIT](LICENSE) © 2026 mayank# database
