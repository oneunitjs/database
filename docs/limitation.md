# Package Limitations

> Active limitations of `@oneunit/database`. Severity: **P3** = documented design constraint (intentional). Engine/product limitations are confirmed by execution, not just documentation. None of the items below are bugs — they are inherent design constraints that operators must account for.

---

## Table of Contents

- [Severity Overview](#severity-overview)
- [P3 — Documented Design Constraints (Active)](#p3--documented-design-constraints-active)
- [Engine / Product Limitations (Active)](#engine--product-limitations-active)
- [Deployment Constraints](#deployment-constraints)
- [Fixed Limitations (Previously Reported)](#fixed-limitations-previously-reported)
- [References](#references)

---

## Severity Overview

| Severity | Meaning | Count | Status |
|----------|---------|-------|--------|
| **P3** | Documented design constraint | 4 | Active |
| Engine/Product | Confirmed by execution, not just docs | 6 | Active |
| Deployment | Environmental / operational constraints | 4 | Active |

---

## P3 — Documented Design Constraints (Active)

These are intentional design decisions, not bugs. They cannot be fixed without changing the package's fundamental architecture.

### P3-1 — No cross-shard transactions via the sharding API

| Aspect | Details |
|--------|---------|
| **What** | `db.sharding.transaction()` only supports single-shard transactions. Cross-shard atomicity requires `distributedTransaction()` (2PC). |
| **Impact** | Cannot perform atomic operations across multiple shards using the sharding API alone. |
| **Mitigation** | Use `db.sharding.distributedTransaction()` or `db.distributedTx` for cross-shard operations. Consider denormalizing data to avoid cross-shard operations where possible. |

### P3-2 — Sharding validation only at startup

| Aspect | Details |
|--------|---------|
| **What** | Runtime configuration changes are not re-validated; validation only occurs in the constructor. |
| **Impact** | Configuration errors introduced at runtime go undetected until they cause failures. |
| **Mitigation** | Restart the application after configuration changes. |

### P3-3 — Mutex not async-safe

| Aspect | Details |
|--------|---------|
| **What** | Simple promise queue implementation may have race conditions under concurrent access. |
| **Impact** | Race conditions in shard routing under extreme concurrency. |
| **Mitigation** | Use serialized access patterns at the application level. |

### P3-4 — No automatic primary election (failover)

| Aspect | Details |
|--------|---------|
| **What** | Failover relies on an external HA provider for primary election. There is no built-in election mechanism. |
| **Impact** | Failover features are non-functional without Patroni / pg_auto_failover / a custom `HaProvider`. A dead primary stays dead without external HA. |
| **Mitigation** | Deploy with external HA infrastructure and wire a `HaProvider` into `FailoverManager`. |

---

## Engine / Product Limitations (Active)

| Limitation | Reality |
|------------|---------|
| **PostgreSQL only** | `package.json` depends only on `pg`. No MySQL / SQLite / SQL Server adapters. Cannot target anything except PostgreSQL. |
| **Node.js >= 20** | Uses modern JavaScript features and APIs available in Node.js 20+. Cannot deploy on Node.js 18 (EOL April 2025) or older. |
| **PostgreSQL 13+** | Many advanced features (replication lag via `pg_last_xact_replay_timestamp()`, distributed TX prepared transactions) require PostgreSQL 13+. Basic features work with PG 10+, but advanced features need PG 13+. |
| **Streaming peers optional** | `pg-cursor` / `pg-query-stream` are optional peer dependencies. Cursor + stream workloads require them installed separately. |
| **`max_prepared_transactions > 0`** | Required for 2PC distributed transactions. If 0, `MaxPreparedTransactionsError` is thrown. Must be set in `postgresql.conf` on all participating servers. |
| **Physical replication not built-in** | The package configures client connections to provisioned replicas; it does not create, promote, or fail over replicas. Physical streaming replication must be configured by the operator. |

---

## Deployment Constraints

| Constraint | Details |
|------------|---------|
| **Node.js >= 20** | Cannot deploy on Node.js 18 or older. Use latest LTS recommended. |
| **PostgreSQL 13+** | Advanced features require PostgreSQL 13+. Verify version before enabling replication or distributed transactions. |
| **Test containers dependency** | Integration tests require Docker to be installed and running. Upstream vitest `globalSetup` (`postgres:16` x5) cannot start without Docker. |
| **No Docker / Testcontainers in some environments** | In environments without Docker, integration tests cannot run via the standard test harness. Use local PostgreSQL instances for manual verification. |

---

## Fixed Limitations (Previously Reported)

The following limitations were reported against earlier versions and have been fixed. They are listed here for historical reference only — they no longer apply.

| ID | Issue | Severity | File |
|----|-------|----------|------|
| P0-1 | `pool.connect` callback incompatibility → `pool.query` / schema / health / check deadlock | P0 | `src/client/pool.ts` |
| P0-2 | `db.query(sql\`...\`)` drops fragment values | P0 | `src/client/client.ts` |
| P0-3 | Optimistic locking breaks `updateById` / `deleteById` / soft-delete | P0 | `src/model/model.ts` |
| P1-1 | `queryAllShards` merge `"application"` returns `{}` | P1 | `src/database.ts` |
| P1-2 | Failed 2PC abort poisons next query with `25P02` | P1 | `src/distributed-tx/distributed-transaction-context.ts` |
| P2-1 | Sharded `transaction()` without `shardKey` silently uses default shard | P2 | `src/database.ts` |
| P2-2 | Missing `defaultShardId` only warns instead of throwing | P2 | `src/database.ts` |
| P2-3 | Schema auto-sync undefined unless explicitly configured | P2 | `src/database.ts` |
| P2-6 | Hardcoded leak timers not `.unref()`'d | P2 | `src/client/pool.ts`, `src/client/client.ts` |
| P2-7 | Module-level `Set`s retain clients on shutdown | P2 | `src/client/pool.ts`, `src/client/client.ts`, `src/database.ts` |
| P2-9 | Query retry opt-in (disabled by default) | P2 | `src/client/client.ts` |

### Default behavior change

| Change | Previous | Current |
|--------|----------|---------|
| Default sharding strategy | `"hash"` | `"consistent-hash"` |

Rationale: `consistent-hash` minimizes key remapping when shards are added or removed, which is safer for production deployments that scale shard membership over time.

---

## References

- **[README.md](../README.md)** — Usage guide, quick start, configuration, production considerations
- **[ARCHITECTURE.md](ARCHITECTURE.md)** — Internal architecture, design principles, module map, production limitations section
- **[CHANGELOG.md](CHANGELOG.md)** — Release notes and fixed issues
- **[LIMITATIONS.md](../LIMITATIONS.md)** — Real-environment test report (raw results from live PostgreSQL clusters)
- **[results.json](../results.json)** — Raw real-environment test results JSON
