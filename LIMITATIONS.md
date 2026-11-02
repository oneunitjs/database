# @oneunit/database — Real-Environment Test Report

Package: `@oneunit/database` `2.0.0-beta.1` from https://github.com/oneunitjs/database
Tested: 2026-11-02 against **five live PostgreSQL 15.19** instances (not mocks, not Docker).

Harness: `real-env/run-real-tests.mjs`
Raw results: `real-env/results.json`

```
PASS=34  FAIL=3  LIMIT=10  (47 cases)
```

---

## Environment that was actually running

Docker and Testcontainers were **not available**. Upstream `vitest` `globalSetup` (`postgres:16` x5) cannot start here.

Instead, five independent PostgreSQL 15 clusters were initialized as user `postgres`:

| Role | Port | Database | Data dir |
| :--- | ---: | :--- | :--- |
| primary / app | 5432 | `app` | `/var/lib/postgresql/real-test/primary` |
| shard-0 | 5440 | `shard0` | `/var/lib/postgresql/real-test/shard0` |
| shard-1 | 5441 | `shard1` | `/var/lib/postgresql/real-test/shard1` |
| shard-2 | 5442 | `shard2` | `/var/lib/postgresql/real-test/shard2` |
| 2PC coordination | 5443 | `coordination` | `/var/lib/postgresql/real-test/coord` |

All instances: `max_prepared_transactions=100`, `wal_level=replica`, `trust` auth on `127.0.0.1`, `pgcrypto` enabled.

Start/stop:

```bash
bash real-env/init-clusters.sh
bash real-env/start-postgres.sh
cd /tmp/oneunitjs-database && node --import tsx /workspace/real-env/run-real-tests.mjs
```

Source under test: `/tmp/oneunitjs-database` (clone of the GitHub repo).

---

## What worked on real PostgreSQL

| Area | Evidence |
| :--- | :--- |
| `db.query` / `db.queryOne` parameterized SQL | Injection payload stored as data; table still exists |
| Transactions + savepoints | Outer insert committed; savepoint insert rolled back |
| `SERIALIZABLE` isolation | `SHOW transaction_isolation` = serializable |
| Named prepared statements | `db.prepare(...).execute([email])` |
| Query timeout | `queryTimeout: 250` aborted `pg_sleep(2)` |
| Models: insert, findById, find, count, exists, insertMany | Live `accounts` table |
| Query builder select/where/order/limit/insert/update | Compiled SQL executed |
| `db.batch.insertMany` chunked | 25 rows, chunkSize 10 |
| Migrations SQL + function, idempotent rerun, rollback | `001_widgets` / `002_color` |
| `db.cursor` + `db.stream` | 40 rows; needs peers `pg-cursor` / `pg-query-stream` |
| Hash shard routing by `shardKey` | Inserts landed on shard-0/1/2 as FNV-1a predicted |
| Single-shard `db.transaction({ shardKey })` | balance 100 -> 105 |
| Tenant strategy | `acme` -> shard-0, `globex` -> shard-1; unknown tenant throws |
| 2PC **commit** across two instances | balances 50/50 -> 40/60, both participants `committed` |
| `sharding.health()` | per-shard up + pool stats (uses `pool.connect`, not `pool.query`) |

---

## Limitations found by executing real queries

Severity: **P0** = broken on a live server, **P1** = wrong results / data risk, **P2** = operational, **P3** = documented design constraint.

### P0-1 — `pool.query()` deadlocks (schema, health, check)

**What.** `createPool()` replaces `pool.connect` with an `async` function that **ignores the node-postgres callback**. `Pool.query()` internally calls `connect(callback)`. The callback never runs.

**Evidence.**

```
db.query("SELECT 1")            -> returns in 27ms
db.pool.query("SELECT 1")       -> TIMEOUT after 2000ms
db.health() / db.check()        -> same hang (createCheck uses pool.query)
db.schema.create.table(...)     -> same hang
```

`pool.connect.length === 0` after wrap.

**Where.** `src/client/pool.ts` (`pool.connect = async function() { ... }`).
Callers: `src/schema/create.ts`, `alter.ts`, `drop.ts`, `constrain.ts`, `introspect.ts`, `src/client/check.ts`.

**Impact.**

- `db.schema.*` is unusable. DDL must go through `db.query(...)`.
- `db.health()` / `db.check()` cannot be used as readiness probes; they hang instead of returning `{ status: "down" }`.
- Each hung `pool.query` also **leaks a pool slot**. A later `db.query` then hits `timeout exceeded when trying to connect`. `db.shutdown()` / `pool.end()` wait forever.

**Workaround.** Never call `db.pool.query`, `db.schema`, `db.health`, or `db.check`. Use `db.query("SELECT 1")` for health.

---

### P0-2 — `db.query(sql\`...\`)` drops fragment parameters

**What.** The `sql` tagged template builds `{ text, values }` correctly, but `unifiedQuery` / `createClient` does not treat a fragment as a query config with values.

**Evidence.**

```
sql`SELECT ${41} AS a, ${1} AS b`
  text   = "SELECT $1 AS a, $2 AS b"
  values = [41, 1]

db.query(frag)                  -> error: there is no parameter $1
db.query(frag.text, frag.values) -> [{ a: 41, b: 1 }]
```

Also `sql\`SELECT $1::int AS v, ${7} AS n\`` compiles to `SELECT $1::int AS v, $1 AS n` with values `[7]` — the literal `$1` in the template is **not** shifted, so mixed handwritten placeholders plus interpolations collide.

**Impact.** The README quick-start `db.query(sql\`...\`)` pattern fails on a real server.

**Workaround.** `db.query(frag.text, frag.values)` or pass a `{ text, values }` object that `createClient.buildQuery` understands.

---

### P0-3 — Optimistic locking breaks `updateById` / `deleteById` / soft-delete

**What.** With `optimisticLocking: { column: "version" }`, `update()` requires `version` in the WHERE object. `updateById(id, data)` and `deleteById` (soft-delete path calls `update`) do not inject the id-only where with version.

**Evidence.** After a successful `insert` + `findById`:

```
accounts.updateById(a1.id, { score: 15, version: 1 })
  -> TypeError: Optimistic locking requires "version" in the WHERE clause
accounts.deleteById(a2.id)
  -> same TypeError  (soft-delete -> update)
```

**Where.** `src/model/model.ts` around the optimistic-lock WHERE check (~line 708) and `remove()` (~769).

**Impact.** Enabling optimistic locking makes the documented `updateById` / `deleteById` APIs unusable unless the caller uses `update({ id, version }, data)`.

---

### P1-1 — `queryAllShards({ merge: "application" })` returns `[{}]`

**What.** `fanOut` collects real rows (`fanOut` returned `{c:1}` per shard), but `queryAllShards` defaults `merge` to `"application"`. The merge switch only handles `sum` / `min` / `max`. `"application"` plus a numeric column produces an empty object.

**Evidence.** Eight users inserted across three shards:

```
queryAllShards({ query: "SELECT COUNT(*)::int AS c FROM users", merge: "application" })
  results: [{}]
  successfulShards: [shard-0, shard-2, shard-1]
  expected total 8, observed 0
```

Direct `db.sharding.fanOut({ query, merge omitted })` returned `[{c:1},{c:1}]`.

**Where.** `src/database.ts` `fanOutApi` merge block (~671–698). `queryAllShardsApi` forces `merge: "application"`.

**Impact.** Cross-shard aggregations silently lose counts. Not a global snapshot either way (independent per-shard transactions).

**Workaround.** Call `fanOut` with `merge: "none"` or `"sum"`.

---

### P1-2 — 2PC abort leaves the participant connection aborted (`25P02`)

**What.** Happy-path 2PC **commit** across shard-0 and shard-1 worked (50/50 -> 40/60). A later 2PC that runs `SELECT 1/0` on one participant throws `division by zero`, then the **next** `db.queryOne` on that shard fails:

```
error: current transaction is aborted, commands ignored until end of transaction block (25P02)
  at unifiedQuery (src/database.ts:1485)  -- client.query("BEGIN")
```

**Where.** Distributed tx participant client is reused without `ROLLBACK` after a SQL error. `unifiedQuery` also issues `BEGIN` on `targetPool.connect()` when `reshardingApi` is attached (always, for sharded writes).

**Impact.** One failed 2PC statement can poison a pooled connection. Subsequent reads/writes on that shard fail until the connection is destroyed. Matches the documented “orphaned prepared transactions / no automatic recovery” class of bugs; this is the live symptom.

---

### P1-3 — `db.query(sql fragment)` and handwritten `$1` in the same template

`sql` does not parse existing `$n` in the static string. `sql\`SELECT $1::int AS v, ${7} AS n\`` becomes two uses of `$1` and one value.

---

### P2-1 — Sharded `db.transaction()` without `shardKey` uses the default shard

Did not throw `CrossShardTransactionError`. Un-keyed writes go to `defaultShardId`.

### P2-2 — Missing `defaultShardId` only `console.warn`s

`createDatabase({ sharding: { enabled, shards }})` succeeded and fell back to the first shard.

### P2-3 — Schema auto-sync is off unless configured

`db.schemaSync` was `undefined` with only `sharding.enabled`. Multi-shard DDL drift is not repaired.

### P2-4 — Failover does not elect a primary

`StaticHaProvider` / `FailoverManager` are exported. There is no built-in election. A dead primary stays dead without Patroni / pg_auto_failover / a custom `HaProvider`.

### P2-5 — No global connection budget

Each shard has its own `pg.Pool`. 3 shards * `max=4` plus coordination plus replicas can exceed `max_connections` (this env used 80).

### P2-6 — Hardcoded timers, not `unref()`’d

`CONNECTION_LEAK_TIMEOUT = 300000` in `client.ts` / `pool.ts`. Cursor read timeout 30s, hold warning 5m. Leak `setTimeout` is not `.unref()`. Hung `pool.query` plus these timers keep the process alive (`shutdown` 10s default can also hang — P0-1).

### P2-7 — Module-level `Set`s retain clients

`activeConnections` in `pool.ts`, `manualClients` in `client.ts` are never cleared on `shutdown()`.

### P2-8 — Coordination DB is a SPOF for 2PC

Coordinator state lives in one `coordinationPool`. This run used port 5443. If that instance dies, in-flight 2PC cannot record prepare/commit decisions.

### P2-9 — Query retry is opt-in

`SELECT 1/0` failed once. Serialization failures (`40001`) / deadlocks (`40P01`) are not retried unless `retry` is passed. No default `queryTimeout` unless you set it.

### P2-10 — `onConflict: "nothing"` as a string is rejected on `batch.insertMany`

Must pass `{ onConflict: true | "nothing" as special }` — actually the batch helper throws `TypeError` if `onConflict` is a string other than via object. Confirmed: string `"nothing"` throws (this is intentional, but easy to get wrong vs README).

---

## Engine / product limitations (confirmed, not just docs)

| Limitation | Reality in this run |
| :--- | :--- |
| PostgreSQL only | `package.json` depends only on `pg`. No MySQL/SQLite. |
| Node >= 20 | Ran on Node v22.22.0 |
| PostgreSQL 13+ | Used 15.19. Upstream tests want `postgres:16`. |
| Streaming peers optional | `pg-cursor` / `pg-query-stream` were installed; cursor+stream passed |
| `max_prepared_transactions > 0` | Set to 100; 2PC commit succeeded. If 0, `MaxPreparedTransactionsError` |
| Physical replication not in this env | Five **independent primaries**. `replica-preferred` still returned data (fell back to primary / unrelated instance). Lag via `pg_last_xact_replay_timestamp()` needs a real standby. |
| Online resharding | `db.resharding` object exists when sharding is on, but copy/cutover was **not** run (needs durable gate table + planned topology change). |
| Hash remapping | `hash` strategy remaps keys when shard count changes. Use `consistent-hash` / `rendezvous` if you add shards. |

---

## Bugs vs documented “known limitations”

The README already lists several of the P2 items (retry off, no global pool cap, hardcoded timeouts, default shard fallback, external HA, 2PC orphan cleanup, coordination SPOF).

**Not adequately covered by the README, and broken on a real server:**

1. `pool.connect` callback incompatibility → schema/health/check deadlock (P0-1)
2. `db.query(sql\`...\`)` drops values (P0-2)
3. Optimistic locking vs `updateById`/`deleteById` (P0-3)
4. `queryAllShards` merge `"application"` returns `{}` (P1-1)
5. Failed 2PC poisons the next query with `25P02` (P1-2)

---

## How to re-run

```bash
bash real-env/init-clusters.sh
bash real-env/start-postgres.sh
# clone already at /tmp/oneunitjs-database with npm install
cd /tmp/oneunitjs-database
node --import tsx /workspace/real-env/run-real-tests.mjs
```

Do **not** call `db.health()`, `db.check()`, or `db.schema.*` in the same process you still need — they leak pool slots (P0-1).
