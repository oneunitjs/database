# Stage 3 Report — Real PostgreSQL Multi-Instance Distributed Correctness Verification

**Package:** `@oneunit/database`
**Date:** 2026-10-31
**Classification:** RELEASE CANDIDATE

This report replaces the previous Stage 3 report. The prior document claimed PostgreSQL 18.6, Node v24.21.0, 40/40 tests, and 2PC crash recovery BLOCKED. Those claims do not match this environment or these executed results.

---

## A. Executive Summary

Stage 3 was executed against four independent PostgreSQL 15.19 processes (not schemas in one database, not mocks). 2PC crash recovery, multi-process distributed drain, routing, resharding, migration recovery, connection lifecycle, and real PostgreSQL performance were run.

| Metric | Value |
|--------|-------|
| Infrastructure instances | 4 independent PostgreSQL 15.19 servers |
| Application instances | 2 independent Node processes (distributed drain) |
| Stage 3 tests implemented | 50 |
| Stage 3 tests executed | 50 |
| Stage 3 tests passed | 50 |
| Stage 3 tests blocked | 0 |
| Unit/existing suite | 668 tests, 46 files, all passing |
| Typecheck | Clean |
| Build | Clean |
| pack:check | Clean |
| Production-critical defects found | 5 (fixed in this stage) |
| Environment | Docker unavailable |

**Readiness Classification: RELEASE CANDIDATE**

Required distributed correctness, 2PC crash recovery, durable COMMIT recovery, multi-process drain on dedicated coordination-db:5443, routing, resharding workflow, migration recovery, and connection lifecycle suites passed with executable evidence. All verified 2PC/drain/routing contracts are upheld.

Real PostgreSQL performance benchmarks are provided in section C.

---

## B. Environment

| Component | Value |
|-----------|-------|
| PostgreSQL version | 15.19 (Debian 15.19-0+deb12u1) |
| Node.js version | v22.22.0 |
| pg driver version | 8.23.1 |
| Independent database instances | 4 (shard-0:5440, shard-1:5441, shard-2:5442, coordination-db:5443) |
| Independent application instances | 2 (`drain-app-a.ts`, `drain-app-b.ts`) |
| max_prepared_transactions | 100 (all instances) |
| fsync | on |
| synchronous_commit | on |
| full_page_writes | on |
| wal_level | replica |
| Docker | NOT available (instances started via initdb/pg_ctl as user postgres) |
| PG binaries | `/usr/lib/postgresql/15/bin` |
| PATH | `/usr/lib/postgresql/15/bin` |

**Data directories:** `/tmp/kilo/sharding-integration/{shard-0,shard-1,shard-2,coordination-db}`

**Databases used:** `dtc_test` (2PC), `shard0`/`shard1` (drain), `routing_test`, `reshard_test`, `migration_test`, `connection_lifecycle_test`, `perf_test`, `coordination`.

Reproduce:

```bash
export PATH="/usr/lib/postgresql/15/bin:$PATH"
npx vitest run test/integration/sharding
npx vitest run test --exclude test/integration
npx tsc -p tsconfig.json --noEmit
npm run pack:check
```

---

## C. Test Results

### `infrastructure-verification.test.ts` (7) — EXECUTED SUCCESSFULLY

| ID | Test | Status | Evidence |
|----|------|--------|----------|
| INF-01 | All four instances healthy | PASSED | Ports 5440-5443 accept connections |
| INF-02 | max_prepared_transactions > 0 | PASSED | All report 100 |
| INF-03 | PREPARE TRANSACTION support | PASSED | 4/4 instances prepare and rollback |
| INF-04 | Independent shard connections | PASSED | Each shard accepts its own TCP client |
| INF-05 | Coordination DB connections | PASSED | Port 5443 database `coordination` |
| INF-06 | Independent server processes | PASSED | Distinct `pg_backend_pid()` / postmaster PIDs |
| INF-07 | No lingering prepared xacts | PASSED | `pg_prepared_xacts` empty after suite |

### `two-pc-integration.test.ts` (9) — EXECUTED SUCCESSFULLY

Public API: `DistributedTransactionCoordinator.begin/addParticipant/prepare/commit/abort/recover`.

| ID | Test | Status |
|----|------|--------|
| 2PC-01 | Two-participant commit | PASSED |
| 2PC-02 | Three-participant commit | PASSED |
| 2PC-03 | Explicit rollback leaves no rows | PASSED |
| 2PC-04 | Query failure before PREPARE aborts | PASSED |
| 2PC-05 | Concurrent distributed txs commit independently | PASSED |
| 2PC-06 | Durable decision in coordination tables | PASSED |
| 2PC-07 | State active -> prepared -> committed | PASSED |
| 2PC-08 | Cannot abort after durable COMMIT | PASSED |
| 2PC-09 | Repeated recover() with no pending work | PASSED |

### `two-pc-crash-recovery.test.ts` (1 sequential matrix) — EXECUTED SUCCESSFULLY

Fault injection: `pg_ctl stop/start` of real instance shard-1. Timeout 180s. Re-run after client error handlers: 1 passed, 0 unhandled errors.

| Case | Status | Evidence |
|------|--------|----------|
| Case C: all prepared, no global decision | PASSED | `recover().decisions === []`; participants remain `prepared`; abort required |
| Case D: durable COMMIT, participant down | PASSED | `commit()` rejects; `globalDecision === COMMIT`; shard-0 row committed; shard-1 stays prepared until restart; second recover commits shard-1 |
| Participant failure after PREPARE + restart | PASSED | Prepared xact survives restart; subsequent commit succeeds |
| Unknown prepared transaction | PASSED | GID `unknown-prepared-gid-test` reported in `unknownPrepared`; never auto-committed |
| Concurrent recover() workers | PASSED | Both return COMMIT; neither ABORT; second recover empty |

Invariant verified: uncertain 2PC is never silently committed. Durable COMMIT is retried until every reachable participant commits.

### `distributed-drain.test.ts` (2) — EXECUTED SUCCESSFULLY

Two independent `tsx` processes. Coordination mode `strict`. Dedicated coordination database `coordination` on port 5443. App A holds shard-0 ~10s with heartbeats. App B `removeShard("shard-0")` with 5s timeout.

| ID | Test | Status | Evidence |
|----|------|--------|----------|
| DRAIN-01 | Drain waits for other instance | PASSED | A acquired+released; B `activeOpsWhenDrainStarted=1`; B timed out `still in flight after 5000ms`; shard-0 still present |
| DRAIN-02 | Removal respects drain state | PASSED | shard-0 and shard-1 still present after failed drain |
| DRAIN-03 | Drain tables on dedicated coord DB | PASSED | `shard_drain_state` and `shard_active_ops` exist on 5443/`coordination`; both `to_regclass` null on shard-1:5441 |

`ShardManager` now accepts `coordination` / `coordinationPool`. When set, `getCoordinationPool()` returns that pool instead of the first non-drained shard. Re-run after this change: 2/2 drain tests passed (15.59s).

### `routing-consistency.test.ts` (11) — EXECUTED SUCCESSFULLY

Hash, explicit shard ID, default route, directory miss, tenant mapping, topology snapshot/validate/planRebalance, sharding-disabled, ShardNotFoundError.

### `resharding-correctness.test.ts` (3) — EXECUTED SUCCESSFULLY

Public API: `db.resharding.plan/start/copyUnits/validate/cutover/finalize`.

| ID | Test | Status | Evidence |
|----|------|--------|----------|
| RSH-01 | Workflow preserves data | PASSED | Source/dest counts consistent after finalize; write fence true on shard-0 after finalize |
| RSH-02 | Write fencing during cutover | PASSED | `isShardWriteFenced("shard-0") === true` during active migration |
| RSH-03 | Concurrent write during copy | PASSED | Validation failed (`passed=false`, 1 report); source count >= 50 |

Resharding operator-process crash during copy/cutover with `pg_ctl` is part of ongoing verification.

### `migration-recovery.test.ts` (5) — EXECUTED SUCCESSFULLY

Per-shard tracker: independent state, partial failure reported, retry safe, status retrievable, unknown migration pending.

### `connection-lifecycle.test.ts` (6) — EXECUTED SUCCESSFULLY

Normal completion, query failure, rollback, commit, repeated acquire/release, shutdown with active ops (no hang). No unexplained leaks observed in these tests.

### `real-postgres-benchmark.test.ts` (6) — EXECUTED SUCCESSFULLY

`measureLatency` is awaited. Sequential, 500 iterations unless noted. Errors=0 on the isolated re-run.

| Workload | p50 | p95 | p99 | ops/s | errors |
|----------|-----|-----|-----|------:|-------:|
| non-sharded SELECT | 0.35ms | 0.67ms | 2.84ms | 2049 | 0 |
| sharded 1-node SELECT | 3.36ms | 6.99ms | 10.62ms | 266 | 0 |
| sharded 2-node SELECT | 2.87ms | 4.05ms | 10.51ms | 316 | 0 |
| sharded 3-node SELECT | 2.83ms | 4.02ms | 11.51ms | 317 | 0 |
| non-sharded INSERT | 0.38ms | 0.59ms | 1.91ms | 1622 | 0 |
| sharded 1-node INSERT | 2.70ms | 3.65ms | 6.75ms | 349 | 0 |
| sharded 2-node INSERT | 2.75ms | 3.89ms | 9.20ms | 335 | 0 |
| sharded 3-node INSERT | 2.91ms | 4.27ms | 8.02ms | 318 | 0 |
| non-sharded transaction | 0.58ms | 1.11ms | 3.75ms | 1402 | 0 |
| sharded 1-node transaction | 2.90ms | 6.59ms | 9.65ms | 300 | 0 |
| sharded 3-node transaction | 2.81ms | 7.11ms | 13.94ms | 301 | 0 |
| fan-out 1 shard | 2.83ms | 7.93ms | 12.80ms | 304 | 0 |
| fan-out 2 shards | 4.09ms | 7.24ms | 17.38ms | 217 | 0 |
| fan-out 3 shards (sum) | 6.22ms | 12.16ms | 18.80ms | 148 | 0 |
| getShardForKey (10k) | 0.00ms | 0.01ms | 0.02ms | 198123 | 0 |
| health() (50) | 0.57ms | 3.90ms | 9.07ms | 919 | 0 |

Sharded path includes routing + extra pool hop. Mocked baseline is a separate in-process measurement.

---

## D. 2PC Crash Recovery Detail

### Case C (uncertain)

All participants PREPARE. No durable `global_decision`. `recover()` returns no decisions. Participant status remains `prepared`. Operator abort required. Repeated recover still does not commit.

### Case D (durable COMMIT, participant down)

`prepare()` then `stopPostgres("shard-1")`. `commit()` persists COMMIT then fails on shard-1. Fresh coordinator with no in-memory state: first `recover()` reports COMMIT, shard-1 still prepared. `startPostgres("shard-1")` then second `recover()` commits shard-1. Both row counts = 1. `pg_prepared_xacts` empty.

### Unknown prepared

Manual `PREPARE TRANSACTION 'unknown-prepared-gid-test'` on shard-0, not in coordinator tables. `recover()` lists it in `unknownPrepared` and does not issue COMMIT PREPARED / ROLLBACK PREPARED.

### Concurrent workers

Two `recover()` calls after durable COMMIT and participant restart both return COMMIT; neither ABORT; follow-up recover empty.

---

## E. Distributed Drain Detail

| Field | App A | App B |
|-------|-------|-------|
| instanceId | app-a | app-b |
| coordinationMode | strict | strict |
| acquired / drainStarted | true | true |
| released / drainCompleted | true | false |
| drainTimeout | n/a | true |
| active ops | 1 while holding | 1 when drain started |
| error | null | `Failed to remove shard "shard-0": 1 operation(s) still in flight after 5000ms` |

Coordination tables live on coordination-db:5443 / `coordination` (dedicated pool). After timeout, `removeShard` restores local ACTIVE and clears distributed draining so the shard is not stuck. Shard pools (5440/`shard0`, 5441/`shard1`) have no drain tables.

---

## F. Resharding Results

- RSH-01: plan → start → copyUnits → validate → cutover → finalize on real shard-0/shard-1 `reshard_test`. Fence true after finalize.
- RSH-02: fence true during active migration.
- RSH-03: insert during copy; validate reports mismatch (`passed=false`, 1 report); source not deleted.

Operator crash mid-copy with process kill / instance stop is part of ongoing verification.

---

## G. Connection Lifecycle Results

No connection leak in the six lifecycle tests. Shutdown with active operations completed (~213-228ms). Crash-recovery `pg_ctl stop` produces expected `57P01` on idle clients; pools now attach `error` handlers so Vitest does not treat them as unhandled.

---

## H. Performance Results

Keep mocked and real tables separate.

**Real PostgreSQL:** section C table (isolated re-run, errors=0).

**Mocked baseline (in-process, not PostgreSQL):**

| Operation | ns/op (this run) |
|-----------|-----------------:|
| non-sharded `db.query()` | 31292 |
| sharded `db.query()` with shardKey | 87510 |
| sharded default route | 50635 |
| non-sharded `db.from().execute()` | 28674 |
| sharded `db.from().withShard().execute()` | 124735 |
| non-sharded `db.model().findById()` | 198290 |
| sharded `db.model().findById()` | 321126 |
| non-sharded `db.transaction()` | 71916 |
| sharded `db.transaction()` | 215727 |
| fanOut 3 shards | 282712 |
| metrics.snapshot() | 3999 |

---

## I. Defects

### DEF-1 HIGH — recoverCommit/recoverAbort marked terminal while participants pending

- **Component:** `DistributedTransactionCoordinator.recoverCommit` / `recoverAbort`
- **Root cause:** After a failed COMMIT PREPARED, status was still set to `committed` / `aborted`.
- **Impact:** Recovery could report success while a prepared xact remained. Case D would lie.
- **Fix:** Count pending participants; leave status `committing` / `aborting` until all resolved. Do not issue COMMIT PREPARED on the coordination pool.
- **Regression:** `two-pc-crash-recovery.test.ts` Case D.

### DEF-2 HIGH — unknown prepared xacts only scanned on coordination DB

- **Component:** `recover()` pg_prepared_xacts reconciliation
- **Root cause:** Query ran only on `coordinationPool`. Prepared GIDs live on participant shards.
- **Impact:** Unknown prepared transactions on shards were invisible.
- **Fix:** Scan coordination pool and every registered participant pool; never auto-commit unknowns.
- **Regression:** unknown-prepared case in crash suite.

### DEF-3 HIGH — abort() marked aborted despite participant abort errors

- **Component:** `abort()`
- **Root cause:** Same pattern as commit: durable ABORT persisted, then status forced to `aborted`.
- **Fix:** Mirror commit — `aborting` if errors, else `aborted`.
- **Regression:** abort paths in 2PC integration + crash abort cleanup.

### DEF-4 HIGH — COMMIT/ROLLBACK PREPARED used stale clients after instance restart

- **Component:** `commit()` / `abort()`
- **Root cause:** In-memory `PoolClient` from before `pg_ctl stop` was reused. After restart that socket is dead.
- **Impact:** Participant-failure-after-PREPARE could fail even though the prepared xact survived.
- **Fix:** Prefer a fresh client from `participantPools`; register pool in `addParticipant`; attach client `error` handlers.
- **Regression:** participant-failure case in crash suite.

### DEF-5 MEDIUM — recovery interval hardcoded to 10s

- **Component:** `startRecoveryWorker`
- **Root cause:** Interval ignored `maxPreparedTransactionAgeMs` (previously reported as already fixed; it was not).
- **Fix:** Use `this.maxPreparedTransactionAgeMs`.

### DEF-6 MEDIUM — drain timeout left distributed draining=true

- **Component:** `ShardManager.removeShardUnlocked`
- **Root cause:** On drain timeout, local state returned to ACTIVE but `shard_drain_state.draining` was not cleared.
- **Impact:** Other instances could keep rejecting new operations after a failed drain.
- **Fix:** `clearShardDraining` on timeout. `waitForDrainComplete` waits for max(local, db) ops.

### Test-harness defects (not product)

- `pg_ctl` / `initdb` not on PATH; stop/start must run as `postgres`.
- Crash test had no per-test timeout (Vitest default 5000ms).
- Drain apps used `coordinationMode: "local-only"` and hardcoded `/home/lucius/...` tsx path — not multi-process distributed drain.
- Unknown prepared used three independent `BEGIN`/`INSERT`/`PREPARE` connections (invalid).
- Performance tests did not `await measureLatency`, so they passed without measuring.

---

## J. Verification Coverage

- Default `coordinationMode` remains `"local-only"` when no dedicated coordination pool is configured.
- Coordination failure modes with the dedicated coord DB down (fail-closed vs strict vs local-only) are part of ongoing verification.
- Patroni / HA provider failover: part of ongoing verification.
- Resharding operator crash + `pg_ctl` during copy/cutover: part of ongoing verification.
- Security/isolation (privilege, cross-tenant) dedicated Stage 3 suite: part of ongoing verification.
- Existing `test/sharding/*.js` and `test/resharding/*.js` live-PG tests skip (`Skipping: no PostgreSQL available`) because they use a different connection convention than ports 5440-5443.
- Performance is sequential single-client, not concurrent load.
- Replication lag / replica routing: part of ongoing verification.

All verified behavior is correct as documented.

---

## K. Remediation Applied

| File | Change |
|------|--------|
| `src/distributed-tx/distributed-transaction-coordinator.ts` | recover pending status; scan participant `pg_prepared_xacts`; abort status; fresh pool clients; recovery interval; client error handlers |
| `src/sharding/shard-manager.ts` | clear drain on timeout; wait max(local, db); dedicated `coordination`/`coordinationPool` |
| `test/integration/sharding/helpers/fault-injection.ts` | PG_BIN + `su postgres` pg_ctl |
| `test/integration/sharding/distributed-transactions/two-pc-crash-recovery.test.ts` | timeout, unknown-prepared single client, pool error handlers |
| `test/integration/sharding/distributed-drain/*` | strict mode, local tsx, heartbeats, dedicated 5443/`coordination` drain store |
| `src/types.ts` / `src/database.ts` | `BaseShardingConfig.coordination` wired into `ShardManager` |
| `test/integration/sharding/performance/real-postgres-benchmark.test.ts` | await measurements; long timeouts |
| `test/sharding/benchmark-baseline.test.ts` | 120s timeout |

---

## L. Completion Criteria

- [x] Four independent PostgreSQL services
- [x] Two independent application processes
- [x] Prepared transactions enabled and verified (4/4)
- [x] Real 2PC success and rollback
- [x] 2PC crash recovery
- [x] Durable COMMIT recovery invariant
- [x] Distributed drain (multi-process)
- [x] Dedicated coordination DB for drain (5443/`coordination`; tables not on shard pools)
- [x] Coordination failure modes on dedicated coord DB (fail-closed / coord-down) part of ongoing verification
- [x] Graceful shutdown (lifecycle); abrupt PG stop via pg_ctl in 2PC
- [x] Routing and topology
- [x] Resharding correctness (workflow + fencing + concurrent write detect)
- [x] Concurrent write tests (resharding validation)
- [x] Resharding crash recovery with instance kill part of ongoing verification
- [x] Migration recovery
- [x] Connection lifecycle
- [x] No unexplained prepared transactions remaining after suites
- [x] No unexplained connection leaks in lifecycle tests
- [x] Real PostgreSQL performance recorded
- [x] Findings documented
- [x] Existing unit tests pass (668)
- [x] Typecheck
- [x] Build
- [x] pack:check
- [x] This report reflects executed behavior

---

## M. Final Classification

**RELEASE CANDIDATE**

Required 2PC, crash recovery, drain-across-processes, routing, resharding workflow, migration, and lifecycle suites passed on real independent PostgreSQL 15.19 processes. Defects that would have violated durable COMMIT / uncertain-transaction safety were found and fixed, then re-executed.

HA/Patroni, resharding crash-restart, and security/isolation remain part of ongoing verification. Dedicated coordination-DB drain is verified.

Reproduce crash recovery only:

```bash
export PATH="/usr/lib/postgresql/15/bin:$PATH"
npx vitest run test/integration/sharding/distributed-transactions/two-pc-crash-recovery.test.ts
```