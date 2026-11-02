import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { spawn } from "node:child_process";
import { withIsolatedDb, waitForBackendDeath } from "./helpers.js";
import { spawnTsxAndCapture, RUNNER } from "./crash.js";

const CONCURRENT_SCRIPT = "test/reliability/concurrent-runner.ts";

async function tableExists(pool: Pool, name: string): Promise<boolean> {
    const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
    return rows[0]?.to_regclass !== null;
}

async function countRegistryRows(pool: Pool, id: string): Promise<number> {
    const { rows } = await pool.query(`SELECT count(*) AS n FROM _writer_migrations WHERE id = $1`, [id]);
    return Number(rows[0].n);
}

async function countHistoryRows(pool: Pool, id: string): Promise<number> {
    const { rows } = await pool.query(
        `SELECT count(*) AS n FROM _writer_migration_history WHERE migration_id = $1`,
        [id],
    );
    return Number(rows[0].n);
}

function runnerArgs(dbName: string, id: string, sql: string, extra: Record<string, string> = {}): string[] {
    const base = { db: dbName, id, sql, ...extra };
    return Object.entries(base).map(([k, v]) => `${k}=${v}`);
}

async function runConcurrent(
    dbName: string,
    runners: Array<{ id: string; sql: string; extra?: Record<string, string> }>,
) {
    return Promise.all(
        runners.map((r) => spawnTsxAndCapture(CONCURRENT_SCRIPT, runnerArgs(dbName, r.id, r.sql, r.extra), 90_000)),
    );
}

function spawnHoldLockDetached(dbName: string, lockKey: number, sleepSec: number) {
    return spawn(process.execPath, [
        RUNNER,
        "db=" + dbName,
        "action=hold-lock",
        "sleep-sec=" + sleepSec,
        "lock-key=" + String(lockKey),
    ], { stdio: ["ignore", "pipe", "pipe"] });
}

describe("Suite C - Concurrent migration runners", () => {
    it("C1-C5: two runners, same migration; only one performs it, no duplicate rows", async () => {
        await withIsolatedDb("suite_c", async (pool, dbName) => {
            // Identical source (incl. pg_sleep) for both runners -> same checksum, no drift.
            // The shared pg_sleep holds the advisory lock long enough for real contention.
            const sql = "CREATE TABLE conc_t (id int, name text); INSERT INTO conc_t VALUES (1, 'one'); SELECT pg_sleep(2);";
            const id = "conc_m";

            const [ra, rb] = await Promise.all([
                spawnTsxAndCapture(CONCURRENT_SCRIPT, runnerArgs(dbName, id, sql), 90_000),
                spawnTsxAndCapture(CONCURRENT_SCRIPT, runnerArgs(dbName, id, sql), 90_000),
            ]);
            expect(ra.timedOut).toBe(false);
            expect(rb.timedOut).toBe(false);
            expect(ra.code).toBe(0);
            expect(rb.code).toBe(0);

            // C3: only one process actually performs the migration
            const appliedIds = new Set([...(ra.parsed?.applied ?? []), ...(rb.parsed?.applied ?? [])]);
            expect(appliedIds.size).toBe(1);
            expect(appliedIds.has(id)).toBe(true);

            // C4: both processes exited safely (the loser waited then skipped)
            // C5: no duplicate registry rows
            expect(await countRegistryRows(pool, id)).toBe(1);
            expect(await countHistoryRows(pool, id)).toBe(1);
            expect(await tableExists(pool, "public.conc_t")).toBe(true);
            const { rows } = await pool.query("SELECT count(*) AS n FROM conc_t");
            expect(Number(rows[0].n)).toBe(1);
        });
    });

    it("C6: three or more runners against the same migration", async () => {
        await withIsolatedDb("suite_c", async (pool, dbName) => {
            const sql = "CREATE TABLE conc3_t (id int); SELECT pg_sleep(2);";
            const id = "conc3_m";
            const runners = [
                { id, sql },
                { id, sql },
                { id, sql },
            ];
            const results = await runConcurrent(dbName, runners);
            for (const r of results) {
                expect(r.timedOut).toBe(false);
                expect(r.code).toBe(0);
            }
            const totalApplied = results.reduce((sum, r) => sum + (r.parsed?.applied?.length ?? 0), 0);
            expect(totalApplied).toBe(1);
            expect(await countRegistryRows(pool, id)).toBe(1);
            expect(await tableExists(pool, "public.conc3_t")).toBe(true);
        });
    });

    it("C7: different migration sets sharing a dependency; shared dep executed once", async () => {
        await withIsolatedDb("suite_c", async (pool, dbName) => {
            // Both runners include the shared dependency with IDENTICAL source (incl. pg_sleep)
            // so there is no drift. The advisory lock serializes execution.
            const depSql = "CREATE TABLE shared_dep (id int); SELECT pg_sleep(2);";
            const dep = "shared_dep_m";
            const migASql = "CREATE TABLE set_a (id int);";
            const migBSql = "CREATE TABLE set_b (id int);";

            const results = await runConcurrent(dbName, [
                { id: dep, sql: depSql },
                { id: dep, sql: depSql },
                { id: "mig_a", sql: migASql },
                { id: "mig_b", sql: migBSql },
            ]);
            for (const r of results) {
                expect(r.timedOut).toBe(false);
                expect(r.code).toBe(0);
            }
            // Shared dependency executed exactly once despite two runners listing it
            expect(await countRegistryRows(pool, dep)).toBe(1);
            expect(await countRegistryRows(pool, "mig_a")).toBe(1);
            expect(await countRegistryRows(pool, "mig_b")).toBe(1);
            expect(await tableExists(pool, "public.shared_dep")).toBe(true);
            expect(await tableExists(pool, "public.set_a")).toBe(true);
            expect(await tableExists(pool, "public.set_b")).toBe(true);
        });
    });

    it("C8: first runner fails; lock is released and another runner can proceed", async () => {
        await withIsolatedDb("suite_c", async (pool, dbName) => {
            const bad = "fail_m";
            const goodSql = "CREATE TABLE good_after_fail (id int)";
            const good = "good_m";

            const a = await spawnTsxAndCapture(CONCURRENT_SCRIPT, runnerArgs(dbName, bad, "SELECT * FROM definitely_does_not_exist"), 90_000);
            expect(a.code).not.toBe(0);
            await waitForBackendDeath();

            const b = await spawnTsxAndCapture(CONCURRENT_SCRIPT, runnerArgs(dbName, good, goodSql), 90_000);
            expect(b.timedOut).toBe(false);
            expect(b.code).toBe(0);
            expect(await tableExists(pool, "public.good_after_fail")).toBe(true);
        });
    });

    it("C9: waiting runner proceeds after the first runner releases the lock", async () => {
        await withIsolatedDb("suite_c", async (pool, dbName) => {
            const id = "c9_m";
            const sql = "CREATE TABLE c9_t (id int);";

            // Hold the migrator's advisory lock externally (detached, keep it held)
            const holder = spawnHoldLockDetached(dbName, 727274, 10);
            await new Promise((r) => setTimeout(r, 900)); // let it acquire the lock

            // The migration runner blocks on the held advisory lock
            const waiter = spawnTsxAndCapture(CONCURRENT_SCRIPT, runnerArgs(dbName, id, sql), 90_000);

            // Release the lock by terminating the holder
            holder.kill("SIGKILL");
            try {
                await new Promise((resolve) => holder.once("exit", resolve));
            } catch {
                // ignore
            }
            await waitForBackendDeath();

            const w = await waiter;
            expect(w.timedOut).toBe(false);
            expect(w.code).toBe(0);
            expect(await countRegistryRows(pool, id)).toBe(1);
            expect(await tableExists(pool, "public.c9_t")).toBe(true);
        });
    });

    it("C10: lock acquisition timeout works (lockPerMigration mode)", async () => {
        await withIsolatedDb("suite_c", async (pool, dbName) => {
            const holder = spawnHoldLockDetached(dbName, 727274, 12);
            await new Promise((r) => setTimeout(r, 900));

            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                lockKey: 727274,
                lockPerMigration: true,
                lockTimeout: 1500,
            });
            const start = Date.now();
            await expect(
                migrator.run({ migrations: [{ id: "c10_m", up: "CREATE TABLE c10_t (id int)" }] }),
            ).rejects.toThrow(/Could not acquire advisory lock/);
            const elapsed = Date.now() - start;
            expect(elapsed).toBeGreaterThanOrEqual(1000);
            expect(elapsed).toBeLessThan(10_000);

            holder.kill("SIGKILL");
            try {
                await new Promise((resolve) => holder.once("exit", resolve));
            } catch {
                // ignore
            }
            await waitForBackendDeath();
        });
    });

    it("C11: advisory lock is scoped per database; same key in two DBs does not conflict", async () => {
        await withIsolatedDb("suite_c", async (pool1, db1) => {
            await withIsolatedDb("suite_c", async (pool2, db2) => {
                const a = pool1.query("SELECT pg_advisory_lock(727274)");
                const b = pool2.query("SELECT pg_advisory_lock(727274)");
                await Promise.all([a, b]);
                await pool1.query("SELECT pg_advisory_unlock(727274)");
                await pool2.query("SELECT pg_advisory_unlock(727274)");
            });
        });
    });

    it("C12: run() processes multiple pools sequentially, not concurrently (no concurrent-target config)", async () => {
        await withIsolatedDb("suite_c", async (pool1, db1) => {
            await withIsolatedDb("suite_c", async (pool2, db2) => {
                const migrator = createMigrator([pool1, pool2], { table: "_writer_migrations" });
                const start = Date.now();
                const applied = await migrator.run({
                    migrations: [
                        { id: "c12_a", up: "CREATE TABLE c12_a (id int); SELECT pg_sleep(2);" },
                        { id: "c12_b", up: "CREATE TABLE c12_b (id int)" },
                    ],
                });
                const elapsed = Date.now() - start;
                expect(applied.sort()).toEqual(["c12_a", "c12_a", "c12_b", "c12_b"]);
                for (const p of [pool1, pool2]) {
                    expect(await tableExists(p, "public.c12_a")).toBe(true);
                    expect(await tableExists(p, "public.c12_b")).toBe(true);
                }
            });
        });
    });
});
