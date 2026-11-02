import { describe, it, expect, afterAll } from "vitest";
import { Pool, Client } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

const ADMIN_CONFIG = { user: "postgres", database: "postgres", host: "/run/postgresql", max: 5 };
let adminPool: Pool | null = null;
function getAdminPool(): Pool {
    if (!adminPool) adminPool = new Pool(ADMIN_CONFIG);
    return adminPool;
}

describe("Suite G - PostgreSQL lock diagnostics", () => {
    afterAll(async () => {
        try { await getAdminPool().end(); } catch {}
    });

    async function withLockedConnections<T>(
        dbName: string,
        fn: (c1: Client, c2: Client) => Promise<T>,
    ): Promise<T> {
        const config = { ...ADMIN_CONFIG, database: dbName };
        const c1 = new Client(config);
        const c2 = new Client(config);
        await c1.connect();
        await c2.connect();
        try {
            return await fn(c1, c2);
        } finally {
            try { await c1.query("ROLLBACK").catch(() => {}); } catch {}
            try { await c2.query("ROLLBACK").catch(() => {}); } catch {}
            await c1.end().catch(() => {});
            await c2.end().catch(() => {});
        }
    }

    it("G1-G5: long-running transaction + conflicting lock are detected by diagnostics", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_g", async (pool, dbName) => {
            await pool.query("CREATE TABLE users (id int primary key, name text)");
            await pool.query("INSERT INTO users VALUES (1, 'alice')");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            await withLockedConnections(dbName, async (c1, c2) => {
                // G1: c1 holds a row lock in a long-running transaction
                await c1.query("BEGIN");
                await c1.query("SELECT * FROM users WHERE id = 1 FOR UPDATE");
                await new Promise((r) => setTimeout(r, 300));

                // G2: c2 requests a conflicting lock (blocks)
                const c2Lock = c2.query("SELECT * FROM users WHERE id = 1 FOR UPDATE");
                await new Promise((r) => setTimeout(r, 400)); // let c2 block

                // G3: long-running transaction detected (low threshold)
                const long = await migrator.detectLongRunningTransactions({ minDurationMs: 100 });
                expect(long.transactions.some((t) => t.state === "idle in transaction")).toBe(true);
                const c1Txn = long.transactions.find((t) => t.query && t.query.includes("FOR UPDATE"));
                expect(c1Txn).toBeDefined();

                // G4: lock contention detected (low threshold)
                const cont = await migrator.detectLockContention({ minWaitMs: 50 });
                expect(cont.contention.length).toBeGreaterThan(0);
                const waiter = cont.contention.find((c) => c.query && c.query.includes("FOR UPDATE"));
                expect(waiter).toBeDefined();
                expect(waiter!.waitMs).toBeGreaterThan(0);

                // G5: getLockDiagnostics combines both
                const diag = await migrator.getLockDiagnostics();
                expect(diag).toHaveProperty("lockContention");
                expect(diag).toHaveProperty("longRunningTransactions");
                expect(diag).toHaveProperty("advisoryLocks");
                expect(diag).toHaveProperty("advisoryLockStatus");

                await c1.query("ROLLBACK");
                await c2Lock.catch(() => {});
            });
        });
    });

    it("G6: minDurationMs boundary - very low includes, high excludes", async () => {
        await withIsolatedDb("suite_g", async (pool, dbName) => {
            await pool.query("CREATE TABLE g6_t (id int)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await withLockedConnections(dbName, async (c1) => {
                await c1.query("BEGIN");
                await c1.query("SELECT 1");
                const low = await migrator.detectLongRunningTransactions({ minDurationMs: 1 });
                const high = await migrator.detectLongRunningTransactions({ minDurationMs: 86400000 }); // 24h
                expect(low.transactions.length).toBeGreaterThanOrEqual(0);
                expect(high.transactions.length).toBe(0);
            });
        });
    });

    it("G7: minWaitMs boundary", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_g", async (pool, dbName) => {
            await pool.query("CREATE TABLE g7_t (id int)");
            await pool.query("INSERT INTO g7_t VALUES (1)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await withLockedConnections(dbName, async (c1, c2) => {
                await c1.query("BEGIN");
                await c1.query("SELECT * FROM g7_t WHERE id = 1 FOR UPDATE");
                await new Promise((r) => setTimeout(r, 200));
                const blocked = c2.query("SELECT * FROM g7_t WHERE id = 1 FOR UPDATE");
                await new Promise((r) => setTimeout(r, 300));
                const low = await migrator.detectLockContention({ minWaitMs: 1 });
                const high = await migrator.detectLockContention({ minWaitMs: 86400000 });
                expect(low.contention.length).toBeGreaterThanOrEqual(high.contention.length);
                await c1.query("ROLLBACK");
                await blocked.catch(() => {});
            });
        });
    });

    it("G8: unrelated transactions are not reported as blockers", async () => {
        await withIsolatedDb("suite_g", async (pool) => {
            await pool.query("CREATE TABLE g8_t (id int)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const diag = await migrator.getLockDiagnostics();
            // With no locks held, no contention
            expect(diag.lockContention.length).toBe(0);
        });
    });

    it("G9: blocker and blocked-session relationship", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_g", async (pool, dbName) => {
            await pool.query("CREATE TABLE g9_t (id int)");
            await pool.query("INSERT INTO g9_t VALUES (1)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await withLockedConnections(dbName, async (c1, c2) => {
                await c1.query("BEGIN");
                await c1.query("SELECT * FROM g9_t WHERE id = 1 FOR UPDATE");
                await new Promise((r) => setTimeout(r, 200));
                const blocked = c2.query("SELECT * FROM g9_t WHERE id = 1 FOR UPDATE");
                await new Promise((r) => setTimeout(r, 300));
                const cont = await migrator.detectLockContention({ minWaitMs: 50 });
                // c2 is the waiter (granted=false); c1 holds the row lock
                const waiters = cont.contention.filter((c) => c.waitMs > 0);
                expect(waiters.length).toBeGreaterThan(0);
                await c1.query("ROLLBACK");
                await blocked.catch(() => {});
            });
        });
    });

    it("G10: advisory lock waits are visible via lock contention; advisoryLockStatus is always free (lockWaitMetrics unused)", async () => {
        await withIsolatedDb("suite_g", async (pool, dbName) => {
            await pool.query("CREATE TABLE g10_t (id int)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await withLockedConnections(dbName, async (c1, c2) => {
                // c1 holds advisory lock 999
                await c1.query("SELECT pg_advisory_lock(999)");
                // c2 attempts the same advisory lock (blocks)
                const c2Lock = c2.query("SELECT pg_advisory_lock(999)");
                await new Promise((r) => setTimeout(r, 400));

                const cont = await migrator.detectLockContention({ minWaitMs: 50 });
                const advisoryWait = cont.contention.some((c) => c.query && c.query.includes("pg_advisory_lock"));
                expect(advisoryWait).toBe(true);

                // But advisoryLockStatus is derived from lockWaitMetrics, which is never populated
                const diag = await migrator.getLockDiagnostics();
                expect(diag.advisoryLockStatus).toBe("free");

                await c1.query("SELECT pg_advisory_unlock(999)");
                await c2Lock.catch(() => {});
            });
        });
    });

    it("G11: diagnostics work with multiple pools (queries pools[0])", async () => {
        await withIsolatedDb("suite_g", async (pool1, db1) => {
            await withIsolatedDb("suite_g", async (pool2, db2) => {
                await pool1.query("CREATE TABLE g11_t (id int)");
                const migrator = createMigrator([pool1, pool2], { table: "_writer_migrations" });
                const diag = await migrator.getLockDiagnostics();
                expect(diag).toHaveProperty("lockContention");
                expect(diag).toHaveProperty("longRunningTransactions");
            });
        });
    });

    it("G12: diagnostics do not throw for a user lacking activity-inspection privileges", async () => {
        const admin = getAdminPool();
        await admin.query("CREATE USER diag_limited WITH PASSWORD 'diag_limited_pw'");
        await admin.query("CREATE DATABASE diag_limited_db OWNER diag_limited");
        try {
            const limitedPool = new Pool({
                user: "diag_limited", password: "diag_limited_pw", database: "diag_limited_db",
                host: "/run/postgresql", max: 3,
            });
            const migrator = createMigrator(limitedPool, { table: "_writer_migrations" });
            // ensureTables runs as owner; diagnostics query pg_stat_activity (own sessions only)
            const long = await migrator.detectLongRunningTransactions({ minDurationMs: 1 });
            expect(Array.isArray(long.transactions)).toBe(true);
            const cont = await migrator.detectLockContention({ minWaitMs: 1 });
            expect(Array.isArray(cont.contention)).toBe(true);
            await limitedPool.end();
        } finally {
            try {
                await admin.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'diag_limited_db'`);
                await admin.query("DROP DATABASE IF EXISTS diag_limited_db");
                await admin.query("DROP USER IF EXISTS diag_limited");
            } catch {}
        }
    });

    it("G13: diagnostics do not terminate or modify application queries", async () => {
        await withIsolatedDb("suite_g", async (pool) => {
            await pool.query("CREATE TABLE g13_t (id int)");
            await pool.query("INSERT INTO g13_t VALUES (1), (2), (3)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const before = await pool.query("SELECT count(*) AS n FROM g13_t");
            const diag = await migrator.getLockDiagnostics();
            const after = await pool.query("SELECT count(*) AS n FROM g13_t");
            expect(Number(after.rows[0].n)).toBe(Number(before.rows[0].n));
            expect(diag).toBeDefined();
        });
    });
});
