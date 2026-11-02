import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb, waitForBackendDeath } from "./helpers.js";
import { spawnAndKill } from "./crash.js";

const MIGRATION_ID = "crash_migration";
const MIGRATION_SQL = `CREATE TABLE crash_t (id int, payload text); INSERT INTO crash_t VALUES (1, 'alpha');`;

async function ensureMigrationsTable(pool: Pool): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS _writer_migrations (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            checksum VARCHAR(64) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            execution_ms INTEGER NOT NULL DEFAULT 0,
            applied_at TIMESTAMPTZ,
            failed_at TIMESTAMPTZ,
            error_message TEXT,
            attempts INTEGER NOT NULL DEFAULT 0
        );
    `);
}

async function tableExists(pool: Pool, name: string): Promise<boolean> {
    const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
    return rows[0]?.to_regclass !== null;
}

async function migrationRow(pool: Pool, id: string) {
    try {
        const { rows } = await pool.query(
            `SELECT id, status, checksum FROM _writer_migrations WHERE id = $1`,
            [id],
        );
        return rows[0] ?? null;
    } catch {
        return null;
    }
}

describe("Suite A - Transactional crash recovery", () => {
    it("A1-A4: crash mid-transaction rolls back DDL and leaves registry unapplied", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_a", async (pool, dbName) => {
            await ensureMigrationsTable(pool);

            // A1/A2: run migration in separate process, kill mid-transaction (during pg_sleep)
            const result = await spawnAndKill(
                { db: dbName, action: "run-then-sleep", sql: MIGRATION_SQL, "sleep-sec": "6" },
                1500,
            );
            expect(result.killed).toBe(true);

            await waitForBackendDeath();

            // A3: PostgreSQL rolled back the incomplete transaction -> table must not exist
            expect(await tableExists(pool, "public.crash_t")).toBe(false);

            // A4: registry must not report the migration as applied (no row written before kill)
            expect(await migrationRow(pool, MIGRATION_ID)).toBeNull();
        });
    });

    it("A5-A7: restarting the runner recovers and reaches the correct final state", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_a", async (pool, dbName) => {
            await ensureMigrationsTable(pool);

            // Leave a crashed (rolled-back) transaction behind first
            await spawnAndKill(
                { db: dbName, action: "run-then-sleep", sql: MIGRATION_SQL, "sleep-sec": "6" },
                1500,
            );
            await waitForBackendDeath();
            expect(await tableExists(pool, "public.crash_t")).toBe(false);
            expect(await migrationRow(pool, MIGRATION_ID)).toBeNull();

            // A5/A6: restart the migrator; absence of a registry row means pending
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const applied = await migrator.run({ migrations: [{ id: MIGRATION_ID, up: MIGRATION_SQL }] });
            expect(applied).toContain(MIGRATION_ID);

            // A7: correct final state - table exists with the inserted data
            expect(await tableExists(pool, "public.crash_t")).toBe(true);
            const { rows } = await pool.query("SELECT count(*) AS n FROM crash_t");
            expect(Number(rows[0].n)).toBe(1);
            const { rows: r2 } = await pool.query("SELECT payload FROM crash_t WHERE id = 1");
            expect(r2[0].payload).toBe("alpha");

            const row = await migrationRow(pool, MIGRATION_ID);
            expect(row).not.toBeNull();
            expect(row.status).toBe("applied");
        });
    });

    it("A8-A10: process killed while holding the advisory lock; lock released and re-acquirable", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_a", async (pool, dbName) => {
            const lockKey = 727274;
            await ensureMigrationsTable(pool);

            // A8: acquire the migration advisory lock in a separate process, then kill it
            await spawnAndKill(
                { db: dbName, action: "hold-lock", "sleep-sec": "6", "lock-key": String(lockKey) },
                1500,
            );
            await waitForBackendDeath();

            // A9: advisory lock must be released after the database connection terminates
            const tryLock = await pool.query("SELECT pg_try_advisory_lock($1)", [lockKey]);
            expect(tryLock.rows[0]?.pg_try_advisory_lock).toBe(true);
            await pool.query("SELECT pg_advisory_unlock($1)", [lockKey]);

            // A10: another runner can acquire the lock and run a migration to completion
            const migrator = createMigrator(pool, { table: "_writer_migrations", lockKey });
            const applied = await migrator.run({
                migrations: [{ id: "lock_migration", up: "CREATE TABLE lock_t (id int)" }],
            });
            expect(applied).toEqual(["lock_migration"]);
            expect(await tableExists(pool, "public.lock_t")).toBe(true);
        });
    });

    it("A-advisory: whole-run lock is blocking with no timeout (default mode)", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_a", async (pool, dbName) => {
            await ensureMigrationsTable(pool);

            // Hold the lock externally with a long sleep, then let it die
            await spawnAndKill(
                { db: dbName, action: "hold-lock", "sleep-sec": "3", "lock-key": "727274" },
                500,
            );
            await waitForBackendDeath();

            // After the holder dies, a fresh migrator should acquire the released lock without hanging.
            const start = Date.now();
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await migrator.run({ migrations: [{ id: "a1", up: "CREATE TABLE t1 (id int)" }] });
            const elapsed = Date.now() - start;
            expect(elapsed).toBeLessThan(30_000);
            expect(await tableExists(pool, "public.t1")).toBe(true);
        });
    });
});
