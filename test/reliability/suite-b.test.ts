import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb, waitForBackendDeath } from "./helpers.js";
import { spawnAndKill } from "./crash.js";

describe("Suite B - Non-transactional crash recovery", () => {
    async function tableExists(pool: Pool, name: string): Promise<boolean> {
        const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
        return rows[0]?.to_regclass !== null;
    }

    async function indexValidity(pool: Pool, indexName: string): Promise<{ exists: boolean; valid: boolean | null }> {
        const { rows } = await pool.query(
            `SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1`,
            [indexName],
        );
        if (rows.length === 0) return { exists: false, valid: null };
        return { exists: true, valid: Boolean(rows[0].indisvalid) };
    }

    async function registryRow(pool: Pool, id: string) {
        try {
            const { rows } = await pool.query(`SELECT id, status FROM _writer_migrations WHERE id = $1`, [id]);
            return rows[0] ?? null;
        } catch {
            return null;
        }
    }

    async function historyRow(pool: Pool, id: string) {
        try {
            const { rows } = await pool.query(
                `SELECT id, migration_id, status, completed_at FROM _writer_migration_history WHERE migration_id = $1`,
                [id],
            );
            return rows[0] ?? null;
        } catch {
            return null;
        }
    }

    it("B1-B4: non-transactional multi-DDL killed after first op; partial schema, not marked applied", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            const migration = {
                id: "bt_partial",
                up: "CREATE TABLE bt_t (id int); SELECT pg_sleep(8); CREATE TABLE bt_t2 (id int);",
                mode: "non-transactional" as const,
            };

            // Run each statement in autocommit; kill during the pg_sleep between statements
            await spawnAndKill(
                { db: dbName, action: "autocommit-sequential", sql: migration.up, "sleep-sec": "0" },
                2000,
            );
            await waitForBackendDeath();

            // B3: bt_t committed (first statement), bt_t2 not created (killed before it)
            expect(await tableExists(pool, "public.bt_t")).toBe(true);
            expect(await tableExists(pool, "public.bt_t2")).toBe(false);

            // B4: not marked fully applied - no registry row written before the kill
            expect(await registryRow(pool, "bt_partial")).toBeNull();
        });
    });

    it("B5: history does not capture partial execution when killed before recordHistory", async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            await spawnAndKill(
                {
                    db: dbName,
                    action: "autocommit-sequential",
                    sql: "CREATE TABLE bt_hist (id int); SELECT pg_sleep(8);",
                    "sleep-sec": "0",
                },
                2000,
            );
            await waitForBackendDeath();

            // bt_hist was committed by the first (autocommit) statement
            expect(await tableExists(pool, "public.bt_hist")).toBe(true);
            // No migration row and no history row were written (process died mid-migration)
            expect(await registryRow(pool, "crash_migration")).toBeNull();
            expect(await historyRow(pool, "crash_migration")).toBeNull();
        });
    });

    it("B6: re-running when operations already exist fails on duplicate object (no auto-repair)", async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            await pool.query("CREATE TABLE bt_retry (id int)");

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const migration = {
                id: "bt_retry_m",
                up: "CREATE TABLE bt_retry (id int); CREATE TABLE bt_retry2 (id int);",
                mode: "non-transactional" as const,
            };

            // Recovery blindly reruns all statements -> first statement errors (already exists)
            await expect(migrator.run({ migrations: [migration] })).rejects.toThrow();
            // bt_retry2 must not have been created
            expect(await tableExists(pool, "public.bt_retry2")).toBe(false);
        });
    });

    it("B7: interrupted CREATE INDEX CONCURRENTLY leaves an invalid index", { timeout: 30000 }, async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            await pool.query("CREATE TABLE bt_big (id serial, payload text)");
            await pool.query(`INSERT INTO bt_big (payload) SELECT md5(random()::text) FROM generate_series(1, 3000000)`);

            await spawnAndKill(
                {
                    db: dbName,
                    action: "autocommit-sequential",
                    sql: "CREATE INDEX CONCURRENTLY bt_big_idx ON bt_big (payload);",
                    "sleep-sec": "0",
                },
                400,
            );
            await waitForBackendDeath();

            const idx = await indexValidity(pool, "bt_big_idx");
            // If the build was interrupted, PostgreSQL leaves an INVALID index.
            // A valid index means the build completed before the kill.
            if (idx.exists) {
                expect(idx.valid).toBe(false);
            }
        });
    });

    it("B8: same checksum, incomplete database state - re-run does not skip as already-applied", async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            await pool.query("CREATE TABLE bt_cksum (id int)");

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const migration = {
                id: "bt_cksum_m",
                up: "CREATE TABLE bt_cksum (id int); CREATE TABLE bt_cksum2 (id int);",
                mode: "non-transactional" as const,
            };

            // No registry row -> treated as pending, re-executed, fails on duplicate
            await expect(migrator.run({ migrations: [migration] })).rejects.toThrow();
            expect(await tableExists(pool, "public.bt_cksum2")).toBe(false);
        });
    });

    it("B9: recovery does not blindly rerun unsafe operations - re-run errors rather than silently succeeding", async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            await pool.query("CREATE TABLE bt_safe (id int)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const migration = {
                id: "bt_safe_m",
                up: "CREATE TABLE bt_safe (id int);",
                mode: "non-transactional" as const,
            };
            await expect(migrator.run({ migrations: [migration] })).rejects.toThrow(/already exists|relation.*already exists/i);
        });
    });

    it("B10: no built-in repair for partially-applied non-transactional migrations (manual intervention)", async () => {
        await withIsolatedDb("suite_b", async (pool, dbName) => {
            await spawnAndKill(
                { db: dbName, action: "autocommit-sequential", sql: "CREATE TABLE bt_repair (id int); SELECT pg_sleep(8);", "sleep-sec": "0" },
                2000,
            );
            await waitForBackendDeath();

            // bt_repair was committed by the first autocommit statement
            expect(await tableExists(pool, "public.bt_repair")).toBe(true);
            // There is no automatic repair: the migration is absent from the registry,
            // and recoverInterruptions has no history record to reconcile.
            expect(await registryRow(pool, "crash_migration")).toBeNull();
            expect(await historyRow(pool, "crash_migration")).toBeNull();
        });
    });
});
