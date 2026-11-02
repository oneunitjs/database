import { describe, it, expect } from "vitest";
import { Pool, Client } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

const ADMIN_CONFIG = { user: "postgres", database: "postgres", host: "/run/postgresql", max: 3 };

describe("Stage 3 regression — advisory lock timeout (H5)", () => {
    it("whole-run advisory lock acquisition is bounded by lockTimeout (not indefinite)", async () => {
        await withIsolatedDb("stage3", async (pool, dbName) => {
            const config = { ...ADMIN_CONFIG, database: dbName };
            // Hold the whole-run advisory lock externally on a separate connection.
            const holder = new Client(config);
            await holder.connect();
            await holder.query("SELECT pg_advisory_lock(727274)");

            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                lockKey: 727274,
                lockTimeout: 1500, // finite default-mode timeout
            });

            const start = Date.now();
            await expect(
                migrator.run({ migrations: [{ id: "s3_lock", up: "CREATE TABLE s3_t (id int)" }] }),
            ).rejects.toThrow(/Could not acquire advisory lock/);
            const elapsed = Date.now() - start;
            // Must be bounded by the deadline, not block indefinitely.
            expect(elapsed).toBeGreaterThanOrEqual(1000);
            expect(elapsed).toBeLessThan(10_000);

            // Release the lock; a subsequent run acquires it and proceeds.
            await holder.query("SELECT pg_advisory_unlock(727274)");
            await holder.end();
            await pool.query("DROP TABLE IF EXISTS s3_t");
            const applied2 = await migrator.run({
                migrations: [{ id: "s3_lock2", up: "CREATE TABLE s3_t (id int)" }],
            });
            expect(applied2).toContain("s3_lock2");
            const { rows } = await pool.query("SELECT to_regclass('public.s3_t') AS rc");
            expect(rows[0].rc).not.toBeNull();
        });
    });

    it("per-migration advisory lock timeout still works (regression)", async () => {
        await withIsolatedDb("stage3", async (pool, dbName) => {
            const config = { ...ADMIN_CONFIG, database: dbName };
            const holder = new Client(config);
            await holder.connect();
            await holder.query("SELECT pg_advisory_lock(727274)");

            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                lockKey: 727274,
                lockPerMigration: true,
                lockTimeout: 1000,
            });

            const start = Date.now();
            await expect(
                migrator.run({ migrations: [{ id: "s3_pm", up: "CREATE TABLE s3_pm_t (id int)" }] }),
            ).rejects.toThrow(/Could not acquire advisory lock/);
            const elapsed = Date.now() - start;
            expect(elapsed).toBeGreaterThanOrEqual(700);
            expect(elapsed).toBeLessThan(10_000);

            await holder.query("SELECT pg_advisory_unlock(727274)");
            await holder.end();
        });
    });
});
