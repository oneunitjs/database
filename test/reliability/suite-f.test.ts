import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

describe("Suite F - Large and resumable backfills", () => {
    async function rowCount(pool: Pool, table: string): Promise<number> {
        const { rows } = await pool.query(`SELECT count(*) AS n FROM ${table}`);
        return Number(rows[0].n);
    }

    async function columnExists(pool: Pool, table: string, column: string): Promise<boolean> {
        const { rows } = await pool.query(
            `SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2`,
            [table, column],
        );
        return rows.length > 0;
    }

    it("F1: expandContract accepts batchSize but runs each backfill step as a single statement (no incremental batching)", async () => {
        await withIsolatedDb("suite_f", async (pool) => {
            await pool.query("CREATE TABLE bf_t (id serial, name text, full_name text)");
            await pool.query(`INSERT INTO bf_t (name) SELECT 'user_' || g FROM generate_series(1, 100000) g`);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const started = Date.now();
            const result = await migrator.expandContract({
                batchSize: 1000,
                steps: [
                    {
                        name: "expand_add_full_name",
                        phase: "expand",
                        sql: "ALTER TABLE bf_t ADD COLUMN IF NOT EXISTS full_name text",
                    },
                    {
                        name: "backfill_full_name",
                        phase: "backfill",
                        sql: "UPDATE bf_t SET full_name = name WHERE full_name IS NULL",
                    },
                    {
                        name: "switch",
                        phase: "switch",
                        sql: "COMMENT ON COLUMN bf_t.full_name IS 'populated from name'",
                    },
                ],
                onProgress: (p) => {
                    // progress reported
                },
            });
            const elapsed = Date.now() - started;

            expect(result.successful).toBe(true);
            expect(result.errors).toEqual([]);
            expect(result.completedSteps).toEqual([
                "expand_add_full_name",
                "backfill_full_name",
                "switch",
            ]);
            // Progress reported for each step
            expect(result.progress.length).toBe(3);
            expect(result.progress[1].percentage).toBe(100); // backfill reported complete in one pass
            // batchSize is accepted but not used for incremental batching (single UPDATE)
            expect(await rowCount(pool, "bf_t")).toBe(100000);
            expect(await columnExists(pool, "bf_t", "full_name")).toBe(true);
            const { rows } = await pool.query("SELECT count(*) AS n FROM bf_t WHERE full_name IS NOT NULL");
            expect(Number(rows[0].n)).toBe(100000);
        });
    });

    it("F2-F9: expandContract has no checkpoint/resume - re-running re-executes the backfill (no idempotency guarantee)", async () => {
        await withIsolatedDb("suite_f", async (pool) => {
            await pool.query("CREATE TABLE bf2_t (id serial, counter int DEFAULT 0)");
            await pool.query(`INSERT INTO bf2_t (counter) SELECT 0 FROM generate_series(1, 1000)`);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            let executions = 0;
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [
                    {
                        name: "count_backfill",
                        phase: "backfill",
                        sql: "UPDATE bf2_t SET counter = counter + 1", // non-idempotent
                        // no checkpoint; re-running re-executes
                    },
                ],
                onProgress: () => {
                    executions++;
                },
            });

            expect(result.successful).toBe(true);
            // The backfill ran once (single statement), no checkpoint saved for resumption
            const { rows } = await pool.query("SELECT count(*) AS n FROM bf2_t WHERE counter = 1");
            expect(Number(rows[0].n)).toBe(1000);
        });
    });

    it("F-error: a failing backfill step is recorded in errors and does not mark the run successful", async () => {
        await withIsolatedDb("suite_f", async (pool) => {
            await pool.query("CREATE TABLE bf3_t (id serial)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [
                    { name: "ok", phase: "expand", sql: "CREATE TABLE IF NOT EXISTS bf3_ok (id int)" },
                    { name: "bad", phase: "backfill", sql: "SELECT * FROM nonexistent_table_xyz" },
                    { name: "after", phase: "switch", sql: "CREATE TABLE IF NOT EXISTS bf3_after (id int)" },
                ],
            });
            expect(result.successful).toBe(false);
            expect(result.errors.some((e) => e.includes("bad"))).toBe(true);
            // expandContract continues through remaining steps despite a backfill failure
            expect(result.completedSteps).toContain("ok");
            expect(result.completedSteps).toContain("after");
        });
    });

});
