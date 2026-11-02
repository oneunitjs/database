import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

async function tableExists(pool: Pool, name: string): Promise<boolean> {
    const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
    return rows[0]?.to_regclass !== null;
}

async function failedRows(pool: Pool): Promise<Array<{ id: string; status: string }>> {
    const { rows } = await pool.query(`SELECT id, status FROM _writer_migrations WHERE status = 'failed'`);
    return rows;
}

describe("Stage 2 regression — multi-target retry and execution semantics (H3/H4)", () => {
    it("retryFailed retries failed migrations on every configured target, not just pools[0] (H3)", async () => {
        await withIsolatedDb("stage2", async (poolA, dbA) => {
            await withIsolatedDb("stage2", async (poolB, dbB) => {
                const migrator = createMigrator([poolA, poolB], { table: "_writer_migrations" });
                const migrations = [
                    { id: "m_ok", up: "CREATE TABLE ok_t (id int)" },
                    { id: "m_bad", up: "SELECT * FROM nonexistent_table_xyz" },
                ];

                // Ensure registry tables exist on both targets before seeding failed rows.
                await migrator.run({ migrations: [] });

                // Pre-seed both targets with a failed m_bad row so retryFailed must address both.
                await poolA.query(
                    `INSERT INTO _writer_migrations (id, name, checksum, status, execution_ms, applied_at, failed_at, error_message, attempts)
                     VALUES ($1,'m_bad','deadbeef','failed',0,NULL,CURRENT_TIMESTAMP,'seed failure',1)`,
                    ["m_bad"],
                );
                await poolB.query(
                    `INSERT INTO _writer_migrations (id, name, checksum, status, execution_ms, applied_at, failed_at, error_message, attempts)
                     VALUES ($1,'m_bad','deadbeef','failed',0,NULL,CURRENT_TIMESTAMP,'seed failure',1)`,
                    ["m_bad"],
                );

                const result = await migrator.retryFailed({ migrations });
                // Both targets retried; m_bad still fails on both (nonexistent table).
                expect(result.retried).toBe(2);
                expect(result.succeeded).toBe(0);
                expect(result.failed).toBe(2);

                // m_ok was never applied (it wasn't in the failed set), but the retry
                // attempt touched both targets independently.
                const af = await failedRows(poolA);
                const bf = await failedRows(poolB);
                expect(af.some((r) => r.id === "m_bad")).toBe(true);
                expect(bf.some((r) => r.id === "m_bad")).toBe(true);
            });
        });
    });

    it("run() with targetFailurePolicy 'continue' attempts every target independently (H4)", async () => {
        await withIsolatedDb("stage2", async (poolA, dbA) => {
            await withIsolatedDb("stage2", async (poolB, dbB) => {
                const migrator = createMigrator([poolA, poolB], { table: "_writer_migrations" });
                const migrations = [
                    { id: "m_ok", up: "CREATE TABLE ok_t (id int)" },
                    { id: "m_bad", up: "SELECT * FROM nonexistent_table_xyz" },
                ];

                // Default "stop": first target's failure aborts before the second target is touched.
                await expect(migrator.run({ migrations })).rejects.toThrow();
                expect(await tableExists(poolA, "public.ok_t")).toBe(true);
                expect(await tableExists(poolB, "public.ok_t")).toBe(false);

                // "continue": every target is attempted; m_ok lands on both, m_bad fails on both.
                await expect(migrator.run({ migrations, targetFailurePolicy: "continue" })).rejects.toThrow(
                    /failed across targets/,
                );
                expect(await tableExists(poolA, "public.ok_t")).toBe(true);
                expect(await tableExists(poolB, "public.ok_t")).toBe(true);
            });
        });
    });
});
