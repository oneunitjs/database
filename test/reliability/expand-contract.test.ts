import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

async function columnExists(pool: Pool, table: string, column: string): Promise<boolean> {
    const { rows } = await pool.query(
        `SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2`,
        [table, column],
    );
    return rows.length > 0;
}

describe("Stage 2 — Expand-contract state machine (enforced phases)", () => {
    it("contract is blocked when a prerequisite phase is not completed", async () => {
        await withIsolatedDb("ec", async (pool, dbName) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 100) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // contract declared before switch: contract requires switch (declared earlier),
            // which is not completed -> contract blocked, switch executes.
            const result = await migrator.expandContract({
                migrationId: "ec_contract_first",
                enforcePhases: true,
                steps: [
                    { name: "contract_drop_name", phase: "contract", sql: "ALTER TABLE users DROP COLUMN name", risk: "safe", requiresManualIntervention: true },
                    { name: "switch", phase: "switch", sql: "COMMENT ON COLUMN users.full_name IS 'x'", risk: "safe", requiresManualIntervention: false },
                ],
            });
            expect(result.successful).toBe(false);
            expect(result.errors.some((e) => e.includes("prerequisite"))).toBe(true);
            expect(await columnExists(pool, "users", "name")).toBe(true);
        });
    });

    it("full expand->backfill->verify->switch->contract chain completes with approval", async () => {
        await withIsolatedDb("ec", async (pool, dbName) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 100) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // Pre-approve the destructive contract step.
            await migrator.approveMigration("ec_full_chain", { targetDatabase: dbName });
            const result = await migrator.expandContract({
                migrationId: "ec_full_chain",
                enforcePhases: true,
                compatibility: { confirmed: true },
                steps: [
                    { name: "expand_add_full_name", phase: "expand", sql: "ALTER TABLE users ADD COLUMN full_name text", risk: "safe", requiresManualIntervention: false },
                    { name: "backfill_full_name", phase: "backfill", sql: "UPDATE users SET full_name = name", risk: "safe", requiresManualIntervention: false },
                    { name: "verify", phase: "verify", sql: "SELECT count(*) AS c FROM users WHERE full_name IS NOT NULL", risk: "safe", requiresManualIntervention: false },
                    { name: "switch", phase: "switch", sql: "COMMENT ON COLUMN users.full_name IS 'read source'", risk: "safe", requiresManualIntervention: false },
                    { name: "contract_drop_name", phase: "contract", sql: "ALTER TABLE users DROP COLUMN name", risk: "potentially-destructive", requiresApproval: true, requiresManualIntervention: true },
                ],
            });
            expect(result.successful).toBe(true);
            expect(result.completedSteps).toEqual([
                "expand_add_full_name",
                "backfill_full_name",
                "verify",
                "switch",
                "contract_drop_name",
            ]);
            expect(await columnExists(pool, "users", "full_name")).toBe(true);
            expect(await columnExists(pool, "users", "name")).toBe(false);
            const { rows } = await pool.query("SELECT count(*) AS n FROM users WHERE full_name IS NOT NULL");
            expect(Number(rows[0].n)).toBe(100);

            // Phase states persisted.
            const states = await pool.query(`SELECT phase, status FROM _writer_migration_phases ORDER BY phase`);
            expect(states.rows.some((s) => s.phase === "expand" && s.status === "completed")).toBe(true);
            expect(states.rows.some((s) => s.phase === "contract" && s.status === "completed")).toBe(true);
        });
    });

    it("backfill cannot start before expand", async () => {
        await withIsolatedDb("ec", async (pool, dbName) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 50) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // backfill declared before expand: backfill requires expand (declared earlier) not completed.
            const result = await migrator.expandContract({
                migrationId: "ec_bf_first",
                enforcePhases: true,
                steps: [
                    { name: "bf", phase: "backfill", sql: "UPDATE users SET name = upper(name)", risk: "safe", requiresManualIntervention: false },
                    { name: "expand_add_col", phase: "expand", sql: "ALTER TABLE users ADD COLUMN extra text", risk: "safe", requiresManualIntervention: false },
                ],
            });
            expect(result.successful).toBe(false);
            expect(result.errors.some((e) => e.includes("prerequisite"))).toBe(true);
            // expand still executed (no prerequisite), backfill did not.
            expect(await columnExists(pool, "users", "extra")).toBe(true);
        });
    });

    it("contract requires approval for potentially-destructive steps", async () => {
        await withIsolatedDb("ec", async (pool, dbName) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null, full_name text)");
            await pool.query(`INSERT INTO users (name, full_name) SELECT 'u_' || g, 'u_' || g FROM generate_series(1, 50) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // Potentially-destructive contract with no approval recorded -> blocked.
            const r = await migrator.expandContract({
                migrationId: "ec_no_approval",
                enforcePhases: true,
                compatibility: { confirmed: true },
                steps: [{ name: "drop_name", phase: "contract", sql: "ALTER TABLE users DROP COLUMN name", risk: "potentially-destructive", requiresApproval: true, requiresManualIntervention: true }],
            });
            expect(r.successful).toBe(false);
            expect(r.errors.some((e) => e.includes("approval"))).toBe(true);
            expect(await columnExists(pool, "users", "name")).toBe(true);

            // With approval recorded, the same contract executes.
            await migrator.approveMigration("ec_no_approval", { targetDatabase: dbName });
            const r2 = await migrator.expandContract({
                migrationId: "ec_no_approval",
                enforcePhases: true,
                compatibility: { confirmed: true },
                steps: [{ name: "drop_name", phase: "contract", sql: "ALTER TABLE users DROP COLUMN name", risk: "potentially-destructive", requiresApproval: true, requiresManualIntervention: true }],
            });
            expect(r2.successful).toBe(true);
            expect(await columnExists(pool, "users", "name")).toBe(false);
        });
    });

    it("nonTransactional steps (e.g. CREATE INDEX CONCURRENTLY) run outside a transaction", async () => {
        await withIsolatedDb("ec", async (pool, dbName) => {
            await pool.query("CREATE TABLE t (id serial primary key, name text)");
            await pool.query(`INSERT INTO t (name) SELECT 'n' || g FROM generate_series(1, 5000) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            const result = await migrator.expandContract({
                migrationId: "ec_idx",
                enforcePhases: false,
                steps: [{ name: "idx", phase: "expand", sql: "CREATE INDEX CONCURRENTLY idx_t_name ON t (name)", risk: "safe", requiresManualIntervention: false, nonTransactional: true }],
            });
            expect(result.successful).toBe(true);
            expect(await columnExists(pool, "t", "name")).toBe(true);
        });
    });

    it("without enforcePhases, steps run in array order (backward compatible)", async () => {
        await withIsolatedDb("ec", async (pool, dbName) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 50) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            const result = await migrator.expandContract({
                migrationId: "ec_order",
                steps: [
                    { name: "contract_first", phase: "contract", sql: "ALTER TABLE users ADD COLUMN added_col text", risk: "safe", requiresManualIntervention: false },
                    { name: "expand_second", phase: "expand", sql: "ALTER TABLE users ADD COLUMN another_col text", risk: "safe", requiresManualIntervention: false },
                ],
            });
            expect(result.successful).toBe(true);
            expect(await columnExists(pool, "users", "added_col")).toBe(true);
            expect(await columnExists(pool, "users", "another_col")).toBe(true);
        });
    });
});
