import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

async function tableExists(pool: Pool, name: string): Promise<boolean> {
    const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
    return rows[0]?.to_regclass !== null;
}

async function columnExists(pool: Pool, table: string, column: string): Promise<boolean> {
    const { rows } = await pool.query(
        `SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2`,
        [table, column],
    );
    return rows.length > 0;
}

async function historyTableDef(pool: Pool): Promise<string | null> {
    const { rows } = await pool.query(
        `SELECT data_type FROM information_schema.columns WHERE table_name='_writer_migration_history' AND column_name='rollback_risk'`,
    );
    return rows[0]?.data_type ?? null;
}

async function registryRow(pool: Pool, id: string) {
    const { rows } = await pool.query(`SELECT id, status FROM _writer_migrations WHERE id = $1`, [id]);
    return rows[0] ?? null;
}

async function historyRows(pool: Pool, id: string) {
    const { rows } = await pool.query(
        `SELECT migration_id, status, rollback_risk FROM _writer_migration_history WHERE migration_id = $1`,
        [id],
    );
    return rows;
}

describe("Stage 1 regression — rollback atomicity and history schema", () => {
    it("potentially-destructive down rollback succeeds atomically: DDL + registry + history all consistent (H2/C1)", async () => {
        await withIsolatedDb("stage1", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 1000) g`);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // Ensure history column is TEXT (schema upgrade path)
            await migrator.status({ migrations: [] });

            const def = await historyTableDef(pool);
            expect(def).toBe("text");

            const applied = await migrator.run({
                migrations: [{ id: "s1_add_ssn", up: "ALTER TABLE users ADD COLUMN ssn text" }],
            });
            expect(applied).toEqual(["s1_add_ssn"]);
            await pool.query("UPDATE users SET ssn = 's_' || id::text");
            expect(await columnExists(pool, "users", "ssn")).toBe(true);

            // DROP COLUMN down is "potentially-destructive" -> previously crashed on VARCHAR(20) overflow.
            const rolledBack = await migrator.rollback({
                migrations: [{ id: "s1_add_ssn", up: "ALTER TABLE users ADD COLUMN ssn text", down: "ALTER TABLE users DROP COLUMN ssn" }],
                steps: 1,
                allowDestructive: true,
            });
            expect(rolledBack).toEqual(["s1_add_ssn"]);

            // Atomic success: column gone, registry row removed, history recorded.
            expect(await columnExists(pool, "users", "ssn")).toBe(false);
            expect(await registryRow(pool, "s1_add_ssn")).toBeNull();
            const hist = await historyRows(pool, "s1_add_ssn");
            expect(hist.some((r) => r.status === "rolled_back")).toBe(true);
            expect(hist.some((r) => r.rollback_risk === "potentially-destructive")).toBe(true);
        });
    });

    it("history schema upgrade widens rollback_risk from VARCHAR(20) to TEXT on existing installations (H2)", async () => {
        await withIsolatedDb("stage1", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // Manually pre-create a history table with the OLD narrow column to simulate an existing installation.
            await pool.query(`
                CREATE TABLE IF NOT EXISTS _writer_migration_history (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    migration_id VARCHAR(255) NOT NULL,
                    target_database VARCHAR(255),
                    checksum VARCHAR(64) NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMPTZ,
                    duration_ms INTEGER NOT NULL DEFAULT 0,
                    status VARCHAR(20) NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    mode VARCHAR(20) NOT NULL DEFAULT 'transactional',
                    error_message TEXT,
                    rollback_risk VARCHAR(20),
                    application_version TEXT,
                    runner_id TEXT NOT NULL DEFAULT '',
                    host TEXT
                );
            `);

            // Before the upgrade, inserting "potentially-destructive" (23 chars) into VARCHAR(20) fails.
            await expect(
                pool.query(`INSERT INTO _writer_migration_history (migration_id, status, checksum, rollback_risk, runner_id) VALUES ($1,'rolled_back','deadbeef','potentially-destructive','t')`, ["s1"]),
            ).rejects.toThrow(/character varying\(20\)/);

            // ensureTables (run via any mutating call) widens the column idempotently.
            await migrator.status({ migrations: [] });
            const def = await historyTableDef(pool);
            expect(def).toBe("text");

            // Now the long risk value fits.
            await pool.query(`INSERT INTO _writer_migration_history (migration_id, status, checksum, rollback_risk, runner_id) VALUES ($1,'rolled_back','deadbeef','potentially-destructive','t')`, ["s1"]);
        });
    });

    it("successful rollback leaves registry and history consistent (no half-state) (C1)", async () => {
        await withIsolatedDb("stage1", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 500) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            await migrator.run({ migrations: [{ id: "s1_phone", up: "ALTER TABLE users ADD COLUMN phone text" }] });
            await pool.query("UPDATE users SET phone = 'p_' || id::text");
            expect(await columnExists(pool, "users", "phone")).toBe(true);

            await migrator.rollback({
                migrations: [{ id: "s1_phone", up: "ALTER TABLE users ADD COLUMN phone text", down: "ALTER TABLE users DROP COLUMN phone" }],
                steps: 1,
                allowDestructive: true,
            });

            // No half-state: column dropped AND registry row gone AND history recorded.
            expect(await columnExists(pool, "users", "phone")).toBe(false);
            expect(await registryRow(pool, "s1_phone")).toBeNull();
            const hist = await historyRows(pool, "s1_phone");
            expect(hist.length).toBeGreaterThan(0);
            expect(hist.some((r) => r.status === "rolled_back")).toBe(true);

            // Data in other columns preserved.
            const { rows } = await pool.query("SELECT count(*) AS n FROM users WHERE name LIKE 'u_%'");
            expect(Number(rows[0].n)).toBe(500);
        });
    });
});
