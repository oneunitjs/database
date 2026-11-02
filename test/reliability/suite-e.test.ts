import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

/**
 * Suite E reflects the CORRECTED rollback behavior after Stage 1:
 * - rollback_risk is TEXT, so "potentially-destructive" fits (no VARCHAR overflow).
 * - rollback() executes down SQL + registry DELETE + history insert in ONE transaction,
 *   so a destructive change is never committed while being reported as a failed rollback.
 */
const ROW_COUNT = 5000;

async function seedUsers(pool: Pool, count = ROW_COUNT): Promise<void> {
    await pool.query(`
        CREATE TABLE users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    `);
    await pool.query(`
        INSERT INTO users (id, name, email)
        SELECT gen_random_uuid(), 'user_' || g, 'user_' || g || '@example.com'
        FROM generate_series(1, $1) g;
    `, [count]);
}

async function seedOrders(pool: Pool): Promise<void> {
    await pool.query(`
        CREATE TABLE orders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id),
            amount NUMERIC NOT NULL,
            status TEXT NOT NULL DEFAULT 'open'
        );
    `);
    await pool.query(`
        INSERT INTO orders (id, user_id, amount, status)
        SELECT gen_random_uuid(), id, (random()*1000)::numeric(10,2), 'open'
        FROM users;
    `);
}

async function columnExists(pool: Pool, table: string, column: string): Promise<boolean> {
    const { rows } = await pool.query(
        `SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2`,
        [table, column],
    );
    return rows.length > 0;
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

async function rowSnapshot(pool: Pool): Promise<Array<{ id: string; name: string; email: string }>> {
    const { rows } = await pool.query("SELECT id, name, email FROM users ORDER BY name");
    return rows as Array<{ id: string; name: string; email: string }>;
}

describe("Suite E - Populated-table rollback safety (corrected behavior)", () => {
    it("E1-E4: add column, populate, rollback succeeds atomically; original data preserved", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            await seedOrders(pool);
            const before = await rowSnapshot(pool);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const addNick = {
                id: "e_add_nick",
                up: "ALTER TABLE users ADD COLUMN nick TEXT",
                down: "ALTER TABLE users DROP COLUMN nick",
            };

            await migrator.run({ migrations: [addNick] });
            expect(await columnExists(pool, "users", "nick")).toBe(true);
            await pool.query("UPDATE users SET nick = name || '_nick'");
            const { rows: populated } = await pool.query("SELECT count(*) AS n FROM users WHERE nick IS NOT NULL");
            expect(Number(populated[0].n)).toBe(ROW_COUNT);

            // DROP COLUMN down is "potentially-destructive"; previously crashed on
            // VARCHAR(20) overflow and committed the DDL before recording history.
            // Now it executes atomically and succeeds.
            const rolledBack = await migrator.rollback({
                migrations: [addNick],
                steps: 1,
                allowDestructive: true,
            });
            expect(rolledBack).toEqual(["e_add_nick"]);

            // Atomic success: column gone, registry row removed, history recorded.
            expect(await columnExists(pool, "users", "nick")).toBe(false);
            expect(await registryRow(pool, "e_add_nick")).toBeNull();
            const hist = await historyRows(pool, "e_add_nick");
            expect(hist.some((r) => r.status === "rolled_back")).toBe(true);
            expect(hist.some((r) => r.rollback_risk === "potentially-destructive")).toBe(true);

            // Other columns' data preserved; nick's data is gone with the column.
            const after = await rowSnapshot(pool);
            expect(after.map((r) => ({ name: r.name, email: r.email }))).toEqual(
                before.map((r) => ({ name: r.name, email: r.email })),
            );
            const ordersStillThere = await pool.query("SELECT count(*) AS n FROM orders");
            expect(Number(ordersStillThere.rows[0].n)).toBe(ROW_COUNT);
        });
    });

    it("E5: explicit down() SQL rollback succeeds atomically", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = {
                id: "e_explicit_down",
                up: "ALTER TABLE users ADD COLUMN phone TEXT",
                down: "ALTER TABLE users DROP COLUMN phone",
            };
            await migrator.run({ migrations: [m] });
            expect(await columnExists(pool, "users", "phone")).toBe(true);

            const rolledBack = await migrator.rollback({ migrations: [m], steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["e_explicit_down"]);
            expect(await columnExists(pool, "users", "phone")).toBe(false);
            expect(await registryRow(pool, "e_explicit_down")).toBeNull();
            const hist = await historyRows(pool, "e_explicit_down");
            expect(hist.some((r) => r.status === "rolled_back")).toBe(true);
        });
    });

    it("E6: automatic rollback generation for ADD COLUMN", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = { id: "e_auto_down", up: "ALTER TABLE users ADD COLUMN nickname TEXT" };
            await migrator.run({ migrations: [m] });
            expect(await columnExists(pool, "users", "nickname")).toBe(true);

            // Auto-generated DROP COLUMN is potentially-destructive -> requires allowDestructive.
            await expect(migrator.rollback({ migrations: [m], steps: 1 })).rejects.toThrow(/allowDestructive/);
            // With allowDestructive, succeeds atomically (no VARCHAR overflow).
            const rolledBack = await migrator.rollback({ migrations: [m], steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["e_auto_down"]);
            expect(await columnExists(pool, "users", "nickname")).toBe(false);
            expect(await registryRow(pool, "e_auto_down")).toBeNull();
        });
    });

    it("E7: irreversible operations cannot be rolled back without an explicit down()", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = { id: "e_irreversible", up: "DELETE FROM users" };
            await migrator.run({ migrations: [m] });
            const { rows } = await pool.query("SELECT count(*) AS n FROM users");
            expect(Number(rows[0].n)).toBe(0);
            await expect(migrator.rollback({ migrations: [m], steps: 1, allowDestructive: true })).rejects.toThrow(
                /no down\(\) migration provided and none could be auto-generated/,
            );
        });
    });

    it("E8: DROP COLUMN rollback restores the column but not the dropped values (safe down risk)", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await migrator.run({ migrations: [{ id: "e_dc_add", up: "ALTER TABLE users ADD COLUMN ssn TEXT" }] });
            await pool.query("UPDATE users SET ssn = 'secret_' || id::text");
            const ssnBefore = await pool.query("SELECT ssn FROM users WHERE ssn LIKE 'secret_%'");
            expect(ssnBefore.rows.length).toBe(ROW_COUNT);

            const dropSsn = {
                id: "e_dc_drop",
                up: "ALTER TABLE users DROP COLUMN ssn",
                down: "ALTER TABLE users ADD COLUMN ssn TEXT",
            };
            await migrator.run({ migrations: [dropSsn] });
            expect(await columnExists(pool, "users", "ssn")).toBe(false);

            const rolledBack = await migrator.rollback({ migrations: [dropSsn], steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["e_dc_drop"]);
            expect(await columnExists(pool, "users", "ssn")).toBe(true);
            const ssnAfter = await pool.query("SELECT ssn FROM users WHERE ssn LIKE 'secret_%'");
            expect(ssnAfter.rows.length).toBe(0);
        });
    });

    it("E9: table rename rollback succeeds", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const rename = {
                id: "e_rename",
                up: "ALTER TABLE users RENAME TO users_archived",
                down: "ALTER TABLE users_archived RENAME TO users",
            };
            await migrator.run({ migrations: [rename] });
            expect(await tableExists(pool, "public.users_archived")).toBe(true);
            expect(await tableExists(pool, "public.users")).toBe(false);

            const rolledBack = await migrator.rollback({ migrations: [rename], steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["e_rename"]);
            expect(await tableExists(pool, "public.users")).toBe(true);
            expect(await tableExists(pool, "public.users_archived")).toBe(false);
        });
    });

    async function tableExists(pool: Pool, name: string): Promise<boolean> {
        const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
        return rows[0]?.to_regclass !== null;
    }

    it("E10: data transformation rollback restores values with allowDestructive", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = {
                id: "e_xform",
                up: "UPDATE users SET name = name || '_changed'",
                down: "UPDATE users SET name = replace(name, '_changed', '')",
            };
            await migrator.run({ migrations: [m] });
            const { rows: changed } = await pool.query("SELECT count(*) AS n FROM users WHERE name LIKE '%_changed'");
            expect(Number(changed[0].n)).toBe(ROW_COUNT);

            await expect(migrator.rollback({ migrations: [m], steps: 1 })).rejects.toThrow(/allowDestructive/);
            const rolledBack = await migrator.rollback({ migrations: [m], steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["e_xform"]);
            const { rows: restored } = await pool.query("SELECT count(*) AS n FROM users WHERE name LIKE '%_changed'");
            expect(Number(restored[0].n)).toBe(0);
        });
    });

    it("E11: rollback dependencies - a later applied migration blocks rollback of an earlier one", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const a = { id: "e_dep_a", up: "ALTER TABLE users ADD COLUMN col_a TEXT" };
            const b = { id: "e_dep_b", up: "ALTER TABLE users ADD COLUMN col_b TEXT", dependsOn: ["e_dep_a"] };
            await migrator.run({ migrations: [a, b] });

            const deps = await migrator.checkRollbackDependencies({ migrations: [a, b], steps: 2 });
            expect(deps.safe).toBe(false);
            expect(deps.blocking.some((x) => x.migrationId === "e_dep_a")).toBe(true);

            const deps2 = await migrator.checkRollbackDependencies({ migrations: [a], steps: 1 });
            expect(deps2.safe).toBe(true);
        });
    });

    it("E12: destructive rollback requires allowDestructive", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = { id: "e_prot", up: "ALTER TABLE users ADD COLUMN protected_col TEXT" };
            await migrator.run({ migrations: [m] });
            await expect(migrator.rollback({ migrations: [m], steps: 1 })).rejects.toThrow(/allowDestructive/);
            expect(await columnExists(pool, "users", "protected_col")).toBe(true);
        });
    });

    it("E13: rollback dryRun is safe and leaves no database changes", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = { id: "e_dry", up: "ALTER TABLE users ADD COLUMN dry_col TEXT" };
            await migrator.run({ migrations: [m] });
            expect(await columnExists(pool, "users", "dry_col")).toBe(true);

            // dryRun executes down() inside a rolled-back transaction; no persistent change.
            const rolledBack = await migrator.rollback({ migrations: [m], steps: 1, dryRun: true, allowDestructive: true });
            expect(rolledBack).toEqual(["e_dry"]);
            expect(await columnExists(pool, "users", "dry_col")).toBe(true);
        });
    });

    it("E14: validateRollback does not leave database changes behind", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = {
                id: "e_val",
                up: "ALTER TABLE users ADD COLUMN val_col TEXT",
                down: "ALTER TABLE users DROP COLUMN val_col",
            };
            await migrator.run({ migrations: [m] });

            const validation = await migrator.validateRollback({ migrations: [m], steps: 1 });
            expect(validation.valid).toBe(true);
            expect(validation.results[0].valid).toBe(true);
            expect(await columnExists(pool, "users", "val_col")).toBe(true);
        });
    });

    it("E15: rollback failure leaves migration history and registry consistent", async () => {
        await withIsolatedDb("suite_e", async (pool) => {
            await seedUsers(pool);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m2 = {
                id: "e_faildown2",
                up: "ALTER TABLE users ADD COLUMN fail_col2 TEXT",
                down: "ALTER TABLE nonexistent_table_xyz DROP COLUMN c",
            };
            await migrator.run({ migrations: [m2] });
            expect(await columnExists(pool, "users", "fail_col2")).toBe(true);

            await expect(migrator.rollback({ migrations: [m2], steps: 1, allowDestructive: true })).rejects.toThrow();
            const status = await migrator.status({ migrations: [m2] });
            expect(status[0].status).toBe("applied");
            expect(await columnExists(pool, "users", "fail_col2")).toBe(true);
        });
    });
});
