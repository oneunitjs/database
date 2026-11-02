import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb } from "./helpers.js";

describe("Suite H - Zero-downtime expand-contract workflow", () => {
    async function columnExists(pool: Pool, table: string, column: string): Promise<boolean> {
        const { rows } = await pool.query(
            `SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2`,
            [table, column],
        );
        return rows.length > 0;
    }

    it("H1-H7: expand, backfill, switch - old and new columns coexist; backfill populates from name", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'user_' || g FROM generate_series(1, 1000) g`);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [
                    // H1: expand - add full_name without removing name
                    { name: "expand_add_full_name", phase: "expand", sql: "ALTER TABLE users ADD COLUMN full_name text" },
                    // H4: backfill - populate full_name from name
                    { name: "backfill_full_name", phase: "backfill", sql: "UPDATE users SET full_name = name WHERE full_name IS NULL" },
                    // H8: switch (logical read switch - represented as a DB step here)
                    { name: "switch_reads_to_full_name", phase: "switch", sql: "COMMENT ON COLUMN users.full_name IS 'read source'" },
                ],
                onProgress: (p) => {
                    expect(p.percentage).toBeGreaterThanOrEqual(0);
                },
            });

            expect(result.successful).toBe(true);
            expect(result.completedSteps).toEqual([
                "expand_add_full_name",
                "backfill_full_name",
                "switch_reads_to_full_name",
            ]);
            expect(result.progress.length).toBe(3);

            // H2: old column still present; old queries work
            expect(await columnExists(pool, "users", "name")).toBe(true);
            const { rows: oldQuery } = await pool.query("SELECT name FROM users WHERE id = 1");
            expect(oldQuery[0].name).toBe("user_1");

            // H3/H7: new column populated; new queries work
            expect(await columnExists(pool, "users", "full_name")).toBe(true);
            const { rows: newQuery } = await pool.query("SELECT full_name FROM users WHERE id = 1");
            expect(newQuery[0].full_name).toBe("user_1");
            const { rows: allFilled } = await pool.query("SELECT count(*) AS n FROM users WHERE full_name IS NOT NULL");
            expect(Number(allFilled[0].n)).toBe(1000);
        });
    });

    it("H5-H6: inserts and updates during transition keep both columns consistent", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null, full_name text)");
            await pool.query("INSERT INTO users (name, full_name) VALUES ('a','a'), ('b','b')");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });

            // Ongoing app write during the transition
            await pool.query("INSERT INTO users (name, full_name) VALUES ('c','c')");
            await pool.query("UPDATE users SET name = 'a_changed' WHERE id = 1");

            // Backfill syncs full_name from name for rows where it's null or stale
            await migrator.expandContract({
                batchSize: 100,
                steps: [
                    { name: "sync", phase: "backfill", sql: "UPDATE users SET full_name = name WHERE full_name IS DISTINCT FROM name" },
                ],
            });
            const { rows } = await pool.query("SELECT id, name, full_name FROM users ORDER BY id");
            for (const row of rows) {
                expect(row.full_name).toBe(row.name);
            }
        });
    });

    it("H8-H9: old and new application versions coexist during transition", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null, full_name text)");
            await pool.query("INSERT INTO users (name, full_name) VALUES ('x','x')");
            // Both columns readable - old and new apps both work
            const oldApp = await pool.query("SELECT name FROM users WHERE id = 1");
            const newApp = await pool.query("SELECT full_name FROM users WHERE id = 1");
            expect(oldApp.rows[0].name).toBe("x");
            expect(newApp.rows[0].full_name).toBe("x");
        });
    });

    it("H10: expandContract has no prerequisite gating - a contract step executes even if placed early (limitation)", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null, full_name text)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            // No gating: expandContract runs steps strictly in array order.
            // Placing contract before expand/backfill still executes without an expandContract-level guard.
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [
                    { name: "premature_contract", phase: "contract", sql: "ALTER TABLE users DROP COLUMN full_name" },
                ],
            });
            expect(result.successful).toBe(true);
            expect(await columnExists(pool, "users", "full_name")).toBe(false);
        });
    });

    it("H11-H12: expandContract does not enforce approval; contract runs after prerequisites when ordered correctly", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null, full_name text)");
            await pool.query("INSERT INTO users (name, full_name) VALUES ('m','m')");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            // expandContract itself performs no approval check (approval is a separate API)
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [
                    { name: "expand", phase: "expand", sql: "ALTER TABLE users ADD COLUMN IF NOT EXISTS extra text" },
                    { name: "contract_extra", phase: "contract", sql: "ALTER TABLE users DROP COLUMN IF EXISTS extra" },
                ],
            });
            expect(result.successful).toBe(true);
        });
    });

    it("H13: final schema and data correctness after full expand-contract", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text not null)");
            await pool.query(`INSERT INTO users (name) SELECT 'u_' || g FROM generate_series(1, 500) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await migrator.expandContract({
                batchSize: 100,
                steps: [
                    { name: "expand", phase: "expand", sql: "ALTER TABLE users ADD COLUMN full_name text" },
                    { name: "backfill", phase: "backfill", sql: "UPDATE users SET full_name = name" },
                    { name: "contract_name", phase: "contract", sql: "ALTER TABLE users DROP COLUMN name" },
                ],
            });
            expect(await columnExists(pool, "users", "full_name")).toBe(true);
            expect(await columnExists(pool, "users", "name")).toBe(false);
            const { rows } = await pool.query("SELECT count(*) AS n FROM users WHERE full_name IS NOT NULL");
            expect(Number(rows[0].n)).toBe(500);
        });
    });

    it("H-add-nullable: add a nullable column", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE t (id serial primary key)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await migrator.expandContract({
                batchSize: 100,
                steps: [{ name: "add_nullable", phase: "expand", sql: "ALTER TABLE t ADD COLUMN note text" }],
            });
            expect(await columnExists(pool, "t", "note")).toBe(true);
        });
    });

    it("H-add-nonnull-default: add a non-null column with a safe default", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE t (id serial primary key, status text not null default 'new')");
            await pool.query("INSERT INTO t (status) VALUES ('old')");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            // Safe default: existing rows get the default, new column non-null
            await migrator.expandContract({
                batchSize: 100,
                steps: [{ name: "add_tier", phase: "expand", sql: "ALTER TABLE t ADD COLUMN tier text not null default 'standard'" }],
            });
            expect(await columnExists(pool, "t", "tier")).toBe(true);
            const { rows } = await pool.query("SELECT tier FROM t WHERE id = 1");
            expect(rows[0].tier).toBe("standard");
        });
    });

    it("H-index-concurrently: CREATE INDEX CONCURRENTLY cannot run inside expandContract's transaction", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE t (id serial primary key, name text)");
            await pool.query(`INSERT INTO t (name) SELECT 'n' || g FROM generate_series(1, 5000) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [{ name: "idx", phase: "expand", sql: "CREATE INDEX CONCURRENTLY idx_t_name ON t (name)" }],
            });
            // CONCURRENTLY inside a transaction fails
            expect(result.successful).toBe(false);
            expect(result.errors.length).toBeGreaterThan(0);
        });
    });

    it("H-fk-staged-validation: add foreign key with NOT VALID then validate", async () => {
        await withIsolatedDb("suite_h", async (pool) => {
            await pool.query("CREATE TABLE users (id serial primary key, name text)");
            await pool.query("CREATE TABLE orders (id serial primary key, user_id int)");
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const result = await migrator.expandContract({
                batchSize: 100,
                steps: [
                    { name: "fk_not_valid", phase: "expand", sql: "ALTER TABLE orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID" },
                    { name: "validate_fk", phase: "switch", sql: "ALTER TABLE orders VALIDATE CONSTRAINT orders_user_fk" },
                ],
            });
            expect(result.successful).toBe(true);
        });
    });
});
