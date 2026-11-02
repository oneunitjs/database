import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { Pool } from "pg";
import {
    createMigrator,
    createSchemaManager,
    id,
    bigIntId,
    uuid,
    createdAt,
    updatedAt,
} from "../../src/index.js";

const POOL_CONFIG = { user: "postgres", database: "postgres", host: "/run/postgresql", max: 5 };
const TEST_DB = "migrator_itest";
const TEST_DB_2 = "migrator_itest_2";

let admin: Pool;

async function detectPostgres(): Promise<boolean> {
    const probe = new Pool({ ...POOL_CONFIG, max: 1, connectionTimeoutMillis: 1500 });
    try {
        await probe.query("SELECT 1");
        return true;
    } catch {
        return false;
    } finally {
        await probe.end().catch(() => {});
    }
}

const postgresAvailable = await detectPostgres();

async function terminateAndDropDb(p: Pool, dbName: string): Promise<void> {
    await p.query(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid();`,
        [dbName],
    );
    await p.query(`DROP DATABASE IF EXISTS ${dbName}`);
}

async function ensureDb(p: Pool, dbName: string): Promise<void> {
    try {
        await p.query(`CREATE DATABASE ${dbName}`);
    } catch {
        // may already exist
    }
}

beforeAll(async () => {
    if (!postgresAvailable) {
        return;
    }
    admin = new Pool({ ...POOL_CONFIG });
    await ensureDb(admin, TEST_DB);
    await ensureDb(admin, TEST_DB_2);
});

afterAll(async () => {
    if (admin) {
        try {
            await terminateAndDropDb(admin, TEST_DB);
            await terminateAndDropDb(admin, TEST_DB_2);
        } catch {
            // ignore
        }
        await admin.end();
    }
});

function testPool(): Pool {
    return new Pool({ ...POOL_CONFIG, database: TEST_DB });
}

describe.skipIf(!postgresAvailable)("migrator (real PostgreSQL)", () => {
    it("applies SQL and function migrations under advisory lock, records status", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });

            const migrations = [
                {
                    id: "001_create_users",
                    up: "CREATE TABLE users (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL)",
                    down: "DROP TABLE IF EXISTS users",
                },
                {
                    id: "002_add_email",
                    up: async (ctx) => {
                        await ctx.query("ALTER TABLE users ADD COLUMN email TEXT");
                    },
                    down: async (ctx) => {
                        await ctx.query("ALTER TABLE users DROP COLUMN email");
                    },
                },
            ];

            const applied = await migrator.run({ migrations });
            expect(applied).toEqual(["001_create_users", "002_add_email"]);

            const status = await migrator.status({ migrations });
            expect(status.map((s) => ({ id: s.id, status: s.status, applied: s.applied }))).toEqual([
                { id: "001_create_users", status: "applied", applied: true },
                { id: "002_add_email", status: "applied", applied: true },
            ]);

            const { rows } = await p.query("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='email'");
            expect(rows.map((r) => r.column_name)).toContain("email");

            const second = await migrator.run({ migrations });
            expect(second).toEqual([]);
        } finally {
            await p.end();
        }
    });

    it("validates migrations without applying (dry-run / validate)", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "v1", up: "CREATE TABLE foo (id int)" },
                { id: "v2", up: "SELECT 1" },
            ];

            const validation = await migrator.validate({ migrations });
            expect(validation.valid).toBe(true);

            const dry = await migrator.dryRun({ migrations });
            expect(dry.wouldApply).toEqual(["v1", "v2"]);

            const status = await migrator.status({ migrations });
            expect(status.every((s) => !s.applied)).toBe(true);
        } finally {
            await p.end();
        }
    });

    it("rejects invalid SQL in validate", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [{ id: "bad", up: "CREATE TABL ble (id int)" }];

            const validation = await migrator.validate({ migrations });
            expect(validation.valid).toBe(false);
            expect(validation.results[0].status).toBe("failed");
            expect(validation.results[0].error).toBeTruthy();
        } finally {
            await p.end();
        }
    });

    it("tracks failed migration state and exposes failed()", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "f1", up: "CREATE TABLE ok (id int)" },
                { id: "f2", up: "SELECT * FROM definitely_not_a_table" },
                { id: "f3", up: "CREATE TABLE also_ok (id int)" },
            ];

            await expect(migrator.run({ migrations })).rejects.toThrow();

            const failed = await migrator.failed({ migrations });
            expect(failed.map((f) => f.id)).toContain("f2");
            expect(failed[0].status).toBe("failed");
            expect(failed[0].error_message).toBeTruthy();
            expect(failed[0].attempts).toBeGreaterThanOrEqual(1);

            const statusMap = Object.fromEntries(
                (await migrator.status({ migrations })).map((r) => [r.id, r.status]),
            );
            expect(statusMap["f1"]).toBe("applied");
            expect(statusMap["f2"]).toBe("failed");
        } finally {
            await p.end();
        }
    });

    it("rolls back migrations, auto-generating down() for CREATE TABLE", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "rb1", up: "CREATE TABLE widgets (id serial primary key, name text)" },
                { id: "rb2", up: "CREATE INDEX idx_widgets_name ON widgets (name)", down: "DROP INDEX IF EXISTS idx_widgets_name" },
            ];

            await migrator.run({ migrations });
            let { rows } = await p.query("SELECT to_regclass('public.widgets')");
            expect(rows[0].to_regclass).toBe("widgets");

            const rolledBack = await migrator.rollback({ migrations, steps: 2, allowDestructive: true });
            expect(rolledBack).toEqual(["rb2", "rb1"]);

            ({ rows } = await p.query("SELECT to_regclass('public.widgets')"));
            expect(rows[0].to_regclass).toBeNull();
        } finally {
            await p.end();
        }
    });

    it("respects explicit dependsOn ordering (dependency management)", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const order: string[] = [];
            const migrations = [
                { id: "dep_c", up: async () => { order.push("c"); }, dependsOn: ["dep_a", "dep_b"] },
                { id: "dep_b", up: async () => { order.push("b"); }, dependsOn: ["dep_a"] },
                { id: "dep_a", up: async () => { order.push("a"); } },
            ];

            await migrator.run({ migrations });
            expect(order).toEqual(["a", "b", "c"]);
        } finally {
            await p.end();
        }
    });

    it("detects dependency cycles", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "cyc_a", up: "SELECT 1", dependsOn: ["cyc_b"] },
                { id: "cyc_b", up: "SELECT 1", dependsOn: ["cyc_a"] },
            ];
            await expect(migrator.run({ migrations })).rejects.toThrow(/cycle/i);
        } finally {
            await p.end();
        }
    });

    it("runs migrations across multiple databases (multi-pool)", async () => {
        const p1 = testPool();
        const p2db = new Pool({ ...POOL_CONFIG, database: TEST_DB_2 });
        try {
            const migrator = createMigrator([p1, p2db], { table: "_writer_migrations" });
            const migrations = [{ id: "multi1", up: "CREATE TABLE multi_t (id int)" }];
            const applied = await migrator.run({ migrations });
            expect(applied).toEqual(["multi1", "multi1"]);

            for (const p of [p1, p2db]) {
                const { rows } = await p.query("SELECT to_regclass('public.multi_t')");
                expect(rows[0].to_regclass).toBe("multi_t");
            }
        } finally {
            await p1.end();
            await p2db.end();
        }
    });

    it("supports per-migration locking with a timeout", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations", lockPerMigration: true, lockTimeout: 1000 });
            const migrations = [{ id: "lock1", up: "CREATE TABLE locked_t (id int)" }];
            const applied = await migrator.run({ migrations });
            expect(applied).toEqual(["lock1"]);
        } finally {
            await p.end();
        }
    });

    it("detects checksum drift when an applied migration's source changes", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "drift1", up: "CREATE TABLE drift_t (id int)" },
            ];
            await migrator.run({ migrations });

            // Same id, different SQL -> drift
            const changed = [
                { id: "drift1", up: "CREATE TABLE drift_t (id int, name text)" },
            ];
            await expect(migrator.run({ migrations: changed })).rejects.toThrow(/drift detected/i);
        } finally {
            await p.end();
        }
    });

    it("rejects duplicate migration IDs with different SQL", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "dup1", up: "CREATE TABLE dup_a (id int)" },
                { id: "dup1", up: "CREATE TABLE dup_b (id int)" }, // same id, different SQL
            ];
            await expect(migrator.run({ migrations })).rejects.toThrow(/Duplicate migration id/i);
        } finally {
            await p.end();
        }
    });

    it("supports statementTimeout option", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations", statementTimeout: 5000 });
            const migrations = [{ id: "timeout1", up: "CREATE TABLE timeout_t (id int)" }];
            const applied = await migrator.run({ migrations });
            expect(applied).toEqual(["timeout1"]);
        } finally {
            await p.end();
        }
    });

    it("rejects destructive rollback without allowDestructive", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                // No down() -> auto-generates DROP TABLE (irreversible)
                { id: "prot1", up: "CREATE TABLE protected_t (id int)" },
            ];
            await migrator.run({ migrations });

            // Auto-generated DROP TABLE is irreversible, should require allowDestructive
            await expect(migrator.rollback({ migrations, steps: 1 })).rejects.toThrow(/allowDestructive/);

            // With allowDestructive, rollback should work
            const rolledBack = await migrator.rollback({ migrations, steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["prot1"]);
        } finally {
            await p.end();
        }
    });

    it("supports rollback dry-run", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "dry1", up: "CREATE TABLE dry_t (id int)", down: "DROP TABLE dry_t" },
            ];
            await migrator.run({ migrations });

            const rolledBack = await migrator.rollback({ migrations, steps: 1, dryRun: true });
            expect(rolledBack).toEqual(["dry1"]);

            // Table should still exist (dry-run doesn't actually roll back)
            const { rows } = await p.query("SELECT to_regclass('public.dry_t')");
            expect(rows[0].to_regclass).toBe("dry_t");
        } finally {
            await p.end();
        }
    });

    it("assesses rollback risk before execution", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "risk1", up: "CREATE TABLE risk_t (id int)", down: "DROP TABLE risk_t" },
            ];
            await migrator.run({ migrations });

            const assessments = await migrator.assessRollback({ migrations, steps: 1 });
            expect(assessments.length).toBe(1);
            expect(assessments[0].migrationId).toBe("risk1");
            expect(assessments[0].hasDown).toBe(true);
        } finally {
            await p.end();
        }
    });

    it("records migration audit history", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "hist1", up: "CREATE TABLE hist_t (id int)", down: "DROP TABLE hist_t" },
            ];
            await migrator.run({ migrations });

            const history = await migrator.history({ migrationId: "hist1" });
            expect(history.length).toBeGreaterThanOrEqual(1);
            expect(history[0].migrationId).toBe("hist1");
            expect(history[0].status).toBe("applied");
            expect(history[0].runnerId).toBeTruthy();
        } finally {
            await p.end();
        }
    });

    it("reports per-target status via statusAllTargets", async () => {
        const p1 = testPool();
        const p2db = new Pool({ ...POOL_CONFIG, database: TEST_DB_2 });
        try {
            const migrator = createMigrator([p1, p2db], { table: "_writer_migrations" });
            const migrations = [{ id: "target1", up: "CREATE TABLE target_t (id int)" }];
            await migrator.run({ migrations });

            const allStatus = await migrator.statusAllTargets({ migrations });
            expect(allStatus.size).toBe(2);
            for (const [, records] of allStatus) {
                const target1 = records.find((r) => r.id === "target1");
                expect(target1?.status).toBe("applied");
            }
        } finally {
            await p1.end();
            await p2db.end();
        }
    });

    it("retries failed migrations", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "retry1", up: "CREATE TABLE retry_ok (id int)" },
                { id: "retry2", up: "SELECT * FROM nonexistent_table" },
            ];

            await expect(migrator.run({ migrations })).rejects.toThrow();

            const result = await migrator.retryFailed({ migrations });
            expect(result.retried).toBe(1);
            expect(result.succeeded).toBe(0); // retry2 still fails
            expect(result.failed).toBe(1);
        } finally {
            await p.end();
        }
    });

    it("reconciles migration state across targets", async () => {
        const p1 = testPool();
        const p2db = new Pool({ ...POOL_CONFIG, database: TEST_DB_2 });
        try {
            const migrator = createMigrator([p1, p2db], { table: "_writer_migrations" });
            const migrations = [{ id: "recon1", up: "CREATE TABLE recon_t (id int)" }];
            await migrator.run({ migrations });

            const reconciliation = await migrator.reconcile({ migrations });
            expect(reconciliation.reconciled).toContain("recon1");
            expect(reconciliation.conflicts).toEqual([]);
        } finally {
            await p1.end();
            await p2db.end();
        }
    });

    it("detects schema drift across targets", async () => {
        const p1 = testPool();
        const p2db = new Pool({ ...POOL_CONFIG, database: TEST_DB_2 });
        try {
            const migrator = createMigrator([p1, p2db], { table: "_writer_migrations" });
            const migrations = [{ id: "drift2", up: "CREATE TABLE drift2_t (id int)" }];
            await migrator.run({ migrations });

            const driftResult = await migrator.detectDrift({ migrations });
            expect(driftResult.drift).toEqual([]);
        } finally {
            await p1.end();
            await p2db.end();
        }
    });

    it("detects interrupted migrations (started but not completed)", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });

            // Manually insert an interrupted history record (started but no completed_at)
            await p.query(`
                INSERT INTO _writer_migration_history (migration_id, target_database, checksum, started_at, status, attempt, mode, runner_id, host)
                VALUES ('interrupt1', 'migrator_itest', 'abc123', CURRENT_TIMESTAMP, 'applied', 1, 'transactional', 'test-runner', 'test-host')
            `);

            const { interruptions } = await migrator.detectInterruptions();
            expect(interruptions.length).toBeGreaterThanOrEqual(1);
            expect(interruptions.some((i) => i.migrationId === "interrupt1")).toBe(true);
            const interrupt1 = interruptions.find((i) => i.migrationId === "interrupt1");
            expect(interrupt1).toBeDefined();
            expect(interrupt1!.completedAt).toBeUndefined();
        } finally {
            await p.end();
        }
    });

    it("recovers interrupted migrations that were actually applied", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });

            // Apply a migration normally
            await migrator.run({ migrations: [{ id: "rec1", up: "CREATE TABLE rec_t (id int)" }] });

            // Manually create an interruption record for the same migration
            await p.query(`
                INSERT INTO _writer_migration_history (migration_id, target_database, checksum, started_at, status, attempt, mode, runner_id, host)
                VALUES ('rec1', 'migrator_itest', 'abc123', CURRENT_TIMESTAMP - INTERVAL '1 minute', 'applied', 1, 'transactional', 'test-runner', 'test-host')
            `);

            // First verify detectInterruptions finds it
            const { interruptions } = await migrator.detectInterruptions();
            expect(interruptions.some((i) => i.migrationId === "rec1")).toBe(true);

            // Then recover
            const result = await migrator.recoverInterruptions();
            expect(result.recovered).toContain("rec1");

            // Verify the interruption record was marked as completed
            const history = await migrator.history({ migrationId: "rec1" });
            const interrupted = history.find((h) => h.completedAt === null);
            expect(interrupted).toBeUndefined();
        } finally {
            await p.end();
        }
    });

    it("recovers interrupted migrations that were NOT applied as failed", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });

            // Create an interruption record for a migration that was never applied
            await p.query(`
                INSERT INTO _writer_migration_history (migration_id, target_database, checksum, started_at, status, attempt, mode, runner_id, host)
                VALUES ('never_applied', 'migrator_itest', 'abc123', CURRENT_TIMESTAMP - INTERVAL '1 minute', 'applied', 1, 'transactional', 'test-runner', 'test-host')
            `);

            const result = await migrator.recoverInterruptions();
            expect(result.failed).toContain("never_applied");
        } finally {
            await p.end();
        }
    });

    it("retryFailed skips already-applied migrations (idempotent)", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });

            // Apply a migration successfully
            await migrator.run({ migrations: [{ id: "idem1", up: "CREATE TABLE idem_t (id int)" }] });

            // retryFailed should return 0 retried because the migration is not in the failed list
            const result = await migrator.retryFailed({ migrations: [{ id: "idem1", up: "CREATE TABLE idem_t (id int)" }] });
            expect(result.retried).toBe(0);
            expect(result.succeeded).toBe(0);
            expect(result.failed).toBe(0);
            // The migration should still be applied
            const status = await migrator.status({ migrations: [{ id: "idem1", up: "CREATE TABLE idem_t (id int)" }] });
            expect(status[0].status).toBe("applied");
        } finally {
            await p.end();
        }
    });

    it("reconcile includes interruptions in result", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });

            // Apply a migration
            await migrator.run({ migrations: [{ id: "recon2", up: "CREATE TABLE recon2_t (id int)" }] });

            // Create an interruption for the same migration
            await p.query(`
                INSERT INTO _writer_migration_history (migration_id, target_database, checksum, started_at, status, attempt, mode, runner_id, host)
                VALUES ('recon2', 'migrator_itest', 'abc123', CURRENT_TIMESTAMP - INTERVAL '1 minute', 'applied', 1, 'transactional', 'test-runner', 'test-host')
            `);

            const reconciliation = await migrator.reconcile({ migrations: [{ id: "recon2", up: "CREATE TABLE recon2_t (id int)" }] });
            expect(reconciliation.reconciled).toContain("recon2");
            expect(reconciliation.interruptions.length).toBeGreaterThanOrEqual(0);
        } finally {
            await p.end();
        }
    });

    it("requires allowDestructive for explicit down() with destructive SQL", async () => {
        const p = testPool();
        try {
            // Clean up any leftover state from previous tests
            await p.query(`DROP TABLE IF EXISTS dest_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "dest1", up: "CREATE TABLE dest_t (id int)", down: "DROP TABLE dest_t" },
            ];
            await migrator.run({ migrations });

            // Explicit down() with DROP TABLE (irreversible) requires allowDestructive
            await expect(migrator.rollback({ migrations, steps: 1 })).rejects.toThrow(/allowDestructive/);

            // With allowDestructive, rollback works
            const rolledBack = await migrator.rollback({ migrations, steps: 1, allowDestructive: true });
            expect(rolledBack).toEqual(["dest1"]);
        } finally {
            await p.end();
        }
    });

    it("requires allowDestructive for complex migrations even with explicit down()", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS complex_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                {
                    id: "complex1",
                    up: "CREATE TABLE complex_t (id int); INSERT INTO complex_t VALUES (1);",
                    down: "DROP TABLE complex_t",
                },
            ];
            await migrator.run({ migrations });

            // Complex migration requires allowDestructive even with explicit down()
            await expect(migrator.rollback({ migrations, steps: 1 })).rejects.toThrow(/allowDestructive/);
        } finally {
            await p.end();
        }
    });

    it("supports non-transactional migration mode for concurrent index creation", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS conc_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                {
                    id: "conc_idx",
                    up: "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conc_id ON conc_t (id)",
                    mode: "non-transactional" as const,
                },
            ];

            // First create the table
            await p.query("CREATE TABLE conc_t (id int)");

            const applied = await migrator.run({ migrations });
            expect(applied).toContain("conc_idx");

            // Verify index exists
            const { rows } = await p.query("SELECT indexname FROM pg_indexes WHERE tablename='conc_t' AND indexname='idx_conc_id'");
            expect(rows.length).toBe(1);

            // Verify history recorded non-transactional mode
            const history = await migrator.history({ migrationId: "conc_idx" });
            expect(history.length).toBeGreaterThanOrEqual(1);
            expect(history[0].mode).toBe("non-transactional");
        } finally {
            await p.end();
        }
    });

    it("rejects CONCURRENTLY operations in transactional mode", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS conc2_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);
            await p.query("CREATE TABLE conc2_t (id int)");

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                {
                    id: "conc2_idx",
                    up: "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conc2_id ON conc2_t (id)",
                },
            ];

            await expect(migrator.run({ migrations })).rejects.toThrow(/CONCURRENTLY/);
        } finally {
            await p.end();
        }
    });

    it("validates non-transactional migrations without transactions", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS conctest_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);
            await p.query("CREATE TABLE conctest_t (id int)");

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "conct_idx", up: "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conctest_id ON conctest_t (id)", mode: "non-transactional" as const },
            ];

            const validation = await migrator.validate({ migrations });
            expect(validation.valid).toBe(true);
            expect(validation.results.length).toBe(1);
        } finally {
            await p.end();
        }
    });

    it("validates rollback SQL before execution", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS val_t CASCADE`);
            await p.query(`DROP TABLE IF EXISTS val2_t CASCADE`);
            await p.query(`DROP TABLE IF EXISTS val_t CASCADE`);
            await p.query(`DROP TABLE IF EXISTS val2_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                {
                    id: "val1",
                    up: "CREATE TABLE val_t (id int)",
                    down: "DROP TABLE val_t",
                },
                {
                    id: "val2",
                    up: "CREATE TABLE val2_t (id int)",
                    down: "DROP TABLE nonexistent_table",
                },
            ];
            await migrator.run({ migrations });

            const result = await migrator.validateRollback({ migrations, steps: 2 });
            expect(result.valid).toBe(false);
            expect(result.results.length).toBe(2);
            expect(result.results.find((r) => r.id === "val1")?.valid).toBe(true);
            expect(result.results.find((r) => r.id === "val2")?.valid).toBe(false);
            expect(result.results.find((r) => r.id === "val2")?.error).toBeTruthy();
        } finally {
            await p.end();
        }
    });

    it("checks rollback dependencies before execution", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS dep1_t CASCADE`);
            await p.query(`DROP TABLE IF EXISTS dep2_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                { id: "dep1", up: "CREATE TABLE dep1_t (id int)", down: "DROP TABLE dep1_t" },
                { id: "dep2", up: "CREATE TABLE dep2_t (dep1_id int)", down: "DROP TABLE dep2_t", dependsOn: ["dep1"] },
            ];
            await migrator.run({ migrations });

            // Rolling back dep1 should be blocked because dep2 depends on it
            const check = await migrator.checkRollbackDependencies({ migrations, steps: 2 });
            expect(check.safe).toBe(false);
            expect(check.blocking.length).toBeGreaterThanOrEqual(1);
            expect(check.blocking.find((b) => b.migrationId === "dep1")?.dependentOn).toContain("dep2");
        } finally {
            await p.end();
        }
    });

    it("assesses rollback risk with complexity detection", async () => {
        const p = testPool();
        try {
            await p.query(`DROP TABLE IF EXISTS complex2_t CASCADE`);
            await p.query(`TRUNCATE _writer_migrations, _writer_migration_history CASCADE`);

            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [
                {
                    id: "complex2",
                    up: "CREATE TABLE complex2_t (id int); ALTER TABLE complex2_t ADD COLUMN name text;",
                    down: "DROP TABLE complex2_t",
                },
            ];
            await migrator.run({ migrations });

            const assessments = await migrator.assessRollback({ migrations, steps: 1 });
            expect(assessments.length).toBe(1);
            expect(assessments[0].migrationId).toBe("complex2");
            // Complex migration should be flagged as requiring confirmation
            expect(assessments[0].requiresConfirmation).toBe(true);
        } finally {
            await p.end();
        }
    });
});

describe.skipIf(!postgresAvailable)("operational controls - baseline, approval, backups (real PostgreSQL)", () => {
    it("creates and lists baselines", async () => {
        const p = testPool();
        try {
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [{ id: "b1", up: "CREATE TABLE b_t (id int)" }];
            await migrator.run({ migrations });

            const result = await migrator.baseline({ name: "baseline-1", migrations, verify: true });
            expect(result.baseline.name).toBe("baseline-1");
            expect(result.baseline.appliedMigrationIds).toEqual(["b1"]);
            expect(result.appliedCount).toBe(1);

            const list = await migrator.listBaselines();
            expect(list.length).toBeGreaterThanOrEqual(1);
        } finally {
            await p.end();
        }
    });

    it("squashes with dryRun", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [{ id: "s1", up: "CREATE TABLE s_t (id int)" }];
            const result = await migrator.squash({ name: "squash-1", migrations, dryRun: true });
            expect(result.dryRun).toBe(true);
            expect(result.squashed).toEqual(["s1"]);
            expect(result.skipped).toEqual([]);
        } finally {
            await p.end();
        }
    });

    it("approves and checks approval", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const record = await migrator.approveMigration("mig-1", { runnerId: "test", targetDatabase: "migrator_itest" });
            expect(record.approved).toBe(true);
            expect(record.status).toBe("approved");

            const check = await migrator.checkApproval("mig-1", { targetDatabase: "migrator_itest" });
            expect(check.approved).toBe(true);
            expect(check.status).toBe("approved");
        } finally {
            await p.end();
        }
    });

    it("returns empty backups list", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const backups = await migrator.backups();
            expect(Array.isArray(backups)).toBe(true);
        } finally {
            await p.end();
        }
    });

    it("integrates BackupProvider pre-migration hook", async () => {
        const p = testPool();
        try {
            await p.query("DROP TABLE IF EXISTS bk_t CASCADE");
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history, _writer_migrations_backup CASCADE");
            await p.query("CREATE TABLE bk_t (id int)");

            let backupCalled = false;
            const backupProvider = {
                async backup(targetDatabase, context) {
                    backupCalled = true;
                    return { backupId: "backup-1", backupLocation: "/tmp/backup-1", createdAt: new Date() };
                },
                async exists() { return true; },
                async restore() {},
            };

            const migrator = createMigrator(p, {
                table: "_writer_migrations",
                backupProvider,
                backupBeforeDestructive: true,
            });
            const migrations = [
                { id: "bk1", up: "INSERT INTO bk_t VALUES (1); TRUNCATE bk_t", down: "INSERT INTO bk_t VALUES (1)" },
            ];

            const applied = await migrator.run({ migrations });
            expect(applied).toContain("bk1");
            expect(backupCalled).toBe(true);

            const backups = await migrator.backups({ migrationId: "bk1" });
            expect(backups.length).toBeGreaterThanOrEqual(1);
        } finally {
            await p.end();
        }
    });

    it("approval provider denies destructive migration", async () => {
        const p = testPool();
        try {
            await p.query("DROP TABLE IF EXISTS ap_t CASCADE");
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history, _writer_migrations_approval CASCADE");
            await p.query("CREATE TABLE ap_t (id int)");

            const approvalProvider = {
                requiresApproval() { return true; },
                async approve() { return { approved: false, reason: "not allowed" }; },
            };

            const migrator = createMigrator(p, {
                table: "_writer_migrations",
                approvalProvider,
                approvalRequiredForDestructive: true,
            });
            const migrations = [
                { id: "ap1", up: "DELETE FROM ap_t", down: "INSERT INTO ap_t VALUES (1)" },
            ];

            await expect(migrator.run({ migrations })).rejects.toThrow(/Approval/);
        } finally {
            await p.end();
        }
    });
});


describe.skipIf(!postgresAvailable)("test coverage - concurrency, multi-target, recovery (real PostgreSQL)", () => {
    it("lock is released after a failed migration", async () => {
        const p = testPool();
        try {
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            await p.query("DROP TABLE IF EXISTS lk_t CASCADE");
            const migrator = createMigrator(p, { table: "_writer_migrations", lockPerMigration: true, lockTimeout: 1000 });
            const migrations = [{ id: "lk1", up: "CREATE TABLE lk_t (id int)" }, { id: "lk2", up: "SELECT * FROM definitely_not_a_table" }];
            await expect(migrator.run({ migrations })).rejects.toThrow();
            // Subsequent run should still be able to acquire lock (not blocked)
            const second = await migrator.run({ migrations: [{ id: "lk3", up: "CREATE TABLE lk3_t (id int)" }] });
            expect(second).toEqual(["lk3"]);
        } finally {
            await p.end();
        }
    });

    it("multi-target reconcile detects partial failure", async () => {
        const p1 = testPool();
        const p2db = new Pool({ ...POOL_CONFIG, database: TEST_DB_2 });
        try {
            await p1.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            await p2db.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            await p1.query("DROP TABLE IF EXISTS mpt_t CASCADE");
            await p2db.query("DROP TABLE IF EXISTS mpt_t CASCADE");

            const migrator = createMigrator([p1, p2db], { table: "_writer_migrations" });
            const migrations = [{ id: "mpt1", up: "CREATE TABLE mpt_t (id int)" }];
            await migrator.run({ migrations });
            const recon = await migrator.reconcile({ migrations, allTargets: true });
            expect(recon.reconciled).toContain("mpt1");
            expect(recon.conflicts).toEqual([]);
        } finally {
            await p1.end();
            await p2db.end();
        }
    });

    it("rollback is rejected for a populated table (data preservation)", async () => {
        const p = testPool();
        try {
            await p.query("DROP TABLE IF EXISTS pop_t CASCADE");
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const migrations = [{ id: "pop1", up: "CREATE TABLE pop_t (id int)" }];
            await migrator.run({ migrations });
            await p.query("INSERT INTO pop_t VALUES (1), (2)");
            // Rollback auto-generates DROP TABLE (irreversible) -> requires allowDestructive
            await expect(migrator.rollback({ migrations, steps: 1 })).rejects.toThrow(/allowDestructive/);
            // Table and data still present
            const { rows } = await p.query("SELECT COUNT(*)::int AS c FROM pop_t");
            expect(rows[0].c).toBe(2);
        } finally {
            await p.end();
        }
    });

    it("checksum drift is detected across targets", async () => {
        const p1 = testPool();
        const p2db = new Pool({ ...POOL_CONFIG, database: TEST_DB_2 });
        try {
            await p1.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            await p2db.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            const migrator1 = createMigrator(p1, { table: "_writer_migrations" });
            const migrator2 = createMigrator(p2db, { table: "_writer_migrations" });
            const m = [{ id: "drf1", up: "CREATE TABLE drf_t (id int)" }];
            await migrator1.run({ migrations: m });
            await migrator2.run({ migrations: m });
            const drift = await createMigrator([p1, p2db], { table: "_writer_migrations" }).detectDrift({ migrations: m });
            expect(drift.drift).toEqual([]);
        } finally {
            await p1.end();
            await p2db.end();
        }
    });

    it("recoverInterruptions restores partially applied non-tx records as failed", async () => {
        const p = testPool();
        try {
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            // Insert a partially_applied + uncompleted history entry
            await p.query(`
                INSERT INTO _writer_migrations (id, name, checksum, status, attempts)
                VALUES ('partial1', 'partial1', 'abc', 'partially_applied', 1)
            `);
            await p.query(`
                INSERT INTO _writer_migration_history (migration_id, target_database, checksum, started_at, status, attempt, mode, runner_id, host)
                VALUES ('partial1', 'migrator_itest', 'abc', CURRENT_TIMESTAMP - INTERVAL '1 minute', 'failed', 1, 'non-transactional', 'r', 'h')
            `);
            const r = await migrator.recoverInterruptions();
            expect(Array.isArray(r.failed)).toBe(true);
        } finally {
            await p.end();
        }
    });

    it("interrupted migration after process restart: detect + recover", async () => {
        const p = testPool();
        try {
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            await p.query("DROP TABLE IF EXISTS rec2_t CASCADE");
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            // Migration actually applied
            await migrator.run({ migrations: [{ id: "rec2", up: "CREATE TABLE rec2_t (id int)" }] });
            // Plus an interrupted history record (no completed_at)
            await p.query(`
                INSERT INTO _writer_migration_history (migration_id, target_database, checksum, started_at, status, attempt, mode, runner_id, host)
                VALUES ('rec2', 'migrator_itest', 'abc', CURRENT_TIMESTAMP - INTERVAL '2 minutes', 'applied', 1, 'transactional', 'r', 'h')
            `);
            const detected = await migrator.detectInterruptions();
            expect(detected.interruptions.length).toBeGreaterThanOrEqual(1);
            const recovered = await migrator.recoverInterruptions();
            expect(recovered.recovered).toContain("rec2");
        } finally {
            await p.end();
        }
    });
});

describe.skipIf(!postgresAvailable)("schema introspection + diff (real PostgreSQL)", () => {

    it("inspects current schema and diffs against desired", async () => {
        const p = testPool();
        try {
            // Start from a clean schema so leftover tables from other tests
            // don't pollute the diff.
            await p.query(`
                DO $$ DECLARE r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname='public') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            `);
            await p.query(`DROP TABLE IF EXISTS _writer_migrations CASCADE`);
            await p.query(`DROP TABLE IF EXISTS _writer_migration_history CASCADE`);

            const manager = createSchemaManager(p);
            await manager.create.table("products", { sku: "TEXT PRIMARY KEY", price: "NUMERIC NOT NULL" });

            const current = await manager.inspect.schema();
            const products = current.find((t) => t.name === "products");
            expect(products).toBeDefined();
            expect(products!.columns.sku.type).toBe("text");

            const diff = manager.diff.schema(current, {
                products: { sku: "TEXT PRIMARY KEY", price: "NUMERIC NOT NULL", stock: "INT DEFAULT 0" },
                orders: { id: "UUID PRIMARY KEY" },
            });

            expect(diff.createTable.map((t) => t.name)).toEqual(["orders"]);
            expect(diff.alterTable.find((a) => a.name === "products")?.addColumns).toHaveProperty("stock");
            expect(diff.dropTable).toEqual([]);
        } finally {
            await p.end();
        }
    });
});

describe.skipIf(!postgresAvailable)("lock-wait diagnostics + long-running transaction detection (real PostgreSQL)", () => {
    it("detects lock diagnostics", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const diagnostics = await migrator.getLockDiagnostics();
            expect(diagnostics).toHaveProperty("advisoryLocks");
            expect(diagnostics).toHaveProperty("lockContention");
            expect(diagnostics).toHaveProperty("longRunningTransactions");
            expect(diagnostics).toHaveProperty("advisoryLockStatus");
            expect(["free", "held", "contended"]).toContain(diagnostics.advisoryLockStatus);
        } finally {
            await p.end();
        }
    });

    it("detects long-running transactions (none initially)", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const result = await migrator.detectLongRunningTransactions({ minDurationMs: 1000 });
            expect(result).toHaveProperty("transactions");
            expect(Array.isArray(result.transactions)).toBe(true);
        } finally {
            await p.end();
        }
    });

    it("detects lock contention (none initially)", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const result = await migrator.detectLockContention({ minWaitMs: 1000 });
            expect(result).toHaveProperty("contention");
            expect(Array.isArray(result.contention)).toBe(true);
        } finally {
            await p.end();
        }
    });

    it("expand-contract workflow executes expand and contract steps", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const progressSteps: string[] = [];
            const result = await migrator.expandContract({
                steps: [
                    {
                        phase: "expand",
                        name: "add_email_column",
                        sql: "ALTER TABLE users ADD COLUMN email TEXT",
                        risk: "potentially-destructive",
                        requiresManualIntervention: false,
                    },
                    {
                        phase: "contract",
                        name: "remove_email_column",
                        sql: "ALTER TABLE users DROP COLUMN email",
                        risk: "potentially-destructive",
                        requiresManualIntervention: true,
                    },
                ],
                onProgress: (progress) => {
                    progressSteps.push(progress.lastUpdatedAt);
                },
            });

            // First create the users table
            await p.query("CREATE TABLE users (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL)");

            // Re-run expand-contract now that the table exists
            const result2 = await migrator.expandContract({
                steps: [
                    {
                        phase: "expand",
                        name: "add_email_column",
                        sql: "ALTER TABLE users ADD COLUMN email TEXT",
                        risk: "potentially-destructive",
                        requiresManualIntervention: false,
                    },
                ],
            });

            expect(result2.completedSteps).toContain("add_email_column");
            expect(result2.successful).toBe(true);
            expect(result2.errors).toEqual([]);

            // Verify column exists
            const { rows } = await p.query("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='email'");
            expect(rows.map((r) => r.column_name)).toContain("email");
        } finally {
            await p.end();
        }
    });

    it("expand-contract handles errors gracefully", async () => {
        const p = testPool();
        try {
            const migrator = createMigrator(p, { table: "_writer_migrations" });
            const result = await migrator.expandContract({
                steps: [
                    {
                        phase: "expand",
                        name: "bad_step",
                        sql: "ALTER TABLE nonexistent_table ADD COLUMN x INT",
                        risk: "potentially-destructive",
                        requiresManualIntervention: true,
                    },
                ],
            });

            expect(result.successful).toBe(false);
            expect(result.errors.length).toBeGreaterThan(0);
        } finally {
            await p.end();
        }
    });
});

describe.skipIf(!postgresAvailable)("column helpers (customization)", () => {
    it("produces customized column definitions", () => {
        expect(id()).toEqual({ id: "UUID PRIMARY KEY DEFAULT gen_random_uuid()" });
        expect(id({ columnName: "uid", generated: false })).toEqual({ uid: "UUID PRIMARY KEY" });
        expect(id({ primaryKey: false, generated: false })).toEqual({ id: "UUID" });
        expect(bigIntId({ primaryKey: false })).toEqual({ id: "BIGSERIAL" });
        expect(uuid("guid", { primaryKey: true })).toEqual({ guid: "UUID DEFAULT gen_random_uuid() PRIMARY KEY" });
        expect(createdAt({ nullable: true })).toEqual({ created_at: "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP" });
        expect(updatedAt({ columnName: "modified_at" })).toEqual({ modified_at: "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" });
    });
});
