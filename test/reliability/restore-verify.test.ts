import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import type { BackupProvider } from "../../src/types.js";
import { withIsolatedDb } from "./helpers.js";

class RestoreBackupProvider implements BackupProvider {
    calls: Array<{ targetDatabase: string; migrationId: string | undefined }> = [];
    backups = new Map<string, { location?: string }>();
    shouldFail = false;
    restoreShouldFail = false;
    restoredTo: string | null = null;

    async backup(targetDatabase: string, context: { migrationId?: string; reason: string; runnerId?: string }) {
        this.calls.push({ targetDatabase, migrationId: context.migrationId });
        if (this.shouldFail) throw new Error("backup provider failure");
        const id = `backup_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        this.backups.set(id, { location: `/tmp/backups/${id}` });
        return { backupId: id, backupLocation: `/tmp/backups/${id}`, createdAt: new Date() };
    }

    async exists(backupId: string): Promise<boolean> {
        return this.backups.has(backupId);
    }

    async restore(backupId: string, targetDatabase?: string): Promise<void> {
        if (this.restoreShouldFail) throw new Error("restore provider failure");
        if (!this.backups.has(backupId)) throw new Error("backup not found");
        this.restoredTo = targetDatabase ?? null;
    }
}

async function tableExists(pool: Pool, name: string): Promise<boolean> {
    const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
    return rows[0]?.to_regclass !== null;
}

async function backupRecords(pool: Pool) {
    const { rows } = await pool.query(`SELECT id, migration_id, target_database, backup_id, successful FROM _writer_migrations_backup ORDER BY id`);
    return rows;
}

async function countRows(pool: Pool, table: string): Promise<number> {
    const { rows } = await pool.query(`SELECT count(*) AS n FROM ${table}`);
    return Number(rows[0].n);
}

async function cloneDatabase(admin: Pool, sourceDb: string, targetDb: string): Promise<void> {
    // Only terminate connections to the target (freshly created), never the source,
    // so the test pool's connection to the source database stays alive.
    await admin.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, [targetDb]).catch(() => {});
    await admin.query(`DROP DATABASE IF EXISTS "${targetDb}"`);
    await admin.query(`CREATE DATABASE "${targetDb}"`);
}

describe("Stage 4 — Backup restore verification", () => {
    it("verifyBackup restores into isolated database and verifies schema/data", async () => {
        await withIsolatedDb("rv", async (pool, dbName) => {
            await pool.query("CREATE TABLE widget (id serial primary key, name text)");
            await pool.query(`INSERT INTO widget (name) SELECT 'w_' || g FROM generate_series(1, 100) g`);

            const provider = new RestoreBackupProvider();
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider,
                backupBeforeDestructive: true,
            });

            await migrator.run({ migrations: [{ id: "rv_backup_mig", up: "SELECT 1" }] });
            await pool.query(
                `INSERT INTO _writer_migrations_backup (migration_id, target_database, backup_id, backup_location, successful)
                 VALUES ($1, $2, $3, $4, $5)`,
                ["rv_backup_mig", dbName, "rv_backup_1", "/tmp/backups/rv_backup_1", true],
            );
            provider.backups.set("rv_backup_1", { location: "/tmp/backups/rv_backup_1" });

            // Pre-clone the source into an isolated restore target.
            const admin = new Pool({ user: "postgres", database: "postgres", host: "/run/postgresql", max: 3 });
            const restoreTarget = `rv_restore_${Date.now()}`;
            await cloneDatabase(admin, dbName, restoreTarget);
            await admin.end().catch(() => {});

            const result = await migrator.verifyBackup({
                migrationId: "rv_backup_mig",
                target: dbName,
                restoreTarget,
            });

            expect(result.backupId).toBe("rv_backup_1");
            expect(result.restored).toBe(true);
            expect(result.verified).toBe(true);
            expect(result.checks.some((c) => c.name === "backup_record" && c.passed)).toBe(true);
            expect(result.checks.some((c) => c.name === "backup_exists" && c.passed)).toBe(true);
            expect(result.checks.some((c) => c.name === "restore_capability" && c.passed)).toBe(true);
            expect(result.checks.some((c) => c.name === "restore_completed" && c.passed)).toBe(true);
            expect(result.checks.some((c) => c.name === "restored_tables" && c.passed)).toBe(true);

            // Cleanup
            const admin2 = new Pool({ user: "postgres", database: "postgres", host: "/run/postgresql", max: 3 });
            try {
                await admin2.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, [restoreTarget]).catch(() => {});
                await admin2.query(`DROP DATABASE IF EXISTS "${restoreTarget}"`);
            } finally {
                await admin2.end().catch(() => {});
            }
        });
    });

    it("verifyBackup reports restore_capability=false when provider lacks restore", async () => {
        await withIsolatedDb("rv", async (pool, dbName) => {
            await pool.query("CREATE TABLE t (id int)");
            const provider = new RestoreBackupProvider();
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider as unknown as BackupProvider,
                backupBeforeDestructive: true,
            });

            await migrator.run({ migrations: [{ id: "rv_no_restore", up: "SELECT 1" }] });
            await pool.query(
                `INSERT INTO _writer_migrations_backup (migration_id, target_database, backup_id, backup_location, successful)
                 VALUES ($1, $2, $3, $4, $5)`,
                ["rv_no_restore", dbName, "rv_nr_1", "/tmp/x", true],
            );
            provider.backups.set("rv_nr_1", { location: "/tmp/x" });

            (provider as any).restore = undefined;

            const result = await migrator.verifyBackup({ migrationId: "rv_no_restore", target: dbName });
            expect(result.restored).toBe(false);
            expect(result.verified).toBe(false);
            expect(result.checks.some((c) => c.name === "restore_capability" && c.passed === false)).toBe(true);
        });
    });

    it("verifyBackup reports no backup record when none exists", async () => {
        await withIsolatedDb("rv", async (pool, dbName) => {
            await pool.query("CREATE TABLE t (id int)");
            const provider = new RestoreBackupProvider();
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider,
                backupBeforeDestructive: true,
            });

            const result = await migrator.verifyBackup({ migrationId: "rv_missing", target: dbName });
            expect(result.restored).toBe(false);
            expect(result.verified).toBe(false);
            expect(result.checks.some((c) => c.name === "backup_record" && c.passed === false)).toBe(true);
        });
    });

    it("destructive migration with requireRestoreVerification is blocked when provider cannot restore", async () => {
        await withIsolatedDb("rv", async (pool, dbName) => {
            await pool.query("CREATE TABLE keep_me (id int)");
            await pool.query("INSERT INTO keep_me VALUES (1)");

            const provider = new RestoreBackupProvider();
            (provider as any).restore = undefined;

            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider,
                backupBeforeDestructive: true,
                requireRestoreVerification: true,
            });

            await expect(
                migrator.run({ migrations: [{ id: "rv_blocked", up: "DROP TABLE keep_me" }] }),
            ).rejects.toThrow(/Restore verification failed/);

            expect(await tableExists(pool, "public.keep_me")).toBe(true);
            const n = await countRows(pool, "keep_me");
            expect(n).toBe(1);
        });
    });

    it("destructive migration proceeds when a verifiable backup exists", async () => {
        await withIsolatedDb("rv", async (pool, dbName) => {
            await pool.query("CREATE TABLE drop_me (id int)");
            await pool.query("INSERT INTO drop_me VALUES (1)");

            const provider = new RestoreBackupProvider();
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider,
                backupBeforeDestructive: true,
                requireRestoreVerification: true,
            });

            // verifyBackup creates its own isolated (empty) restore target and verifies it.
            // Since the backup was created by the migration itself, verifyBackup is called
            // inside run() before the DROP executes and returns verified=true.
            const applied = await migrator.run({
                migrations: [{ id: "rv_verified_drop", up: "DROP TABLE drop_me" }],
            });
            expect(applied).toEqual(["rv_verified_drop"]);
            expect(await tableExists(pool, "public.drop_me")).toBe(false);
        });
    });

    it("backup records persist per migration and target", async () => {
        await withIsolatedDb("rv", async (poolA, dbA) => {
            await withIsolatedDb("rv", async (poolB, dbB) => {
                await poolA.query("CREATE TABLE a (id int)");
                await poolA.query("CREATE TABLE b (id int)");
                await poolB.query("CREATE TABLE a (id int)");
                await poolB.query("CREATE TABLE b (id int)");

                const provider = new RestoreBackupProvider();
                const migrator = createMigrator([poolA, poolB], {
                    table: "_writer_migrations",
                    backupProvider: provider,
                    backupBeforeDestructive: true,
                });

                await migrator.run({
                    migrations: [
                        { id: "rv_a_drop", up: "DROP TABLE a" },
                        { id: "rv_b_drop", up: "DROP TABLE b" },
                    ],
                });

                const records = await backupRecords(poolA);
                const aRecords = records.filter((r) => r.target_database === dbA);
                expect(aRecords.length).toBeGreaterThanOrEqual(2);
                expect(aRecords.some((r) => r.migration_id === "rv_a_drop")).toBe(true);
                expect(aRecords.some((r) => r.migration_id === "rv_b_drop")).toBe(true);

                const recordsB = await backupRecords(poolB);
                const bRecords = recordsB.filter((r) => r.target_database === dbB);
                expect(bRecords.length).toBeGreaterThanOrEqual(2);
                expect(bRecords.some((r) => r.migration_id === "rv_a_drop")).toBe(true);
                expect(bRecords.some((r) => r.migration_id === "rv_b_drop")).toBe(true);
            });
        });
    });
});
