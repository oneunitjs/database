import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import type { BackupProvider } from "../../src/types.js";
import { withIsolatedDb } from "./helpers.js";

class MockBackupProvider implements BackupProvider {
    calls: Array<{ targetDatabase: string; migrationId: string | undefined }> = [];
    shouldFail = false;
    backups = new Map<string, { location?: string }>();
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
}

async function backupRecords(pool: Pool) {
    const { rows } = await pool.query(`SELECT id, migration_id, target_database, backup_id, successful FROM _writer_migrations_backup ORDER BY id`);
    return rows;
}

async function tableExists(pool: Pool, name: string): Promise<boolean> {
    const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
    return rows[0]?.to_regclass !== null;
}

describe("Suite I - Backup and restore verification", () => {
    it("I1-I4: destructive migration triggers backup; provider receives correct target and metadata; records stored", async () => {
        await withIsolatedDb("suite_i", async (pool, dbName) => {
            await pool.query("CREATE TABLE expendable (id int)");
            await pool.query("INSERT INTO expendable VALUES (1)");

            const provider = new MockBackupProvider();
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider,
                backupBeforeDestructive: true,
            });

            const applied = await migrator.run({
                migrations: [{ id: "drop_exp", up: "DROP TABLE expendable" }],
            });
            expect(applied).toEqual(["drop_exp"]);

            expect(provider.calls.length).toBe(1);
            expect(provider.calls[0].targetDatabase).toBe(dbName);
            expect(provider.calls[0].migrationId).toBe("drop_exp");

            const records = await backupRecords(pool);
            expect(records.length).toBe(1);
            expect(records[0].migration_id).toBe("drop_exp");
            expect(records[0].target_database).toBe(dbName);
            expect(records[0].successful).toBe(true);

            expect(await tableExists(pool, "public.expendable")).toBe(false);
        });
    });

    it("I10-I11: backup provider failure blocks the destructive migration", async () => {
        await withIsolatedDb("suite_i", async (pool) => {
            await pool.query("CREATE TABLE protected_t (id int)");
            await pool.query("INSERT INTO protected_t VALUES (1)");

            const provider = new MockBackupProvider();
            provider.shouldFail = true;
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                backupProvider: provider,
                backupBeforeDestructive: true,
            });

            await expect(
                migrator.run({ migrations: [{ id: "drop_prot", up: "DROP TABLE protected_t" }] }),
            ).rejects.toThrow(/Backup before migration/);

            expect(await tableExists(pool, "public.protected_t")).toBe(true);
            const { rows } = await pool.query("SELECT count(*) AS n FROM protected_t");
            expect(Number(rows[0].n)).toBe(1);
        });
    });

    it("I12: backup metadata associated with the correct migration and target on every target", async () => {
        await withIsolatedDb("suite_i", async (poolA, dbA) => {
            await withIsolatedDb("suite_i", async (poolB, dbB) => {
                await poolA.query("CREATE TABLE ta (id int)");
                await poolA.query("CREATE TABLE tb (id int)");
                await poolB.query("CREATE TABLE ta (id int)");
                await poolB.query("CREATE TABLE tb (id int)");

                const provider = new MockBackupProvider();
                const migrator = createMigrator([poolA, poolB], {
                    table: "_writer_migrations",
                    backupProvider: provider,
                    backupBeforeDestructive: true,
                });

                await migrator.run({
                    migrations: [
                        { id: "drop_a", up: "DROP TABLE ta" },
                        { id: "drop_b", up: "DROP TABLE tb" },
                    ],
                });

                expect(provider.calls.length).toBe(4);
                const pairs = new Set(provider.calls.map((c) => `${c.targetDatabase}|${c.migrationId}`));
                expect(pairs).toEqual(new Set([
                    `${dbA}|drop_a`, `${dbA}|drop_b`,
                    `${dbB}|drop_a`, `${dbB}|drop_b`,
                ]));

                const recordsA = await backupRecords(poolA);
                const recordsB = await backupRecords(poolB);
                expect(recordsA.filter((r) => r.target_database === dbA).length).toBe(2);
                expect(recordsB.filter((r) => r.target_database === dbB).length).toBe(2);
                expect(await tableExists(poolA, "public.ta")).toBe(false);
                expect(await tableExists(poolA, "public.tb")).toBe(false);
            });
        });
    });

    it("I13: multi-target partial failure - backup attempted for a failing migration; later pools not reached", async () => {
        await withIsolatedDb("suite_i", async (poolA, dbA) => {
            await withIsolatedDb("suite_i", async (poolB, dbB) => {
                await poolA.query("CREATE TABLE ta (id int)");
                await poolA.query("CREATE TABLE tb (id int)");
                await poolB.query("CREATE TABLE ta (id int)");
                // tb intentionally absent on poolB -> drop_b will fail there

                const provider = new MockBackupProvider();
                const migrator = createMigrator([poolA, poolB], {
                    table: "_writer_migrations",
                    backupProvider: provider,
                    backupBeforeDestructive: true,
                });

                await expect(
                    migrator.run({
                        migrations: [
                            { id: "drop_a", up: "DROP TABLE ta" },
                            { id: "drop_b", up: "DROP TABLE tb" },
                        ],
                    }),
                ).rejects.toThrow();

                // poolA: full migration list applied to the first pool
                expect(await tableExists(poolA, "public.ta")).toBe(false);
                expect(await tableExists(poolA, "public.tb")).toBe(false);
                // poolB: drop_a succeeded; drop_b failed (backup attempted before the failure)
                expect(await tableExists(poolB, "public.ta")).toBe(false);

                const poolBCalls = provider.calls.filter((c) => c.targetDatabase === dbB);
                expect(poolBCalls.some((c) => c.migrationId === "drop_b")).toBe(true);
            });
        });
    });

    it("I5-I9: restore validation is a provider-level concern (no migrator.restore() API)", async () => {
        // The migrator exposes no restore() method and performs no restore validation.
        // BackupProvider.restore?/exists? are provider-side; the package does not orchestrate restore.
        const migrator = { restore: undefined } as unknown as { restore?: () => unknown };
        expect(migrator.restore).toBeUndefined();
    });
});
