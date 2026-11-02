import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import type { ApprovalProvider } from "../../src/types.js";
import { withIsolatedDb } from "./helpers.js";

class RejectingApprovalProvider implements ApprovalProvider {
    requiresApproval() { return true; }
    async approve() { return { approved: false, approvedBy: "reviewer", reason: "rejected" }; }
}

class FailingApprovalProvider implements ApprovalProvider {
    requiresApproval() { return true; }
    async approve() { throw new Error("approval provider down"); }
}

describe("Suite J - General migration integrity", () => {
    it("J1: checksum change after a migration has been applied is detected as drift", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = { id: "j1", up: "CREATE TABLE j1_t (id int)" };
            await migrator.run({ migrations: [m] });
            const changed = [{ id: "j1", up: "CREATE TABLE j1_t (id int, extra text)" }];
            await expect(migrator.run({ migrations: changed })).rejects.toThrow(/drift detected/);
        });
    });

    it("J2: duplicate migration IDs are rejected", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await expect(migrator.run({
                migrations: [
                    { id: "j2", up: "CREATE TABLE j2a (id int)" },
                    { id: "j2", up: "CREATE TABLE j2b (id int)" },
                ],
            })).rejects.toThrow(/Duplicate migration id/);
        });
    });

    it("J3: missing migration dependencies are rejected", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await expect(migrator.run({
                migrations: [{ id: "j3_child", up: "SELECT 1", dependsOn: ["j3_missing"] }],
            })).rejects.toThrow(/depends on unknown migration/);
        });
    });

    it("J4: circular migration dependencies are rejected", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await expect(migrator.run({
                migrations: [
                    { id: "j4_a", up: "SELECT 1", dependsOn: ["j4_b"] },
                    { id: "j4_b", up: "SELECT 1", dependsOn: ["j4_a"] },
                ],
            })).rejects.toThrow(/cycle/i);
        });
    });

    it("J5: migration history table corruption - status/history remain resilient", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await migrator.run({ migrations: [{ id: "j5", up: "CREATE TABLE j5_t (id int)" }] });
            await pool.query(`INSERT INTO _writer_migration_history (migration_id, status, checksum, runner_id) VALUES ('j5', 'corrupt_status', 'deadbeef', 'test')`);
            const status = await migrator.status({ migrations: [{ id: "j5", up: "CREATE TABLE j5_t (id int)" }] });
            expect(status[0].status).toBe("applied");
            const hist = await migrator.history({ migrationId: "j5" });
            expect(hist.length).toBeGreaterThanOrEqual(1);
        });
    });

    it("J9: statement timeout aborts a long-running migration", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations", statementTimeout: 100 });
            await expect(migrator.run({
                migrations: [{ id: "j9", up: "SELECT pg_sleep(5)" }],
            })).rejects.toThrow();
        });
    });

    it("J10: retry exhaustion on a persistently failing migration", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await expect(migrator.run({
                migrations: [{
                    id: "j10",
                    up: "SELECT * FROM nonexistent_table_j10",
                    retry: { maxAttempts: 2, baseDelayMs: 10 },
                }],
            })).rejects.toThrow();
        });
    });

    it("J12: schema drift across targets is reported by detectDrift", async () => {
        await withIsolatedDb("suite_j", async (poolA, dbA) => {
            await withIsolatedDb("suite_j", async (poolB, dbB) => {
                const migrator = createMigrator([poolA, poolB], { table: "_writer_migrations" });
                const m = { id: "j12", up: "CREATE TABLE j12_t (id int)" };
                await migrator.run({ migrations: [m] });
                await poolB.query(`UPDATE _writer_migrations SET checksum = '0000000000000000000000000000000000000000000000000000000000000000' WHERE id = 'j12'`);
                const drift = await migrator.detectDrift({ migrations: [m] });
                const cm = drift.drift.filter((d) => d.migrationId === "j12" && d.driftType === "checksum_mismatch");
                expect(cm.length).toBeGreaterThanOrEqual(1);
            });
        });
    });

    it("J13: baseline creation and verification", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const m = { id: "j13", up: "CREATE TABLE j13_t (id int)" };
            await migrator.run({ migrations: [m] });
            const result = await migrator.baseline({ name: "j13-baseline", migrations: [m], verify: true });
            expect(result.baseline.name).toBe("j13-baseline");
            expect(result.baseline.appliedMigrationIds).toContain("j13");
            const list = await migrator.listBaselines();
            expect(list.some((b) => b.name === "j13-baseline")).toBe(true);
        });
    });

    it("J14: squash creation and verification", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const ms = [
                { id: "j14_a", up: "CREATE TABLE j14_a (id int)" },
                { id: "j14_b", up: "CREATE TABLE j14_b (id int)" },
            ];
            await migrator.run({ migrations: ms });
            const result = await migrator.squash({ name: "j14-squash", migrations: ms, verify: true });
            expect(result.squashed).toEqual(["j14_a", "j14_b"]);
            const list = await migrator.listBaselines();
            expect(list.some((b) => b.name === "j14-squash")).toBe(true);
        });
    });

    it("J15: approval rejection blocks the migration", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                approvalProvider: new RejectingApprovalProvider(),
                approvalRequiredForDestructive: true,
            });
            await expect(migrator.run({
                migrations: [{ id: "j15", up: "DROP TABLE IF EXISTS j15_t" }],
            })).rejects.toThrow(/Approval required/);
        });
    });

    it("J16: approval provider failure blocks the migration", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, {
                table: "_writer_migrations",
                approvalProvider: new FailingApprovalProvider(),
                approvalRequiredForDestructive: true,
            });
            await expect(migrator.run({
                migrations: [{ id: "j16", up: "DROP TABLE IF EXISTS j16_t" }],
            })).rejects.toThrow(/approval provider down/);
        });
    });

    it("J17: migration execution after baseline", async () => {
        await withIsolatedDb("suite_j", async (pool) => {
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const base = { id: "j17_base", up: "CREATE TABLE j17_base (id int)" };
            const next = { id: "j17_next", up: "CREATE TABLE j17_next (id int)" };
            await migrator.run({ migrations: [base] });
            await migrator.baseline({ name: "j17-baseline", migrations: [base], verify: true });
            const applied = await migrator.run({ migrations: [next] });
            expect(applied).toContain("j17_next");
        });
    });

    it("J18: reconciliation reports a checksum conflict", async () => {
        await withIsolatedDb("suite_j", async (poolA, dbA) => {
            await withIsolatedDb("suite_j", async (poolB, dbB) => {
                const migrator = createMigrator([poolA, poolB], { table: "_writer_migrations" });
                const m = { id: "j18", up: "CREATE TABLE j18_t (id int)" };
                await migrator.run({ migrations: [m] });
                await poolB.query(`UPDATE _writer_migrations SET checksum = '0000000000000000000000000000000000000000000000000000000000000000' WHERE id = 'j18'`);
                // detectDrift catches the checksum mismatch
                const drift = await migrator.detectDrift({ migrations: [m] });
                const cm = drift.drift.filter((d) => d.migrationId === "j18" && d.driftType === "checksum_mismatch");
                expect(cm.length).toBeGreaterThanOrEqual(1);
                // reconcile() now inspects checksums, so a drifted-but-applied target is a conflict.
                const rec = await migrator.reconcile({ allTargets: true, migrations: [m] });
                expect(rec.conflicts).toContain("j18");
                expect(rec.reconciled).not.toContain("j18");
            });
        });
    });
});
