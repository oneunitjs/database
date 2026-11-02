import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb, waitForBackendDeath } from "./helpers.js";

describe("Suite D - Multi-target partial failure and recovery", () => {
    const M1 = { id: "m1", up: "CREATE TABLE mt_t (id int, name text); INSERT INTO mt_t VALUES (1, 'a');" };
    const M1_BAD = { id: "m1", up: "SELECT * FROM mt_nonexistent_table_xyz;" };

    async function tableExists(pool: Pool, name: string): Promise<boolean> {
        const { rows } = await pool.query("SELECT to_regclass($1)", [name]);
        return rows[0]?.to_regclass !== null;
    }

    async function statusByTarget(migrator: ReturnType<typeof createMigrator>) {
        const all = await migrator.statusAllTargets({ migrations: [M1] });
        const out = new Map<string, string>();
        for (const [target, records] of all) {
            const r = records.find((x) => x.id === "m1");
            out.set(target, r ? `${r.status}` : "(no record)");
        }
        return out;
    }

    it("D1-D8: partial failure across three targets; statusAllTargets/reconcile/detectDrift report inconsistencies", async () => {
        await withIsolatedDb("suite_d", async (poolA, dbA) => {
            await withIsolatedDb("suite_d", async (poolB, dbB) => {
                await withIsolatedDb("suite_d", async (poolC, dbC) => {
                    // Per-target migrators to establish independent states
                    const migA = createMigrator(poolA, { table: "_writer_migrations" });
                    const migB = createMigrator(poolB, { table: "_writer_migrations" });

                    // D1/D2: target A succeeds
                    await migA.run({ migrations: [M1] });
                    expect(await tableExists(poolA, "public.mt_t")).toBe(true);

                    // D3: target B fails (bad SQL)
                    await expect(migB.run({ migrations: [M1_BAD] })).rejects.toThrow();
                    expect(await tableExists(poolB, "public.mt_t")).toBe(false);

                    // D4: target C remains pending / untouched (run() would stop at B's failure,
                    // so C is never attempted by a single multi-pool run)

                    const multi = createMigrator([poolA, poolB, poolC], { table: "_writer_migrations" });

                    // D5: per-target registry states
                    const states = await statusByTarget(multi);
                    const statuses = Array.from(states.values());
                    expect(statuses).toContain("applied");
                    expect(statuses).toContain("failed");
                    expect(statuses).toContain("pending");

                    // D6: statusAllTargets returns per-target status for all pools
                    const all = await multi.statusAllTargets({ migrations: [M1] });
                    expect(all.size).toBe(3);

                    // D7/D8: reconcile and detectDrift identify inconsistent target states
                    const rec = await multi.reconcile({ allTargets: true, migrations: [M1] });
                    expect(rec.conflicts).toContain("m1"); // B failed + A applied + C pending => conflict
                    expect(rec.reconciled).not.toContain("m1");

                    const drift = await multi.detectDrift({ migrations: [M1] });
                    // B is failed -> status_mismatch drift
                    const bDrift = drift.drift.find((d) => d.migrationId === "m1" && d.driftType === "status_mismatch");
                    expect(bDrift).toBeDefined();
                });
            });
        });
    });

    it("D9-D13: retryFailed retries failed migrations on every target; successful targets not re-executed", async () => {
        await withIsolatedDb("suite_d", async (poolA, dbA) => {
            await withIsolatedDb("suite_d", async (poolB, dbB) => {
                await withIsolatedDb("suite_d", async (poolC, dbC) => {
                    const migA = createMigrator(poolA, { table: "_writer_migrations" });
                    const migB = createMigrator(poolB, { table: "_writer_migrations" });
                    const migC = createMigrator(poolC, { table: "_writer_migrations" });

                    await migA.run({ migrations: [M1] }); // A applied
                    await expect(migB.run({ migrations: [M1_BAD] })).rejects.toThrow(); // B failed (same id "m1")
                    // C pending

                    const multi = createMigrator([poolA, poolB, poolC], { table: "_writer_migrations" });

                    // D10: retryFailed is target-aware: poolB's failure is retried; poolA (applied) is skipped.
                    const retry = await multi.retryFailed({ migrations: [M1] });
                    expect(retry.retried).toBe(1);
                    expect(retry.succeeded).toBe(1);
                    expect(retry.failed).toBe(0);
                    expect(retry.skipped).toBe(0);

                    // D11: target A is not unnecessarily re-executed.
                    expect(await tableExists(poolA, "public.mt_t")).toBe(true);

                    // D12: B resumes safely via retryFailed.
                    expect(await tableExists(poolB, "public.mt_t")).toBe(true);
                    const afterB = await statusByTarget(multi);
                    expect(Array.from(afterB.values()).filter((s) => s === "applied")).toHaveLength(2);
                    expect(Array.from(afterB.values())).toContain("pending"); // C still pending

                    // D13: C eventually reaches the expected state.
                    await migC.run({ migrations: [M1] });
                    expect(await tableExists(poolC, "public.mt_t")).toBe(true);
                    const final = await statusByTarget(multi);
                    expect([...final.values()].every((s) => s === "applied")).toBe(true);

                    const recFinal = await multi.reconcile({ allTargets: true, migrations: [M1] });
                    expect(recFinal.conflicts).toHaveLength(0);
                    expect(recFinal.reconciled).toContain("m1");
                });
            });
        });
    });

    it("D14: process crash between target executions leaves per-target durable state", async () => {
        await withIsolatedDb("suite_d", async (poolA, dbA) => {
            await withIsolatedDb("suite_d", async (poolB, dbB) => {
                await withIsolatedDb("suite_d", async (poolC, dbC) => {
                    // Simulate a crash after target A completed but before B/C were attempted:
                    // only A's migrator ran.
                    const migA = createMigrator(poolA, { table: "_writer_migrations" });
                    await migA.run({ migrations: [M1] });
                    // B and C were never touched (crashed between targets)

                    const multi = createMigrator([poolA, poolB, poolC], { table: "_writer_migrations" });
                    const states = await statusByTarget(multi);
                    expect(states.get(dbA)).toBe("applied");
                    expect(states.get(dbB)).toBe("pending");
                    expect(states.get(dbC)).toBe("pending");
                    // No distributed atomicity: A is durable even though B/C never ran
                    expect(await tableExists(poolA, "public.mt_t")).toBe(true);
                    expect(await tableExists(poolB, "public.mt_t")).toBe(false);
                    expect(await tableExists(poolC, "public.mt_t")).toBe(false);
                });
            });
        });
    });

    it("D15: migration committed to registry but audit history missing (killed between COMMIT and recordHistory)", async () => {
        await withIsolatedDb("suite_d", async (pool, dbName) => {
            const mig = createMigrator(pool, { table: "_writer_migrations" });
            await mig.run({ migrations: [M1] });
            expect(await tableExists(pool, "public.mt_t")).toBe(true);

            // Simulate loss of the audit history record (process died between COMMIT and recordHistory)
            await pool.query(`DELETE FROM _writer_migration_history WHERE migration_id = 'm1'`);

            // The migration registry still reports applied (durable per-target state)
            const status = await mig.status({ migrations: [M1] });
            expect(status[0].status).toBe("applied");

            // detectInterruptions has no history row to reconcile (gap: audit history lost)
            const { interruptions } = await mig.detectInterruptions();
            expect(interruptions.find((i) => i.migrationId === "m1")).toBeUndefined();

            // recoverInterruptions finds nothing to recover (applied state is consistent)
            const rec = await mig.recoverInterruptions();
            expect(rec.recovered).toHaveLength(0);
            expect(rec.failed).toHaveLength(0);
        });
    });

    it("D16: checksum consistency across targets", async () => {
        await withIsolatedDb("suite_d", async (poolA, dbA) => {
            await withIsolatedDb("suite_d", async (poolB, dbB) => {
                const migA = createMigrator(poolA, { table: "_writer_migrations" });
                const migB = createMigrator(poolB, { table: "_writer_migrations" });
                await migA.run({ migrations: [M1] });
                await migB.run({ migrations: [M1] });

                const multi = createMigrator([poolA, poolB], { table: "_writer_migrations" });
                const drift = await multi.detectDrift({ migrations: [M1] });
                // Same source, same checksum on both targets -> no checksum drift
                const checksumDrift = drift.drift.filter((d) => d.driftType === "checksum_mismatch");
                expect(checksumDrift).toHaveLength(0);
            });
        });
    });

    it("D17: conflicting migration histories reported accurately", async () => {
        await withIsolatedDb("suite_d", async (poolA, dbA) => {
            await withIsolatedDb("suite_d", async (poolB, dbB) => {
                const migA = createMigrator(poolA, { table: "_writer_migrations" });
                const migB = createMigrator(poolB, { table: "_writer_migrations" });
                await migA.run({ migrations: [M1] });
                await expect(migB.run({ migrations: [M1_BAD] })).rejects.toThrow();

                const multi = createMigrator([poolA, poolB], { table: "_writer_migrations" });
                const rec = await multi.reconcile({ allTargets: true, migrations: [M1] });
                expect(rec.conflicts).toContain("m1");
                expect(rec.reconciled).not.toContain("m1");
            });
        });
    });
});
