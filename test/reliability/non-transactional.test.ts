import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb, waitForBackendDeath } from "./helpers.js";
import { spawnTsxAndCapture } from "./crash.js";

const NONTRANS_RUNNER = "test/reliability/nontrans-crash-runner.ts";

async function runNontransJob(
    dbName: string,
    id: string,
    table: string,
) {
    return spawnTsxAndCapture(
        NONTRANS_RUNNER,
        [`db=${dbName}`, `table=${table}`, `id=${id}`],
        60_000,
    );
}

async function countFullName(pool: Pool, table: string, suffix: string): Promise<number> {
    const { rows } = await pool.query(`SELECT count(*) AS n FROM ${table} WHERE full_name LIKE $1`, [`%${suffix}`]);
    return Number(rows[0].n);
}

async function operationJournalRow(pool: Pool, migrationId: string) {
    const { rows } = await pool.query(
        `SELECT operation_id, migration_id, status, statement_sql, started_at, completed_at, error_message FROM _writer_migration_operations WHERE migration_id = $1 ORDER BY started_at`,
        [migrationId],
    );
    return rows[0] ?? null;
}

describe("Stage 3 — Non-transactional interruption & durable operation journal", () => {
    it("non-transactional migration runs to completion and records applied status in the journal", async () => {
        await withIsolatedDb("nt", async (pool, dbName) => {
            const table = "nt_complete";
            const id = "nt_mig_complete";
            await pool.query(`
                CREATE TABLE ${table} (
                    id BIGSERIAL PRIMARY KEY,
                    payload TEXT,
                    full_name TEXT
                );
            `);
            await pool.query(`INSERT INTO ${table} (payload, full_name) SELECT 'p_' || g, NULL FROM generate_series(1, 5000) g`);

            const run = await runNontransJob(dbName, id, table);
            expect(run.timedOut).toBe(false);
            expect(run.code).toBe(0);

            // All rows transformed by the two autocommit statements.
            expect(await countFullName(pool, table, "_b")).toBe(5000);

            // Journal records the migration as applied.
            const op = await operationJournalRow(pool, id);
            expect(op).not.toBeNull();
            expect(op.status).toBe("applied");
            expect(op.completed_at).not.toBeNull();
        });
    });

    it("detectInterruptions reports non-transactional started journal rows as interruptions", async () => {
        await withIsolatedDb("nt", async (pool, dbName) => {
            const table = "nt_journal";
            await pool.query(`CREATE TABLE ${table} (id BIGSERIAL PRIMARY KEY, payload TEXT)`);
            await pool.query(`INSERT INTO ${table} (payload) SELECT 'p_' || g FROM generate_series(1, 10) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            // ensureTables must run before we can seed the operations table directly.
            await migrator.status({ migrations: [] });
            // Manually seed a "started" operation journal record (simulating a crash mid-migration).
            await pool.query(
                `INSERT INTO _writer_migration_operations (migration_id, target_database, operation_id, operation_type, status, statement_sql, started_at, runner_id, host)
                 VALUES ('nt_journal_mig', $1, 'op_seed', 'migration', 'started', 'UPDATE nt_journal SET x=1', CURRENT_TIMESTAMP, 'test', 'test')`,
                [dbName],
            );

            const { interruptions } = await migrator.detectInterruptions();
            const found = interruptions.find((i) => i.migrationId === "nt_journal_mig" && i.mode === "non-transactional");
            expect(found).toBeDefined();
            expect(found!.mode).toBe("non-transactional");
        });
    });

    it("recoverInterruptions marks a non-transactional interruption as failed instead of blindly rerunning", async () => {
        await withIsolatedDb("nt", async (pool, dbName) => {
            const table = "nt_recover";
            await pool.query(`CREATE TABLE ${table} (id BIGSERIAL PRIMARY KEY, payload TEXT, full_name TEXT)`);
            await pool.query(`INSERT INTO ${table} (payload, full_name) SELECT 'p_' || g, NULL FROM generate_series(1, 10) g`);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            await migrator.status({ migrations: [] });
            // Seed a "started" journal record (interrupted mid non-transactional migration).
            await pool.query(
                `INSERT INTO _writer_migration_operations (migration_id, target_database, operation_id, operation_type, status, statement_sql, started_at, runner_id, host)
                 VALUES ('nt_recover_mig', $1, 'op_seed2', 'migration', 'started', 'UPDATE nt_recover SET full_name=x', CURRENT_TIMESTAMP, 'test', 'test')`,
                [dbName],
            );

            const recovery = await migrator.recoverInterruptions();
            expect(recovery.failed).toContain("nt_recover_mig");

            const op = await operationJournalRow(pool, "nt_recover_mig");
            expect(op.status).toBe("failed");
            expect(op.error_message).toContain("requires manual verification");
        });
    });
});
