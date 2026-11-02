import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { Pool } from "pg";
import { createMigrator, createSchemaManager } from "../../src/index.js";

const POOL_CONFIG = { user: "postgres", database: "postgres", host: "/run/postgresql", max: 3 };
const TEST_DB = process.env.ITEST_DB ?? "rel_itest";
let admin: Pool;
const TEST_DB_2 = TEST_DB + "_b";
const TEST_DB_3 = TEST_DB + "_c";

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

beforeAll(async () => {
    if (!postgresAvailable) {
        return;
    }
    admin = new Pool({ ...POOL_CONFIG });
    for (const db of [TEST_DB, TEST_DB_2, TEST_DB_3]) {
        await admin.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()`, [db]).catch(() => {});
        await admin.query(`DROP DATABASE IF EXISTS ${db}`).catch(() => {});
        await admin.query(`CREATE DATABASE ${db}`).catch(() => {});
    }
    await admin.end();
});

function testPool(db = TEST_DB): Pool {
    return new Pool({ user: "postgres", database: db, host: "/run/postgresql", max: 3 });
}

function testPoolDb(db): Pool {
    return new Pool({ user: "postgres", database: db, host: "/run/postgresql", max: 3 });
}

describe.skipIf(!postgresAvailable)("Migration Reliability — populated rollback", () => {
    it("rollback with AUTOMATIC down() preserves data via explicit down", async () => {
        const p = testPool();
        try {
            await p.query("DROP TABLE IF EXISTS pr_t CASCADE");
            const m = createMigrator(p, { table: "_writer_migrations" });
            await m.status({ migrations: [] }).catch(() => {});
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history CASCADE");
            await m.run({ migrations: [{ id: "pr1", up: "CREATE TABLE pr_t (id int, name text)", down: "DROP TABLE pr_t" }] });
            await p.query("INSERT INTO pr_t VALUES (1,'a'),(2,'b'),(3,'c')");
            const { rows } = await p.query("SELECT COUNT(*)::int c FROM pr_t");
            expect(rows[0].c).toBe(3);
            // explicit destructive down requires allowDestructive
            await expect(m.rollback({ migrations: [{ id: "pr1", up: "CREATE TABLE pr_t (id int, name text)", down: "DROP TABLE pr_t" }], steps: 1 })).rejects.toThrow();
            const after = await p.query("SELECT COUNT(*)::int c FROM pr_t");
            expect(after.rows[0].c).toBe(3);
        } finally { await p.end(); }
    });
});

describe.skipIf(!postgresAvailable)("Migration Reliability — lock diagnostics", () => {
    it("detects a real long-running transaction", async () => {
        const p = testPool();
        try {
            const m = createMigrator(p, { table: "_writer_migrations" });
            const c = await p.connect();
            await c.query("BEGIN");
            await c.query("CREATE TABLE dlt_t (id int)");
            await new Promise(r => setTimeout(r, 150));
            const result = await m.detectLongRunningTransactions({ minDurationMs: 50 });
            await c.query("ROLLBACK");
            c.release();
            expect(result.transactions.length).toBeGreaterThanOrEqual(1);
            const r = result.transactions.find(t => t.state === "idle in transaction" || t.state === "active");
            expect(r).toBeDefined();
        } finally { await p.end(); }
    });

    it("does not alter activity queries", async () => {
        const p = testPool();
        try {
            const m = createMigrator(p, { table: "_writer_migrations" });
            const result = await m.getLockDiagnostics();
            expect(result.advisoryLocks).toBeDefined();
            expect(result.longRunningTransactions).toBeDefined();
            expect(["free","held","contended"]).toContain(result.advisoryLockStatus);
        } finally { await p.end(); }
    });
});

describe.skipIf(!postgresAvailable)("Migration Reliability — approval + backups", () => {
    it("backup provider failure blocks destructive migration (allowDestructive not set)", async () => {
        const p = testPool();
        try {
            await p.query("DROP TABLE IF EXISTS bkt CASCADE");
            await p.query("CREATE TABLE bkt (id int)");
            const provider = { async backup() { throw new Error("backup down"); }, async exists() { return false; } };
            const m = createMigrator(p, { table: "_writer_migrations", backupProvider: provider as any, backupBeforeDestructive: true });
            await m.status({ migrations: [] }).catch(() => {});
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history, _writer_migrations_backup CASCADE");
            const ms = [{ id: "b2", up: "DELETE FROM bkt", down: "INSERT INTO bkt VALUES (1)" }];
            await expect(m.run({ migrations: ms })).rejects.toThrow(/backup/i);
        } finally { await p.end(); }
    });

    it("approval provider failure stops execution", async () => {
        const p = testPool();
        try {
            await p.query("DROP TABLE IF EXISTS apt CASCADE");
            await p.query("CREATE TABLE apt (id int)");
            const provider = { requiresApproval() { return true; }, async approve() { throw new Error("approval provider down"); } };
            const m = createMigrator(p, { table: "_writer_migrations", approvalProvider: provider as any, approvalRequiredForDestructive: true });
            await m.status({ migrations: [] }).catch(() => {});
            await p.query("TRUNCATE _writer_migrations, _writer_migration_history, _writer_migrations_approval CASCADE");
            const ms = [{ id: "ap2", up: "DELETE FROM apt", down: "INSERT INTO apt VALUES (1)" }];
            await expect(m.run({ migrations: ms })).rejects.toThrow();
        } finally { await p.end(); }
    });
});
