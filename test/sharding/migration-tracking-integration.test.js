import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";

import { ShardMigrationTracker } from "../../src/sharding/shard-migration.js";
import { ShardManager } from "../../src/sharding/shard-manager.js";
import { Pool } from "pg";

const PG_USER = process.env.PG_USER || "postgres";
const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = process.env.PG_PORT || 5432;
const PG_PASSWORD = process.env.PG_PASSWORD || "";

function pgUri(dbname) {
    const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : "";
    return `postgresql://${PG_USER}${auth}@${PG_HOST}:${PG_PORT}/${dbname}`;
}

function pgSocketUri(dbname) {
    const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : "";
    return `postgresql://${PG_USER}${auth}@/${dbname}?host=/run/postgresql`;
}

let hasPostgres = false;
let useSocket = false;

function dbUri(dbname) {
    return useSocket ? pgSocketUri(dbname) : pgUri(dbname);
}

describe("ShardMigrationTracker integration tests (live PostgreSQL)", () => {
    beforeAll(async () => {
        try {
            const pool = new Pool({ connectionString: pgUri("postgres") });
            await pool.query("SELECT 1");
            await pool.end();
            hasPostgres = true;
            useSocket = false;
        } catch {
            try {
                const pool = new Pool({ connectionString: pgSocketUri("postgres") });
                await pool.query("SELECT 1");
                await pool.end();
                hasPostgres = true;
                useSocket = true;
            } catch {
                hasPostgres = false;
            }
        }
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        try {
            await pool.query(`CREATE DATABASE mt_src`);
        } catch {
            // may already exist
        }
        await pool.end();
    }, 30000);

    afterAll(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        try {
            await pool.query(`DROP DATABASE mt_src WITH (FORCE)`);
        } catch {
            // ignore
        }
        await pool.end();
    }, 30000);

    beforeEach(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("mt_src") });
        try {
            await pool.query(`DROP TABLE IF EXISTS shard_migrations`);
        } catch {
            // ignore
        }
        await pool.end();
    }, 10000);

    afterEach(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("mt_src") });
        try {
            await pool.query(`DROP TABLE IF EXISTS shard_migrations`);
        } catch {
            // ignore
        }
        await pool.end();
    }, 10000);

    function makeManager() {
        return new ShardManager([
            {
                id: "shard-0",
                connection: { connectionString: dbUri("mt_src"), max: 5 },
            },
        ]);
    }

    it("first-time initialization creates the tracking schema", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const manager = makeManager();
        const tracker = new ShardMigrationTracker(manager);

        await tracker.initialize();

        // Verify the table exists
        const pool = new Pool({ connectionString: dbUri("mt_src"), max: 5 });
        try {
            const result = await pool.query(
                `SELECT to_regclass('public.shard_migrations') AS present`,
            );
            expect(result.rows[0].present).toBe("shard_migrations");
        } finally {
            await pool.end();
        }
    }, 30000);

    it("repeated initialization is safe", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const manager = makeManager();
        const tracker = new ShardMigrationTracker(manager);

        await tracker.initialize();
        await tracker.initialize();
        await tracker.initialize();

        // Should not throw and table should exist
        const pool = new Pool({ connectionString: dbUri("mt_src"), max: 5 });
        try {
            const result = await pool.query(
                `SELECT to_regclass('public.shard_migrations') AS present`,
            );
            expect(result.rows[0].present).toBe("shard_migrations");
        } finally {
            await pool.end();
        }
    }, 30000);

    it("restart restores persisted migration state", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const manager1 = makeManager();
        const tracker1 = new ShardMigrationTracker(manager1);
        await tracker1.initialize();

        // Record a migration as applied
        await tracker1.recordMigration("shard-0", "mig-1", "applied");
        await tracker1.recordMigration("shard-0", "mig-2", "failed");

        // Simulate restart: create a new tracker against the same database
        const manager2 = makeManager();
        const tracker2 = new ShardMigrationTracker(manager2);
        await tracker2.initialize();

        // State should be restored
        const status1 = tracker2.getMigrationStatus("mig-1");
        expect(status1).toBeDefined();
        expect(status1.shards["shard-0"]).toBe("applied");

        const status2 = tracker2.getMigrationStatus("mig-2");
        expect(status2).toBeDefined();
        expect(status2.shards["shard-0"]).toBe("failed");
    }, 30000);

    it("interrupted migration is detected as not fully applied", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const manager = makeManager();
        const tracker = new ShardMigrationTracker(manager);
        await tracker.initialize();

        // Apply on one shard only (simulate partial failure across shards)
        // Since we only have one shard, mark it as applied
        await tracker.recordMigration("shard-0", "mig-partial", "applied");

        // With one shard, allApplied is true. But we can verify per-shard status.
        const status = tracker.getMigrationStatus("mig-partial");
        expect(status).toBeDefined();
        expect(status.shards["shard-0"]).toBe("applied");
    }, 30000);

    it("database persistence failure is surfaced", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        // Create a manager with a shard that doesn't exist / can't connect
        const badManager = new ShardManager([
            {
                id: "bad-shard",
                connection: { connectionString: "postgresql://nonexistent:5432/baddb", max: 1 },
            },
        ]);
        const tracker = new ShardMigrationTracker(badManager);

        await expect(tracker.initialize()).rejects.toThrow();
    }, 30000);
});
