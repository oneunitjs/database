import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";

import {
    createDatabase,
} from "../../src/index.js";
import { Pool } from "pg";

// Integration tests require a live PostgreSQL instance.
// These tests are skipped if no PostgreSQL is available.
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
    // Unix socket connection: host is specified via query parameter
    return `postgresql://${PG_USER}${auth}@/${dbname}?host=/run/postgresql`;
}

let hasPostgres = false;
let useSocket = false;

function dbUri(dbname) {
    return useSocket ? pgSocketUri(dbname) : pgUri(dbname);
}

describe("Sharding integration tests (live PostgreSQL)", () => {
    beforeAll(async () => {
        // Try TCP first
        try {
            const pool = new Pool({ connectionString: pgUri("postgres") });
            await pool.query("SELECT 1");
            await pool.end();
            hasPostgres = true;
            useSocket = false;
        } catch {
            // Try Unix socket
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
        if (!hasPostgres) {
            return;
        }
        // Create test databases
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["shard_integration_0", "shard_integration_1"]) {
            try {
                await pool.query(`CREATE DATABASE ${db}`);
            } catch {
                // Database may already exist
            }
        }
        await pool.end();
    }, 30000);

    afterAll(async () => {
        if (!hasPostgres) {
            return;
        }
        // Drop test databases
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["shard_integration_0", "shard_integration_1"]) {
            try {
                await pool.query(`DROP DATABASE ${db} WITH (FORCE)`);
            } catch {
                // Ignore errors
            }
        }
        await pool.end();
    }, 30000);

    beforeEach(async () => {
        if (!hasPostgres) {
            return;
        }
        // Ensure tables exist
        const pool0 = new Pool({ connectionString: dbUri("shard_integration_0") });
        const pool1 = new Pool({ connectionString: dbUri("shard_integration_1") });
        await pool0.query(`
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                shard_key TEXT
            )
        `);
        await pool1.query(`
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                shard_key TEXT
            )
        `);
        await pool0.end();
        await pool1.end();
    }, 10000);

    it("createDatabase with real shards and hash routing", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    {
                        id: "shard-0",
                        connection: {
                            connectionString: dbUri("shard_integration_0"),
                            max: 5,
                        },
                    },
                    {
                        id: "shard-1",
                        connection: {
                            connectionString: pgUri("shard_integration_1"),
                            max: 5,
                        },
                    },
                ],
                defaultShardId: "shard-0",
            },
        });

        expect(db.sharding.enabled).toBe(true);
        expect(db.sharding.strategy).toBe("hash");
        expect(db.sharding.shards.length).toBe(2);
        expect(db.sharding.routingVersion).toBe(1);

        // Route a query to shard-0 via hash
        const shard = db.sharding.getShardForKey("user-1");
        expect(shard.id).toBeDefined();

        // Execute a query through the unified API
        const result = await db.query(
            "INSERT INTO test_table (name, shard_key) VALUES ($1, $2) RETURNING id, shard_key",
            ["test-user", "user-1"],
            { shardKey: "user-1" },
        );
        expect(result.rows.length).toBe(1);
        expect(result.rows[0].shard_key).toBe("user-1");

        // Query from the same shard
        const result2 = await db.query(
            "SELECT * FROM test_table WHERE shard_key = $1",
            ["user-1"],
            { shardKey: "user-1" },
        );
        expect(result2.rows.length).toBe(1);

        // Transaction on a shard
        const txResult = await db.transaction(
            async (client) => {
                await client.query(
                    "INSERT INTO test_table (name, shard_key) VALUES ($1, $2)",
                    ["tx-user", "tx-key"],
                );
                return { inserted: true };
            },
            { shardKey: "tx-key" },
        );
        expect(txResult.inserted).toBe(true);

        await db.sharding.shutdown();
    }, 30000);

    it("health check on real shards", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    {
                        id: "shard-0",
                        connection: {
                            connectionString: dbUri("shard_integration_0"),
                        },
                    },
                    {
                        id: "shard-1",
                        connection: {
                            connectionString: dbUri("shard_integration_1"),
                        },
                    },
                ],
            },
        });

        const health = await db.sharding.health();
        expect(health.healthy).toBe(true);
        expect(health.shards.length).toBe(2);
        expect(health.upShards.length).toBe(2);
        expect(health.downShards.length).toBe(0);

        for (const shard of health.shards) {
            expect(shard.status).toBe("up");
            expect(shard.latency.unit).toBe("ms");
            expect(typeof shard.latency.value).toBe("number");
            expect(shard.pool).toBeDefined();
            expect(shard.pool.total).toBeGreaterThanOrEqual(0);
        }

        const single = await db.sharding.shardHealth("shard-0");
        expect(single).toBeDefined();
        expect(single.status).toBe("up");

        await db.sharding.shutdown();
    }, 30000);

    it("addShard and removeShard (resharding)", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    {
                        id: "shard-0",
                        connection: {
                            connectionString: dbUri("shard_integration_0"),
                        },
                    },
                ],
            },
        });

        expect(db.sharding.shards.length).toBe(1);
        expect(db.sharding.routingVersion).toBe(1);

        let beforeVersion;
        let afterVersion;
        let addedShards = [];
        let removedShards = [];

        // Add shard-1 with hooks
        await db.sharding.addShard(
            {
                id: "shard-1",
                connection: {
                    connectionString: dbUri("shard_integration_1"),
                },
            },
            {
                onBeforeReshard: (oldV, newV, _added, _removed) => {
                    beforeVersion = oldV;
                    addedShards = _added.map((s) => s.id);
                    removedShards = _removed;
                },
                onAfterReshard: (oldV, newV, _added, _removed) => {
                    afterVersion = newV;
                },
            },
        );

        expect(db.sharding.shards.length).toBe(2);
        expect(db.sharding.routingVersion).toBe(2);
        expect(beforeVersion).toBe(1);
        expect(afterVersion).toBe(2);
        expect(addedShards).toEqual(["shard-1"]);
        expect(removedShards).toEqual([]);

        // Route to new shard
        const shard = db.sharding.getShardForKey("user-999");
        expect(shard.id).toMatch(/^shard-[01]$/);

        await db.sharding.shutdown();
    }, 30000);

    it("migration coordination across shards", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    {
                        id: "shard-0",
                        connection: {
                            connectionString: dbUri("shard_integration_0"),
                        },
                    },
                    {
                        id: "shard-1",
                        connection: {
                            connectionString: dbUri("shard_integration_1"),
                        },
                    },
                ],
            },
        });

        // Run a migration on all shards
        const result = await db.sharding.migrate.runMigrationOnAllShards(
            "001_create_test_table",
            `
                CREATE TABLE IF NOT EXISTS migration_test (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            `,
        );

        expect(result.allApplied).toBe(true);
        expect(result.anyFailed).toBe(false);
        expect(result.shards["shard-0"]).toBe("applied");
        expect(result.shards["shard-1"]).toBe("applied");

        // Check migration status
        const status = db.sharding.migrate.getMigrationStatus("001_create_test_table");
        expect(status).toBeDefined();
        expect(status.allApplied).toBe(true);

        // Check per-shard migration records
        const shard0Migrations = db.sharding.migrate.getShardMigrationStatus("shard-0");
        expect(shard0Migrations.length).toBe(1);
        expect(shard0Migrations[0].migrationId).toBe("001_create_test_table");
        expect(shard0Migrations[0].status).toBe("applied");

        // Get migration stats
        const stats = db.sharding.migrate.getMigrationStats();
        expect(stats.totalMigrations).toBe(1);
        expect(stats.fullyApplied).toBe(1);

        await db.sharding.shutdown();
    }, 30000);

    it("migration failure handling", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    {
                        id: "shard-0",
                        connection: {
                            connectionString: dbUri("shard_integration_0"),
                        },
                    },
                ],
            },
        });

        // Run a migration that will fail (invalid SQL)
        const result = await db.sharding.migrate.runMigrationOnAllShards(
            "002_bad_migration",
            "CREATE TABLE invalid_syntax ((", // Invalid SQL
        );

        expect(result.allApplied).toBe(false);
        expect(result.anyFailed).toBe(true);
        expect(result.shards["shard-0"]).toBe("failed");

        const status = db.sharding.migrate.getMigrationStatus("002_bad_migration");
        expect(status.allApplied).toBe(false);
        expect(status.anyFailed).toBe(true);

        await db.sharding.shutdown();
    }, 30000);

    it("non-sharded database still works with real PostgreSQL", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const db = createDatabase({
            connectionString: dbUri("shard_integration_0"),
        });

        expect(db.sharding.enabled).toBe(false);

        const result = await db.query(
            "INSERT INTO test_table (name, shard_key) VALUES ($1, $2) RETURNING id",
            ["direct-insert", "direct-key"],
        );
        expect(result.rows.length).toBe(1);
        expect(typeof result.rows[0].id).toBe("number");

        await db.shutdown();
    }, 30000);
});
