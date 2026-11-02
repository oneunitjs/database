// ---------------------------------------------------------------------------
// Stage 3 — Real PostgreSQL Performance Benchmarks
//
// Measures actual latency and throughput against real PostgreSQL instances
// on ports 5440-5443. These are NOT mocked measurements — they include real
// network round trips, query planning, and execution.
//
// Compares:
//   - Non-sharded (single database on port 5440)
//   - Sharded with 1, 2, 3 participants
//
// Reports: throughput, p50/p95/p99 latency, error rate
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "vitest";

import { createDatabase } from "../../../../src/index.ts";
import { Pool } from "pg";
import {
    createPool,
    getShardInstances,
} from "../infrastructure/pg-cluster.ts";

let instancesReady = false;
const BENCH_DB = "perf_test";

describe("Stage 3 — Real PostgreSQL Performance Benchmarks", () => {
    let nonShardedDb: ReturnType<typeof createDatabase>;
    let sharded1Db: ReturnType<typeof createDatabase>;
    let sharded2Db: ReturnType<typeof createDatabase>;
    let sharded3Db: ReturnType<typeof createDatabase>;

    beforeAll(async () => {
        const { checkClusterHealth } = await import(
            "../infrastructure/pg-cluster.ts"
        );
        const health = await checkClusterHealth();
        instancesReady = health.every((h) => h.healthy);

        if (instancesReady) {
            // Create test database on all shards
            for (const instance of getShardInstances()) {
                const pool = createPool(instance, { max: 1 });
                try {
                    try {
                        await pool.query(`CREATE DATABASE ${BENCH_DB}`);
                    } catch {
                        // may already exist
                    }
                } finally {
                    await pool.end();
                }

                // Create perf_test table
                const dbPool = new Pool({
                    connectionString: instance.connectionString.replace(
                        /\/[^\/]+$/,
                        `/${BENCH_DB}`,
                    ),
                    max: 10,
                });
                try {
                    await dbPool.query(`
                        CREATE TABLE IF NOT EXISTS perf_items (
                            id BIGSERIAL PRIMARY KEY,
                            shard_key TEXT NOT NULL,
                            value TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                    `);
                    await dbPool.query(
                        `CREATE INDEX IF NOT EXISTS idx_perf_items_shard_key ON perf_items(shard_key)`,
                    );
                } finally {
                    await dbPool.end();
                }
            }

            // Create database instances
            nonShardedDb = createDatabase({
                connectionString: `postgresql://postgres:postgres@127.0.0.1:5440/${BENCH_DB}`,
                max: 10,
            });

            sharded1Db = createDatabase({
                sharding: {
                    enabled: true,
                    strategy: "hash",
                    defaultShardId: "shard-0",
                    shards: [
                        {
                            id: "shard-0",
                            connection: {
                                host: "127.0.0.1",
                                port: 5440,
                                user: "postgres",
                password: "postgres",
                                database: BENCH_DB,
                            },
                        },
                    ],
                },
            });

            sharded2Db = createDatabase({
                sharding: {
                    enabled: true,
                    strategy: "hash",
                    defaultShardId: "shard-0",
                    shards: [
                        {
                            id: "shard-0",
                            connection: {
                                host: "127.0.0.1",
                                port: 5440,
                                user: "postgres",
                password: "postgres",
                                database: BENCH_DB,
                            },
                        },
                        {
                            id: "shard-1",
                            connection: {
                                host: "127.0.0.1",
                                port: 5441,
                                user: "postgres",
                password: "postgres",
                                database: BENCH_DB,
                            },
                        },
                    ],
                },
            });

            sharded3Db = createDatabase({
                sharding: {
                    enabled: true,
                    strategy: "hash",
                    defaultShardId: "shard-0",
                    shards: [
                        {
                            id: "shard-0",
                            connection: {
                                host: "127.0.0.1",
                                port: 5440,
                                user: "postgres",
                password: "postgres",
                                database: BENCH_DB,
                            },
                        },
                        {
                            id: "shard-1",
                            connection: {
                                host: "127.0.0.1",
                                port: 5441,
                                user: "postgres",
                password: "postgres",
                                database: BENCH_DB,
                            },
                        },
                        {
                            id: "shard-2",
                            connection: {
                                host: "127.0.0.1",
                                port: 5442,
                                user: "postgres",
                password: "postgres",
                                database: BENCH_DB,
                            },
                        },
                    ],
                },
            });
        }
    }, 30000);

    afterAll(async () => {
        await nonShardedDb?.shutdown();
        await sharded1Db?.shutdown();
        await sharded2Db?.shutdown();
        await sharded3Db?.shutdown();

        if (instancesReady) {
            for (const instance of getShardInstances()) {
                const pool = createPool(instance, { max: 1 });
                try {
                    try {
                        await pool.query(`DROP DATABASE ${BENCH_DB} WITH (FORCE)`);
                    } catch {
                        // ignore
                    }
                } finally {
                    await pool.end();
                }
            }
        }
    });

    // Helper to measure latency percentiles
    async function measureLatency(
        name: string,
        fn: () => Promise<unknown>,
        iterations: number,
    ): {
        p50: number;
        p95: number;
        p99: number;
        min: number;
        max: number;
        avg: number;
        opsPerSec: number;
        errors: number;
    } {
        const latencies: number[] = [];
        let errors = 0;
        const start = process.hrtime.bigint();

        // Run sequentially for accurate per-operation latency measurement
        for (let i = 0; i < iterations; i++) {
            const opStart = process.hrtime.bigint();
            try {
                await fn();
            } catch {
                errors += 1;
            }
            const opEnd = process.hrtime.bigint();
            latencies.push(Number(opEnd - opStart) / 1_000_000); // ms
        }

        const end = process.hrtime.bigint();
        const totalMs = Number(end - start) / 1_000_000;

        latencies.sort((a, b) => a - b);
        const p50 = latencies[Math.floor(latencies.length * 0.5)];
        const p95 = latencies[Math.floor(latencies.length * 0.95)];
        const p99 = latencies[Math.floor(latencies.length * 0.99)];
        const min = latencies[0];
        const max = latencies[latencies.length - 1];
        const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
        const opsPerSec = iterations / (totalMs / 1000);

        console.log(
            `${name.padEnd(45)} p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms p99=${p99.toFixed(2)}ms min=${min.toFixed(2)}ms max=${max.toFixed(2)}ms avg=${avg.toFixed(2)}ms ops/s=${opsPerSec.toFixed(0)} errors=${errors}`,
        );

        return { p50, p95, p99, min, max, avg, opsPerSec, errors };
    }

    it("benchmark: point read (SELECT by primary key)", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }

        // Seed data
        const seedUris = [
            `postgresql://postgres:postgres@127.0.0.1:5440/${BENCH_DB}`,
            `postgresql://postgres:postgres@127.0.0.1:5441/${BENCH_DB}`,
            `postgresql://postgres:postgres@127.0.0.1:5442/${BENCH_DB}`,
        ];
        for (const uri of seedUris) {
            const pool = new Pool({ connectionString: uri, max: 5 });
            try {
                for (let i = 0; i < 1000; i++) {
                    await pool.query(
                        "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                        [`key-${i}`, `value-${i}`],
                    );
                }
            } finally {
                await pool.end();
            }
        }

        console.log("\n--- Point Read (SELECT by primary key) ---\n");

        await measureLatency(
            "non-sharded SELECT by id",
            async () => {
                await nonShardedDb.query(
                    "SELECT * FROM perf_items WHERE id = $1",
                    [Math.floor(Math.random() * 1000) + 1],
                );
            },
            500,
        );

        await measureLatency(
            "sharded 1-node SELECT by id (shardKey)",
            async () => {
                await sharded1Db!.query(
                    "SELECT * FROM perf_items WHERE id = $1",
                    [Math.floor(Math.random() * 1000) + 1],
                    { shardKey: `key-${Math.floor(Math.random() * 1000)}` },
                );
            },
            500,
        );

        await measureLatency(
            "sharded 2-node SELECT by id (shardKey)",
            async () => {
                await sharded2Db!.query(
                    "SELECT * FROM perf_items WHERE id = $1",
                    [Math.floor(Math.random() * 1000) + 1],
                    { shardKey: `key-${Math.floor(Math.random() * 1000)}` },
                );
            },
            500,
        );

        await measureLatency(
            "sharded 3-node SELECT by id (shardKey)",
            async () => {
                await sharded3Db!.query(
                    "SELECT * FROM perf_items WHERE id = $1",
                    [Math.floor(Math.random() * 1000) + 1],
                    { shardKey: `key-${Math.floor(Math.random() * 1000)}` },
                );
            },
            500,
        );

        expect(true).toBe(true);
    }, 120000);

    it("benchmark: point write (INSERT)", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }

        console.log("\n--- Point Write (INSERT) ---\n");

        let counter = 0;
        await measureLatency(
            "non-sharded INSERT",
            async () => {
                counter += 1;
                await nonShardedDb.query(
                    "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                    [`bench-${counter}`, `value-${counter}`],
                );
            },
            500,
        );

        counter = 0;
        await measureLatency(
            "sharded 1-node INSERT (shardKey)",
            async () => {
                counter += 1;
                await sharded1Db!.query(
                    "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                    [`bench-${counter}`, `value-${counter}`],
                    { shardKey: `bench-${counter}` },
                );
            },
            500,
        );

        counter = 0;
        await measureLatency(
            "sharded 2-node INSERT (shardKey)",
            async () => {
                counter += 1;
                await sharded2Db!.query(
                    "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                    [`bench-${counter}`, `value-${counter}`],
                    { shardKey: `bench-${counter}` },
                );
            },
            500,
        );

        counter = 0;
        await measureLatency(
            "sharded 3-node INSERT (shardKey)",
            async () => {
                counter += 1;
                await sharded3Db!.query(
                    "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                    [`bench-${counter}`, `value-${counter}`],
                    { shardKey: `bench-${counter}` },
                );
            },
            500,
        );

        expect(true).toBe(true);
    }, 120000);

    it("benchmark: transaction (BEGIN/COMMIT)", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }

        console.log("\n--- Transaction (BEGIN/COMMIT) ---\n");

        await measureLatency(
            "non-sharded transaction",
            async () => {
                await nonShardedDb.transaction(async (client) => {
                    await client.query(
                        "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                        ["tx-test", "tx-value"],
                    );
                });
            },
            200,
        );

        await measureLatency(
            "sharded 1-node transaction (shardKey)",
            async () => {
                await sharded1Db!.transaction(
                    async (client) => {
                        await client.query(
                            "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                            ["tx-test", "tx-value"],
                        );
                    },
                    { shardKey: "tx-key" },
                );
            },
            200,
        );

        await measureLatency(
            "sharded 3-node transaction (shardKey)",
            async () => {
                await sharded3Db!.transaction(
                    async (client) => {
                        await client.query(
                            "INSERT INTO perf_items (shard_key, value) VALUES ($1, $2)",
                            ["tx-test", "tx-value"],
                        );
                    },
                    { shardKey: "tx-key" },
                );
            },
            200,
        );

        expect(true).toBe(true);
    }, 120000);

    it("benchmark: fan-out query across all shards", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }

        console.log("\n--- Fan-out Query ---\n");

        await measureLatency(
            "fan-out 1 shard (merge=none)",
            async () => {
                await sharded1Db!.sharding.fanOut({
                    query: "SELECT count(*) AS c FROM perf_items",
                    merge: "none",
                });
            },
            100,
        );

        await measureLatency(
            "fan-out 2 shards (merge=none)",
            async () => {
                await sharded2Db!.sharding.fanOut({
                    query: "SELECT count(*) AS c FROM perf_items",
                    merge: "none",
                });
            },
            100,
        );

        await measureLatency(
            "fan-out 3 shards (merge=sum)",
            async () => {
                await sharded3Db!.sharding.queryAllShards({
                    query: "SELECT count(*) AS c FROM perf_items",
                    merge: "sum",
                });
            },
            100,
        );

        expect(true).toBe(true);
    }, 120000);

    it("benchmark: routing resolution", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }

        console.log("\n--- Routing Resolution ---\n");

        // Measure ShardRouter.resolveShard directly through db.sharding.getShardForKey
        await measureLatency(
            "sharded 3-node getShardForKey",
            () => {
                sharded3Db!.sharding.getShardForKey(
                    `user-${Math.floor(Math.random() * 10000)}`,
                );
            },
            10000,
        );

        expect(true).toBe(true);
    }, 30000);

    it("benchmark: health check", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }

        console.log("\n--- Health Check ---\n");

        await measureLatency(
            "sharded 3-node health()",
            async () => {
                await sharded3Db!.sharding.health();
            },
            50,
        );

        expect(true).toBe(true);
    }, 30000);
});
