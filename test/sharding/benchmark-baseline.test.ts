// Performance baseline for @oneunit/database sharding layer.
// Mocks pg.Pool/PoolClient to measure routing/execution overhead.
// Run: npx vitest run test/sharding/benchmark-baseline.test.js
import { describe, it, expect, beforeEach, vi } from "vitest";
import { Pool } from "pg";

const mockQuery = vi.fn(() =>
    Promise.resolve({ rows: [], rowCount: 1, command: "SELECT", oid: 0, fields: [] }),
);
const mockRelease = vi.fn();
const mockConnect = vi.fn(() => Promise.resolve({ query: mockQuery, release: mockRelease }));

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
class MockPool {
    totalCount = 0;
    idleCount = 10;
    waitingCount = 0;
    connect = mockConnect;
    query = mockQuery;
    end = vi.fn(() => Promise.resolve());
    on = vi.fn();
}
    return {
        ...actual,
        Pool: MockPool,
    };
});

import { createDatabase } from "../../src/database.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function benchSync(name: string, fn: () => unknown, iterations = 10000) {
    for (let i = 0; i < 100; i++) fn();
    const start = process.hrtime.bigint();
    for (let i = 0; i < iterations; i++) fn();
    const end = process.hrtime.bigint();
    const ns = Number(end - start);
    const ms = ns / 1_000_000;
    const avgNs = ns / iterations;
    console.log(
        `${name.padEnd(50)} ${ms.toFixed(2)} ms total | ${avgNs.toFixed(0)} ns/op | ${(
            iterations /
            (ms / 1000)
        ).toFixed(0)} ops/sec`,
    );
}

async function benchAsync(name: string, fn: () => Promise<unknown>, iterations = 1000) {
    for (let i = 0; i < 20; i++) await fn();
    const start = process.hrtime.bigint();
    for (let i = 0; i < iterations; i++) await fn();
    const end = process.hrtime.bigint();
    const ns = Number(end - start);
    const ms = ns / 1_000_000;
    const avgNs = ns / iterations;
    console.log(
        `${name.padEnd(50)} ${ms.toFixed(2)} ms total | ${avgNs.toFixed(0)} ns/op | ${(
            iterations /
            (ms / 1000)
        ).toFixed(0)} ops/sec`,
    );
}

// ---------------------------------------------------------------------------
describe("Sharding performance baseline", () => {
    let nonShardedDb: ReturnType<typeof createDatabase>;
    let shardedDb: ReturnType<typeof createDatabase>;

    beforeEach(() => {
        vi.clearAllMocks();
        mockQuery.mockResolvedValue({ rows: [], rowCount: 1 });

        nonShardedDb = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        shardedDb = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                defaultShardId: "shard-0",
                shards: [
                    { id: "shard-0", connection: { host: "localhost" } },
                    { id: "shard-1", connection: { host: "localhost" } },
                    { id: "shard-2", connection: { host: "localhost" } },
                ],
            },
        });

        // Mock write fencing to return unfenced for benchmarking
        if (shardedDb.resharding) {
            vi.spyOn(shardedDb.resharding, "isShardWriteFenced").mockResolvedValue(false);
        }
    });

    it("prints baseline numbers", async () => {
        console.log("\n=== @oneunit/database Sharding Performance Baseline ===\n");

        console.log("--- Query execution ---\n");
        benchSync("non-sharded db.query()", () => {
            nonShardedDb.query("SELECT 1", [], {});
        });
        benchSync("sharded db.query() with shardKey", () => {
            shardedDb.query("SELECT 1", [], { shardKey: "user-42" });
        });
        benchSync(
            "sharded db.query() without shardKey (default route)",
            () => {
                shardedDb.query("SELECT 1", [], {});
            },
        );
        benchSync("sharded db.queryOne() with shardKey", () => {
            shardedDb.queryOne("SELECT 1", [], { shardKey: "user-42" });
        });
        benchSync("sharded db.prepared() with shardKey", () => {
            shardedDb.prepared("p1", "SELECT 1", [], { shardKey: "user-42" });
        });

        console.log("\n--- QueryBuilder ---\n");
        await benchAsync(
            "non-sharded db.from().execute()",
            async () => {
                await nonShardedDb.from("users").select("*").execute();
            },
        );
        await benchAsync(
            "sharded db.from().withShard().execute()",
            async () => {
                await shardedDb
                    .from("users")
                    .select("*")
                    .withShard("user-42")
                    .execute();
            },
        );
        await benchAsync(
            "sharded db.from().execute() (default route)",
            async () => {
                await shardedDb.from("users").select("*").execute();
            },
        );

        console.log("\n--- Models ---\n");
        await benchAsync(
            "non-sharded db.model().findById()",
            async () => {
                await nonShardedDb.model("users").findById(1);
            },
        );
        await benchAsync(
            "sharded db.model().findById() with shardKey",
            async () => {
                await shardedDb.model("users").findById(1, {
                    shardKey: "user-42",
                });
            },
        );
        await benchAsync(
            "sharded db.model() bound to shard",
            async () => {
                const model = shardedDb.model("users", {
                    shardKey: "tenant-a",
                });
                await model.findById(1);
            },
        );

        console.log("\n--- Transactions ---\n");
        await benchAsync(
            "non-sharded db.transaction()",
            async () => {
                await nonShardedDb.transaction(async (client) => {
                    await client.query("SELECT 1");
                });
            },
        );
        await benchAsync(
            "sharded db.transaction() with shardKey",
            async () => {
                await shardedDb.transaction(
                    async (client) => {
                        await client.query("SELECT 1");
                    },
                    { shardKey: "user-42" },
                );
            },
        );

        console.log("\n--- Batch insert ---\n");
        const rows100 = Array.from({ length: 100 }, (_, i) => ({
            id: i,
            name: `user-${i}`,
        }));
        await benchAsync(
            "non-sharded db.batch.insertMany(100 rows)",
            async () => {
                await nonShardedDb.batch.insertMany("users", rows100);
            },
        );
        await benchAsync(
            "sharded db.batch.insertMany(100 rows) with shardKey",
            async () => {
                await shardedDb.batch.insertMany("users", rows100, {
                    shardKey: "tenant-a",
                });
            },
        );

        console.log("\n--- Fan-out ---\n");
        await benchAsync(
            "db.sharding.fanOut() 3 shards merge=none",
            async () => {
                await shardedDb.sharding.fanOut({
                    query: "SELECT 1 as cnt",
                    merge: "none",
                });
            },
        );
        await benchAsync(
            "db.sharding.queryAllShards() merge=sum",
            async () => {
                await shardedDb.sharding.queryAllShards({
                    query: "SELECT 1 as cnt",
                    merge: "sum",
                });
            },
        );

        console.log("\n--- Topology ---\n");
        benchSync(
            "db.sharding.topology.snapshot()",
            () => {
                shardedDb.sharding.topology.snapshot();
            },
        );
        benchSync("db.sharding.topology.validate()", () => {
            shardedDb.sharding.topology.validate();
        });
        benchSync("db.sharding.topology.planRebalance()", () => {
            shardedDb.sharding.topology.planRebalance();
        });

        console.log("\n--- Observability ---\n");
        benchSync("db.sharding.metrics.snapshot()", () => {
            shardedDb.sharding.metrics.snapshot();
        });
        await benchAsync("db.sharding.health()", async () => {
            await shardedDb.sharding.health();
        });

        console.log("\n=== Baseline complete ===\n");
        expect(true).toBe(true);
    }, 120000);
});
