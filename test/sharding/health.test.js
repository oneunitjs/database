import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    createDatabase,
    ShardHealth,
    ShardManager,
} from "../../src/index.js";

const mockQuery = vi.fn();
const mockConnect = vi.fn();
const mockRelease = vi.fn();
const mockEnd = vi.fn();
const mockOn = vi.fn();

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => ({
            query: mockQuery,
            connect: mockConnect,
            release: mockRelease,
            end: mockEnd,
            on: mockOn,
            totalCount: 5,
            idleCount: 3,
            waitingCount: 0,
        })),
    };
});

function resetMocks() {
    vi.clearAllMocks();
    mockConnect.mockResolvedValue({
        query: mockQuery,
        release: mockRelease,
    });
    mockQuery.mockResolvedValue({ rows: [1], rowCount: 1 });
    mockEnd.mockResolvedValue(undefined);
}

function makeShardConfig(id, connectionString) {
    return {
        id,
        connection: connectionString
            ? { connectionString }
            : { host: "localhost", port: 5432, database: id, user: "test" },
    };
}

describe("ShardHealth", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("creates health checker for each shard", () => {
        const manager = new ShardManager([
            makeShardConfig("shard-0"),
            makeShardConfig("shard-1"),
        ]);
        const health = new ShardHealth(manager);

        expect(health.getConsecutiveFailures("shard-0")).toBe(0);
        expect(health.getConsecutiveFailures("shard-1")).toBe(0);
        expect(health.getLastError("shard-0")).toBeUndefined();
        expect(health.getLastCheckedAt("shard-0")).toBeUndefined();

        manager.shutdown();
    });

    it("shardHealth returns up for healthy pool", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        const result = await health.shardHealth("shard-0");
        expect(result).toBeDefined();
        expect(result?.shardId).toBe("shard-0");
        expect(result?.status).toBe("up");
        expect(result?.latency.unit).toBe("ms");
        expect(typeof result?.latency.value).toBe("number");
        expect(result?.pool).toEqual({ total: 5, idle: 3, waiting: 0 });

        manager.shutdown();
    });

    it("shardHealth returns undefined for unknown shard", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        const result = await health.shardHealth("unknown-shard");
        expect(result).toBeUndefined();

        manager.shutdown();
    });

    it("health returns aggregate status for all shards", async () => {
        const manager = new ShardManager([
            makeShardConfig("shard-0"),
            makeShardConfig("shard-1"),
            makeShardConfig("shard-2"),
        ]);
        const health = new ShardHealth(manager);

        const result = await health.health();
        expect(result.healthy).toBe(true);
        expect(result.shards.length).toBe(3);
        expect(result.upShards.length).toBe(3);
        expect(result.downShards.length).toBe(0);
        expect(result.checkedAt).toBeDefined();

        manager.shutdown();
    });

    it("health reports down shards when a pool fails", async () => {
        mockQuery.mockRejectedValueOnce(new Error("connection refused"));

        const manager = new ShardManager([
            makeShardConfig("shard-0"),
            makeShardConfig("shard-1"),
        ]);
        const health = new ShardHealth(manager);

        const result = await health.health();
        expect(result.healthy).toBe(false);
        expect(result.downShards.length).toBe(1);
        expect(result.upShards.length).toBe(1);
        expect(result.downShards[0]).toBe("shard-0");
        expect(result.shards[0].status).toBe("down");
        expect(result.shards[0].error).toBe("connection refused");

        manager.shutdown();
    });

    it("isShardHealthy returns boolean", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        expect(await health.isShardHealthy("shard-0")).toBe(true);
        expect(await health.isShardHealthy("unknown")).toBe(false);

        manager.shutdown();
    });

    it("tracks consecutive failures", async () => {
        // First call fails
        mockQuery.mockRejectedValueOnce(new Error("fail 1"));

        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        await health.shardHealth("shard-0");
        expect(health.getConsecutiveFailures("shard-0")).toBe(1);
        expect(health.getLastError("shard-0")).toBe("fail 1");

        // Second call succeeds
        mockQuery.mockResolvedValue({ rows: [1], rowCount: 1 });
        await health.shardHealth("shard-0");
        expect(health.getConsecutiveFailures("shard-0")).toBe(0);
        expect(health.getLastError("shard-0")).toBeUndefined();

        manager.shutdown();
    });

    it("records lastCheckedAt on success", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        await health.shardHealth("shard-0");
        expect(health.getLastCheckedAt("shard-0")).toBeDefined();

        manager.shutdown();
    });

    it("records lastCheckedAt on failure", async () => {
        mockQuery.mockRejectedValueOnce(new Error("fail"));

        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        await health.shardHealth("shard-0");
        expect(health.getLastCheckedAt("shard-0")).toBeDefined();

        manager.shutdown();
    });
});

describe("Database sharding health integration", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("db.sharding.health() returns aggregate health", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("shard-0"),
                    makeShardConfig("shard-1"),
                ],
            },
        });

        const result = await db.sharding.health();
        expect(result.healthy).toBe(true);
        expect(result.shards.length).toBe(2);
        expect(result.upShards.length).toBe(2);
        expect(result.downShards.length).toBe(0);

        await db.sharding.shutdown();
    });

    it("db.sharding.shardHealth() returns per-shard health", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        const result = await db.sharding.shardHealth("shard-0");
        expect(result).toBeDefined();
        expect(result?.shardId).toBe("shard-0");
        expect(result?.status).toBe("up");

        await db.sharding.shutdown();
    });

    it("db.sharding.shardHealth() returns undefined for unknown shard", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        const result = await db.sharding.shardHealth("unknown");
        expect(result).toBeUndefined();

        await db.sharding.shutdown();
    });

    it("db.sharding.health() when sharding disabled returns empty healthy result", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        const result = await db.sharding.health();
        expect(result.healthy).toBe(true);
        expect(result.shards.length).toBe(0);
    });

    it("db.sharding.shardHealth() when sharding disabled returns undefined", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        const result = await db.sharding.shardHealth("any");
        expect(result).toBeUndefined();
    });

    it("shard info includes consecutiveFailures after failure", async () => {
        mockQuery.mockRejectedValueOnce(new Error("connection lost"));

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        await db.sharding.shardHealth("shard-0");

        const shard = db.sharding.getShard("shard-0");
        expect(shard?.consecutiveFailures).toBe(1);
        expect(shard?.lastError).toBe("connection lost");
        expect(shard?.lastCheckedAt).toBeDefined();

        await db.sharding.shutdown();
    });
});
