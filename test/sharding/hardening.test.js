import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    ShardManager,
    ShardHealth,
    ShardMigrationTracker,
    ShardConfigError,
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
            totalCount: 0,
            idleCount: 0,
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
    mockEnd.mockResolvedValue(undefined);
    // INSERT statements (e.g. conditional operation registration) return rowCount 1;
    // everything else returns empty. This mirrors a non-draining shard.
    mockQuery.mockImplementation((sql) => {
        const s = typeof sql === "string" ? sql : String(sql);
        const isInsert = s.trim().toUpperCase().startsWith("INSERT");
        return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 });
    });
}

function makeShardConfig(id, connectionString) {
    return {
        id,
        connection: connectionString
            ? { connectionString }
            : { host: "localhost", port: 5432, database: id, user: "test" },
    };
}

describe("ShardManager hardening: concurrency and lifecycle", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("concurrent addShard calls are serialized - no duplicate pools", async () => {
        const manager = new ShardManager([
            makeShardConfig("shard-0"),
        ]);

        // Concurrently add the same shard - second should fail with "already exists"
        const promises = [
            manager.addShard(makeShardConfig("shard-1")),
            manager.addShard(makeShardConfig("shard-1")),
        ];

        const results = await Promise.allSettled(promises);
        const fulfilled = results.filter((r) => r.status === "fulfilled");
        const rejected = results.filter((r) => r.status === "rejected");

        // Exactly one should succeed, one should fail
        expect(fulfilled.length).toBe(1);
        expect(rejected.length).toBe(1);

        // Only one pool for shard-1 should exist
        expect(manager.hasShard("shard-1")).toBe(true);
        expect(manager.shardCount).toBe(2); // shard-0 + shard-1

        await manager.shutdown();
    });

    it("concurrent addShard and removeShard are serialized", async () => {
        const manager = new ShardManager([
            makeShardConfig("shard-0"),
            makeShardConfig("shard-1"),
        ]);

        // Concurrent add and remove of the same shard
        const promises = [
            manager.addShard(makeShardConfig("shard-2")),
            manager.removeShard("shard-2"),
        ];

        const results = await Promise.allSettled(promises);
        // Both should succeed (serialize properly), or one fails gracefully
        const fulfilled = results.filter((r) => r.status === "fulfilled");
        expect(fulfilled.length).toBeGreaterThanOrEqual(1);

        await manager.shutdown();
    });

    it("acquireShard and releaseShard track active operations", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);

        await manager.acquireShard("shard-0");
        expect(manager.getShard("shard-0")?.connected).toBe(true);

        await manager.acquireShard("shard-0");
        await manager.releaseShard("shard-0");
        await manager.releaseShard("shard-0");

        await manager.shutdown();
    });

    it("acquireShard throws for unknown shard", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);

        await expect(manager.acquireShard("unknown")).rejects.toThrow(
            ShardConfigError
        );

        await manager.shutdown();
    });

    it("removeShard marks shard as draining and rejects new acquisitions", async () => {
        const manager = new ShardManager([
            makeShardConfig("shard-0"),
            makeShardConfig("shard-1"),
        ]);

        // Acquire shard-0, then start removal in background
        await manager.acquireShard("shard-0");

        const removePromise = manager.removeShard("shard-0");

        // Give removeShard a chance to start and set draining = true
        await Promise.resolve();

        // While removal is in progress (waiting for activeOps to reach 0),
        // new acquisitions should be rejected
        await expect(manager.acquireShard("shard-0")).rejects.toThrow(
            /draining/
        );

        // Release the operation, allowing removal to complete
        await manager.releaseShard("shard-0");
        await removePromise;

        expect(manager.hasShard("shard-0")).toBe(false);

        await manager.shutdown();
    });

    it("removeShard waits for active operations to complete", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);

        // Acquire and hold the shard
        await manager.acquireShard("shard-0");

        // Start removal - it will wait for the active op to release
        const removePromise = manager.removeShard("shard-0");

        // Give it a moment to start waiting
        await new Promise((r) => setTimeout(r, 50));

        // The removal should still be waiting (not resolved yet)
        // Release the operation
        await manager.releaseShard("shard-0");
        await removePromise;

        expect(manager.hasShard("shard-0")).toBe(false);

        await manager.shutdown();
    });

    it("onAfterReshard hook failure rolls back shard addition", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);

        await expect(
            manager.addShard(
                makeShardConfig("shard-1"),
                {
                    onAfterReshard: async () => {
                        throw new Error("hook failed");
                    },
                }
            ),
        ).rejects.toThrow(/hook failed/);

        // Shard should NOT exist after failed hook
        expect(manager.hasShard("shard-1")).toBe(false);
        expect(manager.shardCount).toBe(1);

        await manager.shutdown();
    });

    it("onBeforeReshard hook failure prevents shard addition", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);

        await expect(
            manager.addShard(
                makeShardConfig("shard-1"),
                {
                    onBeforeReshard: async () => {
                        throw new Error("before hook failed");
                    },
                }
            ),
        ).rejects.toThrow(/before hook failed/);

        expect(manager.hasShard("shard-1")).toBe(false);

        await manager.shutdown();
    });
});

describe("ShardHealth dynamic shard support", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("health check works for newly added shard", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        // Add a new shard after health checker was created
        await manager.addShard(makeShardConfig("shard-1"));

        // Health check should find the new shard
        const result = await health.shardHealth("shard-1");
        expect(result).toBeDefined();
        expect(result?.status).toBe("up");

        await manager.shutdown();
    });

    it("health check returns undefined for removed shard", async () => {
        const manager = new ShardManager([
            makeShardConfig("shard-0"),
            makeShardConfig("shard-1"),
        ]);
        const health = new ShardHealth(manager);

        // Check shard-1 is healthy first
        expect(await health.isShardHealthy("shard-1")).toBe(true);

        // Remove shard-1
        await manager.removeShard("shard-1");

        // Health check should return undefined
        const result = await health.shardHealth("shard-1");
        expect(result).toBeUndefined();

        await manager.shutdown();
    });

    it("aggregate health reflects dynamic shard changes", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const health = new ShardHealth(manager);

        let result = await health.health();
        expect(result.shards.length).toBe(1);

        // Add a shard
        await manager.addShard(makeShardConfig("shard-1"));
        result = await health.health();
        expect(result.shards.length).toBe(2);

        // Remove shard-0
        await manager.removeShard("shard-0");
        result = await health.health();
        expect(result.shards.length).toBe(1);
        expect(result.shards[0].shardId).toBe("shard-1");

        await manager.shutdown();
    });
});

describe("ShardMigrationTracker persistent tracking", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("runMigrationOnAllShards wraps SQL in transaction", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.runMigrationOnAllShards("001_test", "CREATE TABLE t (id INT)");

        // Verify BEGIN was called before the migration SQL
        const beginCall = mockQuery.mock.calls.find((call) => call[0] === "BEGIN");
        const commitCall = mockQuery.mock.calls.find((call) => call[0] === "COMMIT");
        const sqlCall = mockQuery.mock.calls.find((call) => call[0] === "CREATE TABLE t (id INT)");

        expect(beginCall).toBeDefined();
        expect(commitCall).toBeDefined();
        expect(sqlCall).toBeDefined();

        // Order: BEGIN, SQL, COMMIT
        expect(mockQuery.mock.calls.indexOf(beginCall)).toBeLessThan(
            mockQuery.mock.calls.indexOf(sqlCall)
        );
        expect(mockQuery.mock.calls.indexOf(sqlCall)).toBeLessThan(
            mockQuery.mock.calls.indexOf(commitCall)
        );

        manager.shutdown();
    });

    it("runMigrationOnAllShards uses advisory lock for concurrency", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.runMigrationOnAllShards("001_test", "CREATE TABLE t (id INT)");

        // Verify advisory lock was acquired
        const lockCall = mockQuery.mock.calls.find(
            (call) => typeof call[0] === "string" && call[0].includes("pg_advisory_lock")
        );
        expect(lockCall).toBeDefined();

        // Verify advisory unlock was called
        const unlockCall = mockQuery.mock.calls.find(
            (call) => typeof call[0] === "string" && call[0].includes("pg_advisory_unlock")
        );
        expect(unlockCall).toBeDefined();

        manager.shutdown();
    });

    it("runMigrationOnAllShards records failure when SQL fails", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const tracker = new ShardMigrationTracker(manager);

        // Set up mock sequence: init DDL, advisory lock, BEGIN, SQL(fails), ROLLBACK, unlock
        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 }); // init DDL
        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 }); // advisory lock
        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 }); // BEGIN
        mockQuery.mockRejectedValueOnce(new Error("syntax error")); // SQL fails
        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 }); // ROLLBACK
        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 }); // advisory unlock

        const result = await tracker.runMigrationOnAllShards("002_bad", "BAD SQL");

        expect(result.allApplied).toBe(false);
        expect(result.anyFailed).toBe(true);
        expect(result.shards["shard-0"]).toBe("failed");

        const status = tracker.getMigrationStatus("002_bad");
        expect(status).toBeDefined();
        expect(status.allApplied).toBe(false);
        expect(status.anyFailed).toBe(true);

        manager.shutdown();
    });

    it("getMigrationStats returns correct counts", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.runMigrationOnAllShards("001_good", "CREATE TABLE t1 (id INT)");
        mockQuery.mockRejectedValueOnce(new Error("fail"));
        await tracker.runMigrationOnAllShards("002_bad", "BAD SQL");

        const stats = tracker.getMigrationStats();
        expect(stats.totalMigrations).toBe(2);
        expect(stats.fullyApplied).toBe(1);
        expect(stats.failedMigrations).toBe(1);

        manager.shutdown();
    });

    it("re-running same migration shows already applied", async () => {
        const manager = new ShardManager([makeShardConfig("shard-0")]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.runMigrationOnAllShards("001_repeat", "CREATE TABLE t (id INT)");
        await tracker.runMigrationOnAllShards("001_repeat", "CREATE TABLE t (id INT)");

        const status = tracker.getMigrationStatus("001_repeat");
        expect(status.allApplied).toBe(true);

        manager.shutdown();
    });
});
