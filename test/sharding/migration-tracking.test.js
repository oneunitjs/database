import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import { ShardMigrationTracker } from "../../src/sharding/shard-migration.js";
import { ShardManager } from "../../src/sharding/shard-manager.js";

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
}

function makeShardConfig(id) {
    return {
        id,
        connection: { host: "localhost", port: 5432, database: id, user: "test" },
    };
}

function makeManager(shardIds) {
    return new ShardManager(shardIds.map((id) => makeShardConfig(id)));
}

describe("ShardMigrationTracker unit tests", () => {
    beforeEach(() => {
        resetMocks();
        mockConnect.mockResolvedValue({
            query: mockQuery,
            release: mockRelease,
        });
    });

    afterEach(() => {
        resetMocks();
    });

    it("initialize creates tracking table on all shards", async () => {
        const manager = makeManager(["shard-0", "shard-1"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.initialize();

        // Should have created the table on each shard (CREATE TABLE IF NOT EXISTS)
        const createTableCalls = mockQuery.mock.calls.filter(
            (c) => typeof c[0] === "string" && String(c[0]).includes("CREATE TABLE IF NOT EXISTS shard_migrations"),
        );
        expect(createTableCalls.length).toBe(2);
    });

    it("initialize is idempotent", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.initialize();
        await tracker.initialize();

        const createTableCalls = mockQuery.mock.calls.filter(
            (c) => typeof c[0] === "string" && String(c[0]).includes("CREATE TABLE IF NOT EXISTS shard_migrations"),
        );
        // Should only create once per shard due to idempotency
        expect(createTableCalls.length).toBe(1);
    });

    it("initShard is idempotent", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.initShard("shard-0");
        await tracker.initShard("shard-0");

        const createTableCalls = mockQuery.mock.calls.filter(
            (c) => typeof c[0] === "string" && String(c[0]).includes("CREATE TABLE IF NOT EXISTS shard_migrations"),
        );
        expect(createTableCalls.length).toBe(1);
    });

    it("loadShardState loads persisted records from a shard", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({
            rows: [
                { migration_id: "mig-1", status: "applied", applied_at: "2026-01-01T00:00:00Z", error: null },
                { migration_id: "mig-2", status: "failed", applied_at: null, error: "connection lost" },
            ],
            rowCount: 2,
        });

        await tracker.loadShardState("shard-0");

        const status = tracker.getMigrationStatus("mig-1");
        expect(status).toBeDefined();
        expect(status.shards["shard-0"]).toBe("applied");

        const status2 = tracker.getMigrationStatus("mig-2");
        expect(status2).toBeDefined();
        expect(status2.shards["shard-0"]).toBe("failed");
    });

    it("initialize loads persisted state on all shards", async () => {
        const manager = makeManager(["shard-0", "shard-1"]);
        const tracker = new ShardMigrationTracker(manager);

        let callCount = 0;
        mockQuery.mockImplementation((sql) => {
            callCount++;
            if (typeof sql === "string" && String(sql).includes("SELECT migration_id, status")) {
                return Promise.resolve({
                    rows: [{ migration_id: "mig-1", status: "applied", applied_at: null, error: null }],
                    rowCount: 1,
                });
            }
            return Promise.resolve({ rows: [], rowCount: 0 });
        });

        await tracker.initialize();

        const status = tracker.getMigrationStatus("mig-1");
        expect(status).toBeDefined();
        expect(status.shards["shard-0"]).toBe("applied");
        expect(status.shards["shard-1"]).toBe("applied");
    });

    it("recordMigration updates in-memory cache and persists", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        await tracker.initShard("shard-0");

        await tracker.recordMigration("shard-0", "mig-1", "applied");

        const status = tracker.getMigrationStatus("mig-1");
        expect(status).toBeDefined();
        expect(status.shards["shard-0"]).toBe("applied");
        expect(status.allApplied).toBe(true);
    });

    it("runMigrationOnAllShards initializes tracking table before running", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        const result = await tracker.runMigrationOnAllShards("mig-1", "CREATE TABLE test (id INT)");

        expect(result.migrationId).toBe("mig-1");
        expect(result.allApplied).toBe(true);

        // Should have created the shard_migrations table
        const createTableCalls = mockQuery.mock.calls.filter(
            (c) => typeof c[0] === "string" && String(c[0]).includes("CREATE TABLE IF NOT EXISTS shard_migrations"),
        );
        expect(createTableCalls.length).toBeGreaterThan(0);
    });

    it("throws on initialization failure when a shard is unavailable", async () => {
        const manager = makeManager(["shard-0", "shard-1"]);
        const tracker = new ShardMigrationTracker(manager);

        // shard-0 succeeds, shard-1 fails
        let callCount = 0;
        mockQuery.mockImplementation((sql) => {
            callCount++;
            if (callCount > 2) {
                return Promise.reject(new Error("connection refused"));
            }
            return Promise.resolve({ rows: [], rowCount: 0 });
        });

        await expect(tracker.initialize()).rejects.toThrow();
    });

    it("concurrent initialize calls are safe", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        // Call initialize multiple times concurrently
        await Promise.all([tracker.initialize(), tracker.initialize(), tracker.initialize()]);

        const createTableCalls = mockQuery.mock.calls.filter(
            (c) => typeof c[0] === "string" && String(c[0]).includes("CREATE TABLE IF NOT EXISTS shard_migrations"),
        );
        // Should only create once due to per-shard lock
        expect(createTableCalls.length).toBe(1);
    });

    it("getMigrationStatus reports partial failures", async () => {
        const manager = makeManager(["shard-0", "shard-1"]);
        const tracker = new ShardMigrationTracker(manager);

        // Manually set up mixed state
        await tracker.recordMigration("shard-0", "mig-1", "applied");
        await tracker.recordMigration("shard-1", "mig-1", "failed");

        const status = tracker.getMigrationStatus("mig-1");
        expect(status).toBeDefined();
        expect(status.allApplied).toBe(false);
        expect(status.anyFailed).toBe(true);
        expect(status.shards["shard-0"]).toBe("applied");
        expect(status.shards["shard-1"]).toBe("failed");
    });

    it("existing APIs remain compatible", async () => {
        const manager = makeManager(["shard-0"]);
        const tracker = new ShardMigrationTracker(manager);

        mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

        // registerShard / unregisterShard
        tracker.registerShard("shard-2");
        expect(tracker.getShardMigrationStatus("shard-2")).toEqual([]);

        tracker.unregisterShard("shard-2");
        expect(tracker.getShardMigrationStatus("shard-2")).toEqual([]);

        // getMigrationStatus with no records returns pending for all shards
        const status = tracker.getMigrationStatus("nonexistent");
        expect(status).toBeDefined();
        expect(status.shards["shard-0"]).toBe("pending");
        expect(status.allApplied).toBe(false);
    });
});
