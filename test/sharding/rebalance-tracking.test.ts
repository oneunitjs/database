import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    ShardRebalanceTracker,
    createDatabase,
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
}

function makeShardConfig(id, connectionString) {
    return {
        id,
        connection: connectionString
            ? { connectionString }
            : { host: "localhost", port: 5432, database: id, user: "test" },
    };
}

describe("ShardRebalanceTracker standalone", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("createRebalance creates a planning rebalance", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 1000);

        expect(r.rebalanceId).toBeDefined();
        expect(r.sourceShardId).toBe("shard-0");
        expect(r.destinationShardId).toBe("shard-1");
        expect(r.totalKeys).toBe(1000);
        expect(r.movedKeys).toBe(0);
        expect(r.percentage).toBe(0);
        expect(r.status).toBe("planning");
        expect(r.errors).toEqual([]);
        expect(r.startedAt).toBeDefined();
        expect(r.updatedAt).toBeDefined();
    });

    it("updateProgress updates keys and percentage", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 1000);

        const updated = tracker.updateProgress(r.rebalanceId, {
            movedKeys: 250,
            status: "copying",
        });

        expect(updated).toBeDefined();
        expect(updated!.movedKeys).toBe(250);
        expect(updated!.percentage).toBe(25);
        expect(updated!.status).toBe("copying");
        expect(updated!.updatedAt).toBeDefined();
        expect(updated!.updatedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    });

    it("updateProgress on unknown id returns undefined", () => {
        const tracker = new ShardRebalanceTracker();
        expect(tracker.updateProgress("unknown-id", { movedKeys: 1 })).toBeUndefined();
    });

    it("updateProgress on terminal status is ignored", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 100);
        tracker.completeRebalance(r.rebalanceId);

        const updated = tracker.updateProgress(r.rebalanceId, { movedKeys: 50 });
        expect(updated!.status).toBe("completed");
        expect(updated!.movedKeys).toBe(0);
    });

    it("getRebalance returns undefined for unknown id", () => {
        const tracker = new ShardRebalanceTracker();
        expect(tracker.getRebalance("unknown")).toBeUndefined();
    });

    it("listRebalances returns all rebalances", () => {
        const tracker = new ShardRebalanceTracker();
        tracker.createRebalance("shard-0", "shard-1", 100);
        tracker.createRebalance("shard-1", "shard-2", 200);

        expect(tracker.listRebalances().length).toBe(2);
    });

    it("completeRebalance sets status and percentage to 100", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 100);
        tracker.updateProgress(r.rebalanceId, { movedKeys: 30 });

        const completed = tracker.completeRebalance(r.rebalanceId);
        expect(completed!.status).toBe("completed");
        expect(completed!.percentage).toBe(100);
    });

    it("completeRebalance on unknown id returns undefined", () => {
        const tracker = new ShardRebalanceTracker();
        expect(tracker.completeRebalance("unknown")).toBeUndefined();
    });

    it("completeRebalance on already terminal status is no-op", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 100);
        tracker.failRebalance(r.rebalanceId, "boom");

        const completed = tracker.completeRebalance(r.rebalanceId);
        expect(completed!.status).toBe("failed");
    });

    it("failRebalance records error", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 100);

        const failed = tracker.failRebalance(r.rebalanceId, "connection lost");
        expect(failed!.status).toBe("failed");
        expect(failed!.errors).toContain("connection lost");
    });

    it("cancelRebalance sets status to cancelled", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 100);

        const cancelled = tracker.cancelRebalance(r.rebalanceId);
        expect(cancelled!.status).toBe("cancelled");
    });

    it("clearCompleted removes terminal rebalances", () => {
        const tracker = new ShardRebalanceTracker();
        const r1 = tracker.createRebalance("shard-0", "shard-1", 100);
        const r2 = tracker.createRebalance("shard-1", "shard-2", 200);
        const r3 = tracker.createRebalance("shard-2", "shard-0", 300);

        tracker.completeRebalance(r1.rebalanceId);
        tracker.failRebalance(r2.rebalanceId, "error");

        expect(tracker.listRebalances().length).toBe(3);
        const removed = tracker.clearCompleted();
        expect(removed).toBe(2);
        expect(tracker.listRebalances().length).toBe(1);
        expect(tracker.getRebalance(r3.rebalanceId)).toBeDefined();
    });

    it("auto-transitions to completed when all keys accounted for", () => {
        const tracker = new ShardRebalanceTracker();
        const r = tracker.createRebalance("shard-0", "shard-1", 10);

        tracker.updateProgress(r.rebalanceId, { movedKeys: 5, failedKeys: 3, skippedKeys: 2 });
        expect(tracker.getRebalance(r.rebalanceId)!.status).toBe("completed");
    });
});

describe("db.sharding.rebalanceTracker integration", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("db.sharding.rebalanceTracker is available", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
            },
        });

        expect(db.sharding.rebalanceTracker).toBeDefined();
    });

    it("rebalanceTracker works through db.sharding", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
            },
        });

        const r = db.sharding.rebalanceTracker.createRebalance("shard-0", "shard-1", 500);
        expect(r.sourceShardId).toBe("shard-0");
        expect(r.destinationShardId).toBe("shard-1");

        const updated = db.sharding.rebalanceTracker.updateProgress(r.rebalanceId, {
            movedKeys: 125,
        });
        expect(updated!.percentage).toBe(25);
    });

    it("rebalanceTracker is independent of sharding enabled state", () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        expect(db.sharding.rebalanceTracker).toBeDefined();
        const r = db.sharding.rebalanceTracker.createRebalance("shard-0", "shard-1", 100);
        expect(r.rebalanceId).toBeDefined();
    });
});
