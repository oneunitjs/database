import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    createDatabase,
    HashShardStrategy,
    DirectoryShardStrategy,
    createShardStrategy,
    ShardNotFoundError,
    ShardingDisabledError,
    ShardConfigError,
    isShardNotFoundError,
    isShardingDisabledError,
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

describe("sharding configuration", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("creates database without sharding when option omitted", () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        expect(db.sharding.enabled).toBe(false);
        expect(db.sharding.shards).toEqual([]);
        expect(db.sharding.getShard("shard-1")).toBeUndefined();
        expect(db.sharding.isClosed).toBe(false);
    });

    it("creates database with sharding disabled", () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: {
                enabled: false,
                strategy: "hash",
                shards: [],
            },
        });

        expect(db.sharding.enabled).toBe(false);
        expect(db.sharding.strategy).toBeUndefined();
        expect(db.sharding.shards).toEqual([]);
    });

    it("creates database with sharding enabled and hash strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("shard-0", "postgresql://localhost:5432/shard0"),
                    makeShardConfig("shard-1", "postgresql://localhost:5432/shard1"),
                    makeShardConfig("shard-2", "postgresql://localhost:5432/shard2"),
                ],
            },
        });

        expect(db.sharding.enabled).toBe(true);
        expect(db.sharding.strategy).toBe("hash");
        expect(db.sharding.shards.length).toBe(3);
        expect(db.sharding.getShard("shard-0").id).toBe("shard-0");
        expect(db.sharding.getShard("shard-1").id).toBe("shard-1");
        expect(db.sharding.getShard("shard-2").id).toBe("shard-2");
    });

    it("creates database with sharding enabled and directory strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("shard-a"), makeShardConfig("shard-b")],
                routing: {
                    directory: { "user-1": "shard-a", "user-2": "shard-b" },
                },
            },
        });

        expect(db.sharding.enabled).toBe(true);
        expect(db.sharding.strategy).toBe("directory");
        expect(db.sharding.shards.length).toBe(2);
    });

    it("throws when sharding enabled with no shards", () => {
        expect(() =>
            createDatabase({
                sharding: {
                    enabled: true,
                    strategy: "hash",
                    shards: [],
                },
            }),
        ).toThrow();
    });

    it("throws when sharding enabled with duplicate shard IDs", () => {
        expect(() =>
            createDatabase({
                sharding: {
                    enabled: true,
                    strategy: "hash",
                    shards: [
                        makeShardConfig("shard-1"),
                        makeShardConfig("shard-1"),
                    ],
                },
            }),
        ).toThrow();
    });

    it("preserves backwards compatibility for existing configs", () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            max: 10,
            application_name: "test-app",
        });

        expect(db.pool).toBeDefined();
        expect(db.query).toBeDefined();
        expect(db.transaction).toBeDefined();
        expect(db.shutdown).toBeDefined();
        expect(db.sharding.enabled).toBe(false);
    });
});

describe("shard strategies", () => {
    it("HashShardStrategy resolves deterministically", () => {
        const strategy = new HashShardStrategy();
        const shardIds = ["shard-0", "shard-1", "shard-2"];

        const result1 = strategy.resolveShard("user-42", {
            shardKey: "user-42",
            strategy: "hash",
            shardIds,
        });
        const result2 = strategy.resolveShard("user-42", {
            shardKey: "user-42",
            strategy: "hash",
            shardIds,
        });

        expect(result1).toBe(result2);
        expect(shardIds).toContain(result1);
    });

    it("HashShardStrategy distributes across shards", () => {
        const strategy = new HashShardStrategy();
        const shardIds = ["shard-0", "shard-1", "shard-2"];
        const results = new Set();

        for (let i = 0; i < 100; i++) {
            results.add(
                strategy.resolveShard(`user-${i}`, {
                    shardKey: `user-${i}`,
                    strategy: "hash",
                    shardIds,
                }),
            );
        }

        expect(results.size).toBeGreaterThan(1);
    });

    it("HashShardStrategy throws with no shards", () => {
        const strategy = new HashShardStrategy();
        expect(() =>
            strategy.resolveShard("key", {
                shardKey: "key",
                strategy: "hash",
                shardIds: [],
            }),
        ).toThrow();
    });

    it("DirectoryShardStrategy resolves from mapping", () => {
        const strategy = new DirectoryShardStrategy();
        const result = strategy.resolveShard("user-1", {
            shardKey: "user-1",
            strategy: "directory",
            shardIds: ["shard-a", "shard-b"],
            directory: { "user-1": "shard-a", "user-2": "shard-b" },
        });

        expect(result).toBe("shard-a");
    });

    it("DirectoryShardStrategy throws when key not in directory", () => {
        const strategy = new DirectoryShardStrategy();
        expect(() =>
            strategy.resolveShard("unknown-user", {
                shardKey: "unknown-user",
                strategy: "directory",
                shardIds: ["shard-a", "shard-b"],
                directory: { "user-1": "shard-a" },
            }),
        ).toThrow();
    });

    it("DirectoryShardStrategy throws when no directory provided", () => {
        const strategy = new DirectoryShardStrategy();
        expect(() =>
            strategy.resolveShard("key", {
                shardKey: "key",
                strategy: "directory",
                shardIds: ["shard-a"],
            }),
        ).toThrow();
    });

    it("createShardStrategy factory creates correct strategies", () => {
        expect(createShardStrategy("hash")).toBeInstanceOf(HashShardStrategy);
        expect(createShardStrategy("directory")).toBeInstanceOf(
            DirectoryShardStrategy,
        );
    });

    it("createShardStrategy throws for unknown strategy", () => {
        expect(() =>
            createShardStrategy("unknown-strategy"),
        ).toThrow();
    });
});

describe("shard routing", () => {
    beforeEach(() => {
        resetMocks();
    });

    it("getShardForKey routes to consistent shard", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("shard-0", "postgresql://localhost:5432/shard0"),
                    makeShardConfig("shard-1", "postgresql://localhost:5432/shard1"),
                ],
            },
        });

        const shard1 = db.sharding.getShardForKey("user-42");
        const shard2 = db.sharding.getShardForKey("user-42");

        expect(shard1.id).toBe(shard2.id);
        expect(shard1.connected).toBe(true);
    });

    it("getShardForKey throws when sharding disabled", () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        expect(() => db.sharding.getShardForKey("user-1")).toThrow(
            ShardingDisabledError,
        );
    });

    it("getShard returns undefined for unknown shard", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        expect(db.sharding.getShard("unknown-shard")).toBeUndefined();
    });

    it("getShard returns shard info for known shard", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        const shard = db.sharding.getShard("shard-0");
        expect(shard.id).toBe("shard-0");
        expect(shard.connected).toBe(true);
    });

    it("routes different keys to potentially different shards", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("shard-0", "postgresql://localhost:5432/shard0"),
                    makeShardConfig("shard-1", "postgresql://localhost:5432/shard1"),
                    makeShardConfig("shard-2", "postgresql://localhost:5432/shard2"),
                ],
            },
        });

        const shardIds = new Set();
        for (let i = 0; i < 50; i++) {
            shardIds.add(db.sharding.getShardForKey(`user-${i}`).id);
        }
        expect(shardIds.size).toBeGreaterThan(1);
    });
});

describe("sharding error classes", () => {
    it("ShardNotFoundError has correct code", () => {
        const err = new ShardNotFoundError("bad-shard");
        expect(err.code).toBe("SHARD_NOT_FOUND");
        expect(err.message).toContain("bad-shard");
        expect(isShardNotFoundError(err)).toBe(true);
    });

    it("ShardingDisabledError has correct code", () => {
        const err = new ShardingDisabledError();
        expect(err.code).toBe("SHARDING_DISABLED");
        expect(isShardingDisabledError(err)).toBe(true);
    });

    it("ShardConfigError wraps config errors", () => {
        const err = new ShardConfigError("bad config");
        expect(err.code).toBe("SHARD_CONFIG_ERROR");
        expect(err.message).toBe("bad config");
    });

    it("isShardNotFoundError returns false for non-matching errors", () => {
        expect(isShardNotFoundError(new Error("other"))).toBe(false);
    });
});
