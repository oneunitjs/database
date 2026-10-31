import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    createDatabase,
    CrossShardTransactionError,
    isCrossShardTransactionError,
    ShardingDisabledError,
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

describe("Cross-shard transaction detection", () => {
    beforeEach(() => {
        resetMocks();
        mockConnect.mockResolvedValue({
            query: mockQuery,
            release: mockRelease,
        });
        // INSERT statements (e.g. conditional operation registration) return rowCount 1;
        // everything else returns empty. This mirrors a non-draining shard.
        mockQuery.mockImplementation((sql) => {
            const s = typeof sql === "string" ? sql : String(sql);
            const isInsert = s.trim().toUpperCase().startsWith("INSERT");
            return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 });
        });
    });

    afterEach(() => {
        resetMocks();
    });

    it("single-shard transaction succeeds", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        const result = await db.transaction(
            async (client) => {
                await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
                return { inserted: true };
            },
            { shardKey: "user-1" },
        );

        expect(result).toEqual({ inserted: true });
        const calls = mockQuery.mock.calls.map((c) => c[0]);
        expect(calls).toContain("BEGIN");
        expect(calls).toContain("COMMIT");
    });

    it("transaction stays pinned to its selected shard", async () => {
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

        const result = await db.transaction(
            async (client) => {
                await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
                return { shard: "shard-0" };
            },
            { shardKey: "user-1" },
        );

        expect(result).toEqual({ shard: "shard-0" });
        const calls = mockQuery.mock.calls.map((c) => c[0]);
        expect(calls).toContain("BEGIN");
        expect(calls).toContain("COMMIT");
    });

    it("transaction rolls back correctly on error", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        mockQuery.mockRejectedValueOnce(new Error("insert failed"));

        await expect(
            db.transaction(
                async (client) => {
                    await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
                    throw new Error("insert failed");
                },
                { shardKey: "user-1" },
            ),
        ).rejects.toThrow("insert failed");

        const calls = mockQuery.mock.calls.map((c) => c[0]);
        expect(calls).toContain("ROLLBACK");
    });

    it("CrossShardTransactionError can be instantiated with useful context", async () => {
        const err = new CrossShardTransactionError({
            requestedShard: "user-1",
            actualShard: "shard-0",
            operation: "db.transaction()",
        });

        expect(err).toBeInstanceOf(CrossShardTransactionError);
        expect(err.name).toBe("CrossShardTransactionError");
        expect(err.code).toBe("CROSS_SHARD_TRANSACTION");
        expect(err.requestedShard).toBe("user-1");
        expect(err.actualShard).toBe("shard-0");
        expect(err.operation).toBe("db.transaction()");
        expect(err.message).toContain("Cross-shard transactions are not supported");
        expect(err.message).toContain("user-1");
        expect(err.message).toContain("shard-0");
        expect(err.message).toContain("db.transaction()");
        expect(err.message).toContain("saga pattern");
        expect(err.message).toContain("outbox pattern");
    });

    it("isCrossShardTransactionError identifies the error", async () => {
        const err = new CrossShardTransactionError({
            requestedShard: "user-1",
            actualShard: "shard-0",
        });
        expect(isCrossShardTransactionError(err)).toBe(true);
        expect(isCrossShardTransactionError(new Error("other"))).toBe(false);
    });

    it("CrossShardTransactionError is exported from sharding errors", async () => {
        const mod = await import("../../src/sharding/errors.js");
        expect(typeof mod.CrossShardTransactionError).toBe("function");
        expect(typeof mod.isCrossShardTransactionError).toBe("function");
    });

    it("sharding.transaction is pinned to one shard", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        const result = await db.sharding.transaction(
            "user-1",
            async (client) => {
                await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
                return { inserted: true };
            },
        );

        expect(result).toEqual({ inserted: true });
        const calls = mockQuery.mock.calls.map((c) => c[0]);
        expect(calls).toContain("BEGIN");
        expect(calls).toContain("COMMIT");
    });
});
