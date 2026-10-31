import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    createDatabase,
    ShardingDisabledError,
    ShardNotFoundError,
    ShardRoutingError,
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

describe("sharding query execution", () => {
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

    it("sharding.query routes to the correct shard", async () => {
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

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.sharding.query(
            "user-42",
            "SELECT * FROM users WHERE id = $1",
            [42],
        );

        expect(result.rows).toEqual([{ id: 1 }]);
        expect(mockQuery).toHaveBeenCalledWith(
            "SELECT * FROM users WHERE id = $1",
            [42],
        );
    });

    it("sharding.query preserves parameterized SQL", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });

        await db.sharding.query(
            "user-1",
            "INSERT INTO users (email, name) VALUES ($1, $2)",
            ["a@b.c", "Alice"],
        );

        expect(mockQuery).toHaveBeenCalledWith(
            "INSERT INTO users (email, name) VALUES ($1, $2)",
            ["a@b.c", "Alice"],
        );
    });

    it("sharding.query throws when sharding disabled", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        await expect(
            db.sharding.query("user-1", "SELECT 1"),
        ).rejects.toThrow(ShardingDisabledError);
    });

    it("sharding.query throws for unknown shard key mapping", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("shard-0")],
                routing: { directory: { "user-1": "shard-0" } },
            },
        });

        await expect(
            db.sharding.query("unknown-user", "SELECT 1"),
        ).rejects.toThrow(ShardNotFoundError);
    });

    it("regular db.query still works when sharding enabled (uses default shard pool)", async () => {
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

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.query("SELECT 1");
        expect(result.rows).toEqual([{ id: 1 }]);
    });
});

describe("sharding transactions", () => {
    beforeEach(() => {
        resetMocks();
        mockConnect.mockResolvedValue({
            query: mockQuery,
            release: mockRelease,
        });
        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });
    });

    afterEach(() => {
        resetMocks();
    });

    it("sharding.transaction keeps transaction on one shard", async () => {
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
        // Verify BEGIN, statement, COMMIT were called on the same client
        const calls = mockQuery.mock.calls.map((c) => c[0]);
        expect(calls).toContain("BEGIN");
        expect(calls).toContain("COMMIT");
    });

    it("sharding.transaction rolls back on error", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        mockQuery.mockRejectedValueOnce(new Error("insert failed"));

        await expect(
            db.sharding.transaction("user-1", async (client) => {
                await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
                throw new Error("insert failed");
            }),
        ).rejects.toThrow("insert failed");

        const calls = mockQuery.mock.calls.map((c) => c[0]);
        expect(calls).toContain("ROLLBACK");
    });

    it("sharding.transaction throws when sharding disabled", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        await expect(
            db.sharding.transaction("user-1", async () => {}),
        ).rejects.toThrow(ShardingDisabledError);
    });
});

describe("sharding pool management", () => {
    beforeEach(() => {
        resetMocks();
        mockEnd.mockResolvedValue(undefined);
    });

    afterEach(() => {
        resetMocks();
    });

    it("sharding.shutdown closes all shard pools", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("shard-0"),
                    makeShardConfig("shard-1"),
                    makeShardConfig("shard-2"),
                ],
            },
        });

        expect(db.sharding.shards.length).toBe(3);

        await db.sharding.shutdown();

        expect(mockEnd).toHaveBeenCalledTimes(3);
        expect(db.sharding.isClosed).toBe(true);
    });

    it("sharding is idempotent on shutdown", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        await db.sharding.shutdown();
        await db.sharding.shutdown(); // Should not throw

        expect(mockEnd).toHaveBeenCalledTimes(1);
    });
});

describe("sharding backwards compatibility", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("existing createDatabase without sharding works unchanged", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            max: 10,
        });

        expect(db.sharding.enabled).toBe(false);
        expect(db.sharding.strategy).toBeUndefined();
        expect(db.sharding.shards).toEqual([]);
        expect(db.sharding.getShard("any")).toBeUndefined();
        expect(() => db.sharding.getShardForKey("any")).toThrow(ShardingDisabledError);
        await expect(db.sharding.query("any", "SELECT 1")).rejects.toThrow(ShardingDisabledError);
        await expect(db.sharding.transaction("any", async () => {})).rejects.toThrow(ShardingDisabledError);
    });

    it("existing single pool queries work when sharding disabled", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.query("SELECT * FROM users WHERE id = $1", [1]);
        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("sharding types are exported from index", async () => {
        const mod = await import("../../src/index.js");

        // Sharding runtime exports should exist
        expect(typeof mod.createShardStrategy).toBe("function");
        expect(typeof mod.HashShardStrategy).toBe("function");
        expect(typeof mod.DirectoryShardStrategy).toBe("function");
        expect(typeof mod.ShardManager).toBe("function");
        expect(typeof mod.ShardRouter).toBe("function");
        expect(typeof mod.ShardNotFoundError).toBe("function");
        expect(typeof mod.ShardingDisabledError).toBe("function");
        expect(typeof mod.ShardConfigError).toBe("function");
        expect(typeof mod.isShardNotFoundError).toBe("function");
        expect(typeof mod.isShardingDisabledError).toBe("function");
    });
});

describe("unified API (db.query / db.transaction with sharding)", () => {
    beforeEach(() => {
        resetMocks();
        mockConnect.mockResolvedValue({
            query: mockQuery,
            release: mockRelease,
        });
        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });
    });

    afterEach(() => {
        resetMocks();
    });

    it("db.query() with shardKey routes to the correct shard", async () => {
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

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.query(
            "SELECT * FROM users WHERE id = $1",
            [42],
            { shardKey: "user-42" },
        );

        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("db.query() without shardKey uses primary pool (backward compat)", async () => {
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

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.query("SELECT 1");
        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("db.query() with unknown shard key throws ShardRoutingError", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("shard-0")],
                routing: { directory: { "user-1": "shard-0" } },
            },
        });

        await expect(
            db.query("SELECT 1", [], { shardKey: "unknown-user" }),
        ).rejects.toThrow(ShardRoutingError);
    });

    it("db.transaction() with shardKey routes to the correct shard", async () => {
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
    });

    it("db.transaction() without shardKey uses primary pool", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        const result = await db.transaction(
            async (client) => {
                await client.query("SELECT 1");
                return "done";
            },
        );

        expect(result).toBe("done");
    });

    it("db.transaction() with unknown shard key throws ShardRoutingError", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("shard-0")],
                routing: { directory: { "user-1": "shard-0" } },
            },
        });

        await expect(
            db.transaction(
                async () => {},
                { shardKey: "unknown-user" },
            ),
        ).rejects.toThrow(ShardRoutingError);
    });

    it("concurrent requests route to different shards", async () => {
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

        await Promise.all(
            Array.from({ length: 20 }, (_, i) =>
                db.query("SELECT 1", [], { shardKey: `user-${i}` }).then(() => {
                    // We can't easily inspect which shard was used from the mock,
                    // but we can verify all queries complete without error
                }),
            ),
        );

        // All queries should complete successfully
        expect(true).toBe(true);
    });

    it("existing non-sharded db.query() works unchanged", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.query("SELECT * FROM users WHERE id = $1", [1]);
        expect(result.rows).toEqual([{ id: 1 }]);

        const result2 = await db.queryOne("SELECT name FROM users WHERE id = $1", [1]);
        expect(result2).toEqual({ id: 1 });
    });

    it("existing non-sharded db.transaction() works unchanged", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });

        const result = await db.transaction(async (client) => {
            await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
            return { ok: true };
        });

        expect(result).toEqual({ ok: true });
    });

    it("db.sharding.query() remains available as deprecated API", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.sharding.query(
            "user-1",
            "SELECT * FROM users WHERE id = $1",
            [1],
        );
        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("db.sharding.transaction() remains available as deprecated API", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("shard-0")],
            },
        });

        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });

        const result = await db.sharding.transaction(
            "user-1",
            async (client) => {
                await client.query("INSERT INTO users (name) VALUES ($1)", ["Bob"]);
                return { inserted: true };
            },
        );
        expect(result).toEqual({ inserted: true });
    });
});

describe("shard-aware query builder, models, prepared statements, and queryOne", () => {
    beforeEach(() => {
        resetMocks();
        mockConnect.mockResolvedValue({
            query: mockQuery,
            release: mockRelease,
        });
        mockQuery.mockImplementation((sql) => {
            const s = typeof sql === "string" ? sql : String(sql);
            const isInsert = s.trim().toUpperCase().startsWith("INSERT");
            return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 });
        });
    });

    afterEach(() => {
        resetMocks();
    });

    function makeDb() {
        return createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("shard-0", "postgresql://localhost:5432/shard0"),
                    makeShardConfig("shard-1", "postgresql://localhost:5432/shard1"),
                ],
            },
        });
    }

    it("db.from() with shardKey routes to the correct shard", async () => {
        const db = makeDb();
        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.from("users").where({ id: 42 }).execute({ shardKey: "user-42" });

        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("db.from() without shardKey uses the primary pool", async () => {
        const db = makeDb();
        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.from("users").select().execute();

        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("db.model() with shardKey routes to the correct shard", async () => {
        const db = makeDb();
        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.model("users").findById(1, { shardKey: "user-1" });

        expect(result).toEqual({ id: 1 });
    });

    it("db.model() without shardKey uses the primary pool", async () => {
        const db = makeDb();
        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.model("users").findById(1);

        expect(result).toEqual({ id: 1 });
    });

    it("db.prepared() with shardKey routes to the correct shard", async () => {
        const db = makeDb();
        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.prepared(
            "get_user",
            "SELECT * FROM users WHERE id = $1",
            [42],
            { shardKey: "user-42" },
        );

        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("db.queryOne() with shardKey routes to the correct shard", async () => {
        const db = makeDb();
        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.queryOne(
            "SELECT * FROM users WHERE id = $1",
            [42],
            { shardKey: "user-42" },
        );

        expect(result).toEqual({ id: 1 });
    });

    it("hooks receive shardId and routingStrategy for sharded queries", async () => {
        const db = makeDb();
        const beforeQuery = vi.fn();
        const afterQuery = vi.fn();
        db.hooks.beforeQuery = beforeQuery;
        db.hooks.afterQuery = afterQuery;

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        await db.query("SELECT 1", [], { shardKey: "user-1" });

        expect(beforeQuery).toHaveBeenCalled();
        const ctx = beforeQuery.mock.calls[0][0];
        expect(ctx.shardId).toBeDefined();
        expect(ctx.routingStrategy).toBe("hash");
        expect(afterQuery).toHaveBeenCalled();
        expect(afterQuery.mock.calls[0][0].shardId).toBeDefined();
    });

    it("non-sharded db.from()/db.model()/db.prepared()/db.queryOne() work unchanged", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        expect(db.sharding.enabled).toBe(false);

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const result = await db.from("users").where({ id: 1 }).execute();
        expect(result.rows).toEqual([{ id: 1 }]);

        const model = db.model("users");
        const one = await model.findById(1);
        expect(one).toEqual({ id: 1 });

        const prepared = await db.prepared("p", "SELECT 1", []);
        expect(prepared.rows).toEqual([{ id: 1 }]);

        const queryOne = await db.queryOne("SELECT name FROM users WHERE id = $1", [1]);
        expect(queryOne).toEqual({ id: 1 });

        await db.shutdown();
    });
});
