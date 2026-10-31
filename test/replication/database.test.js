import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createDatabase } from "../../src/index.js";

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => {
            const queryImpl = (sql) => {
                const s = typeof sql === "string" ? sql : String(sql);
                const isInsert = s.trim().toUpperCase().startsWith("INSERT");
                return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 });
            };
            return {
                query: vi.fn(queryImpl),
                connect: vi.fn().mockResolvedValue({
                    query: vi.fn(queryImpl),
                    release: vi.fn(),
                }),
                end: vi.fn().mockResolvedValue(undefined),
                on: vi.fn(),
                totalCount: 0,
                idleCount: 0,
                waitingCount: 0,
            };
        }),
    };
});

function resetMocks() {
    vi.clearAllMocks();
}

describe("createDatabase with replication (mocked pg)", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("non-replicated config keeps db.replication.enabled false (backward compat)", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/db",
        });
        expect(db.replication.enabled).toBe(false);
        expect(db.replication.mode).toBe("primary");
        expect(db.replication.replicaEndpointIds).toEqual([]);
        await db.shutdown();
    });

    it("replicated config exposes primary + replicas and routes reads by preference", async () => {
        const db = createDatabase({
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: {
                    id: "primary",
                    connection: { connectionString: "postgresql://localhost:5432/db" },
                },
                replicas: [
                    {
                        id: "replica-0",
                        connection: { connectionString: "postgresql://localhost:5432/replica-0" },
                    },
                ],
            },
        });

        expect(db.replication.enabled).toBe(true);
        expect(db.replication.mode).toBe("replica-preferred");
        expect(db.replication.primaryEndpointId).toBe("primary");
        expect(db.replication.replicaEndpointIds).toEqual(["replica-0"]);

        await db.replication.health();

        // Default read preference follows the configured mode (replica-preferred)
        const defaultRead = db.replication.readEndpoint();
        expect(defaultRead.endpointId).toBe("replica-0");
        expect(defaultRead.role).toBe("replica");

        // Explicit primary overrides the mode
        const primaryRead = db.replication.readEndpoint("primary");
        expect(primaryRead.endpointId).toBe("primary");
        expect(primaryRead.role).toBe("primary");

        const replicaRead = db.replication.readEndpoint("replica-preferred");
        expect(replicaRead.endpointId).toBe("replica-0");
        expect(replicaRead.role).toBe("replica");

        await db.shutdown();
    });

    it("replica-only read endpoint throws when no eligible replica", async () => {
        const db = createDatabase({
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: {
                    id: "primary",
                    connection: { connectionString: "postgresql://localhost:5432/db" },
                },
                replicas: [],
            },
        });
        expect(() => db.replication.readEndpoint("replica-only")).toThrow();
        await db.shutdown();
    });

    it("replication health resolves without throwing", async () => {
        const db = createDatabase({
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: {
                    id: "primary",
                    connection: { connectionString: "postgresql://localhost:5432/db" },
                },
                replicas: [
                    {
                        id: "replica-0",
                        connection: { connectionString: "postgresql://localhost:5432/replica-0" },
                    },
                ],
            },
        });
        const health = await db.replication.health();
        expect(health.primary.endpointId).toBe("primary");
        expect(health.replicas.length).toBe(1);
        expect(health.replicas[0].lagUnknown).toBe(true);
        await db.shutdown();
    });

    it("db.query and db.transaction still work without replication (backward compat)", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/db",
        });
        const res = await db.query("SELECT 1");
        expect(res.rows).toEqual([]);
        const tx = await db.transaction(async (_client) => {
            return { ok: true };
        });
        expect(tx.ok).toBe(true);
        await db.shutdown();
    });

    it("sharded database with per-shard replication adds/removes replica endpoints", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    {
                        id: "shard-0",
                        connection: { connectionString: "postgresql://localhost:5432/shard-0" },
                        replication: {
                            enabled: true,
                            mode: "replica-preferred",
                            replicas: [
                                {
                                    id: "replica-0",
                                    connection: {
                                        connectionString: "postgresql://localhost:5432/shard-0-replica-0",
                                    },
                                },
                            ],
                        },
                    },
                ],
                defaultShardId: "shard-0",
            },
        });

        expect(db.sharding.enabled).toBe(true);
        // db.replication is the top-level (non-sharded) API -> disabled here
        expect(db.replication.enabled).toBe(false);

        // Read via shard key routes to shard-0; replica-preferred targets the shard replica
        await db.query("SELECT 1", [], { shardKey: "user-1", readPreference: "replica-preferred" });

        // Dynamically add shard-1 with its own replica
        await db.sharding.addShard({
            id: "shard-1",
            connection: { connectionString: "postgresql://localhost:5432/shard-1" },
            replication: {
                enabled: true,
                mode: "replica-preferred",
                replicas: [
                    {
                        id: "replica-1",
                                    connection: {
                            connectionString: "postgresql://localhost:5432/shard-1-replica-1",
                        },
                    },
                ],
            },
        });

        expect(db.sharding.shards.length).toBe(2);

        await db.sharding.removeShard("shard-1");
        expect(db.sharding.shards.length).toBe(1);

        await db.shutdown();
    });
});
