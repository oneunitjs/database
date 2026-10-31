import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { Pool } from "pg";
import { ReplicationManager } from "../../src/replication/replication-manager.js";
import {
    ReplicationConfigError,
    ReplicaUnavailableError,
} from "../../src/replication/errors.js";

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => ({
            query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
            end: vi.fn().mockResolvedValue(undefined),
            on: vi.fn(),
            totalCount: 0,
            idleCount: 0,
            waitingCount: 0,
        })),
    };
});

function resetMocks() {
    vi.clearAllMocks();
}

function makeConfig(overrides = {}) {
    return {
        enabled: true,
        mode: "replica-preferred",
        primary: {
            endpointId: "primary",
            connection: { connectionString: "postgresql://localhost:5432/db" },
        },
        replicas: [
            {
                id: "replica-0",
                connection: { connectionString: "postgresql://localhost:5432/replica-0" },
            },
        ],
        ...overrides,
    };
}

function makeManager(options = {}) {
    const config = makeConfig(options);
    const primaryPool = new Pool(config.primary.connection);
    return new ReplicationManager({
        primary: {
            endpointId: config.primary.endpointId,
            pool: primaryPool,
            connection: config.primary.connection,
            owned: false,
        },
        replicas: config.replicas,
        mode: config.mode,
        lagThresholdMs: config.lagThresholdMs,
        healthCheck: config.healthCheck,
    });
}

describe("ReplicationManager", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("validates config before creating replica pools", () => {
        const primaryPool = new Pool({
            connectionString: "postgresql://localhost:5432/db",
        });
        expect(() =>
            new ReplicationManager({
                primary: {
                    endpointId: "primary",
                    pool: primaryPool,
                    connection: {
                        connectionString: "postgresql://localhost:5432/db",
                    },
                },
                replicas: [
                    {
                        id: "r0",
                        connection: {
                            connectionString: "postgresql://localhost:5432/r0",
                        },
                    },
                    {
                        id: "r0",
                        connection: {
                            connectionString: "postgresql://localhost:5432/r0",
                        },
                    },
                ],
                mode: "replica-preferred",
            }),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects an invalid mode", () => {
        const primaryPool = new Pool({
            connectionString: "postgresql://localhost:5432/db",
        });
        expect(() =>
            new ReplicationManager({
                primary: {
                    endpointId: "primary",
                    pool: primaryPool,
                    connection: {
                        connectionString: "postgresql://localhost:5432/db",
                    },
                },
                replicas: [],
                mode: "invalid",
            }),
        ).toThrow(ReplicationConfigError);
    });

    it("initializes replica pools", () => {
        const mgr = makeManager();
        mgr.initialize();
        expect(Pool).toHaveBeenCalledTimes(2);
        const replica = mgr.getEndpoint("replica-0");
        expect(replica).toBeDefined();
        expect(replica.role).toBe("replica");
    });

    it("writeEndpoint returns the primary pool", () => {
        const mgr = makeManager();
        mgr.initialize();
        const write = mgr.writeEndpoint();
        expect(write.role).toBe("primary");
        expect(write.endpointId).toBe("primary");
    });

    it("readEndpoint defaults to primary", () => {
        const mgr = makeManager();
        mgr.initialize();
        const read = mgr.readEndpoint();
        expect(read.endpointId).toBe("primary");
        expect(read.role).toBe("primary");
    });

    it("readEndpoint replica-preferred routes to an eligible replica", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await mgr.endpointHealth("replica-0");
        const read = mgr.readEndpoint("replica-preferred");
        expect(read.endpointId).toBe("replica-0");
        expect(read.role).toBe("replica");
    });

    it("readEndpoint replica-only throws when no eligible replica", async () => {
        const mgr = makeManager();
        mgr.initialize();
        expect(() => mgr.readEndpoint("replica-only")).toThrow(
            ReplicaUnavailableError,
        );
    });

    it("readEndpoint replica-only returns replica when eligible", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await mgr.endpointHealth("replica-0");
        const read = mgr.readEndpoint("replica-only");
        expect(read.endpointId).toBe("replica-0");
    });

    it("health reports primary and replica endpoints", async () => {
        const mgr = makeManager();
        mgr.initialize();
        const result = await mgr.health();
        expect(result.primary.endpointId).toBe("primary");
        expect(result.replicas.length).toBe(1);
        expect(result.replicas[0].endpointId).toBe("replica-0");
    });

    it("reports unknown lag distinctly from zero lag", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await mgr.endpointHealth("replica-0");
        const h = mgr.getEndpointHealth("replica-0");
        expect(h.lagUnknown).toBe(true);
    });

    it("addReplica adds a new replica at runtime", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await mgr.addReplica({
            id: "replica-1",
            connection: { connectionString: "postgresql://localhost:5432/replica-1" },
        });
        expect(mgr.replicaEndpointIds).toContain("replica-1");
        expect(mgr.getEndpoint("replica-1").role).toBe("replica");
    });

    it("addReplica rejects duplicate ids", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await expect(
            mgr.addReplica({
                id: "replica-0",
                connection: { connectionString: "postgresql://localhost:5432/replica-0" },
            }),
        ).rejects.toThrow(ReplicationConfigError);
    });

    it("removeReplica removes a replica", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await mgr.removeReplica("replica-0");
        expect(mgr.replicaEndpointIds).not.toContain("replica-0");
        expect(mgr.getEndpoint("replica-0")).toBeUndefined();
    });

    it("removeReplica rejects unknown replica", async () => {
        const mgr = makeManager();
        mgr.initialize();
        await expect(mgr.removeReplica("nope")).rejects.toThrow(
            ReplicationConfigError,
        );
    });

    it("shutdown closes owned replica pools", async () => {
        const mgr = makeManager();
        mgr.initialize();
        const replicaPool = mgr.getEndpoint("replica-0").pool;
        await mgr.shutdown();
        expect(replicaPool.end).toHaveBeenCalled();
        expect(mgr.isClosed).toBe(true);
    });
});
