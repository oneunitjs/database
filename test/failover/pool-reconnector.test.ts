import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    PoolReconnector,
    DEFAULT_RECONNECTION_OPTIONS,
} from "../../src/failover/pool-reconnector.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

function makeMockPool(shouldFail = false) {
    const pool: any = {
        connect: vi.fn().mockImplementation(async () => {
            if (shouldFail) throw new Error("connect failed");
            return {
                query: vi.fn().mockResolvedValue({ rows: [{ "?column?": 1 }] }),
                release: vi.fn(),
            };
        }),
        end: vi.fn().mockResolvedValue(undefined),
    };
    return pool;
}

describe("PoolReconnector", () => {
    let poolMap: Map<string, any>;
    let reconnector: PoolReconnector;
    let poolFactory: (config: any) => any;

    beforeEach(() => {
        poolMap = new Map();
        poolFactory = (config: any) => makeMockPool();
        reconnector = new PoolReconnector(
            poolMap,
            async (nodeId: any) => ({ host: "localhost", port: 5432, database: "db", user: "u", password: "p" }),
            logger,
            {
                maxRetries: 2,
                retryDelayMs: 10,
                maxRetryDelayMs: 100,
                validatePool: true,
            },
            poolFactory,
        );
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("reconnectPool creates a new pool and swaps it in", async () => {
        const oldPool = makeMockPool();
        poolMap.set("node-1", oldPool);

        const expectedNewPool = makeMockPool();
        poolFactory = vi.fn().mockReturnValue(expectedNewPool) as any;
        reconnector = new PoolReconnector(
            poolMap,
            async () => ({ host: "localhost", port: 5432, database: "db", user: "u", password: "p" }),
            logger,
            { maxRetries: 2, retryDelayMs: 10, maxRetryDelayMs: 100, validatePool: true },
            poolFactory,
        );

        const result = await reconnector.reconnectPool("node-1");
        expect(result.success).toBe(true);
        expect(result.nodeId).toBe("node-1");
        expect(result.newPool).toBe(expectedNewPool);
        expect(result.oldPool).toBe(oldPool);
        expect(result.retries).toBe(0);
        // Old pool ended
        expect(oldPool.end).toHaveBeenCalled();
        // Map swapped
        expect(poolMap.get("node-1")).toBe(expectedNewPool);
    });

    it("reconnectPool retries with exponential backoff on failure", async () => {
        const oldPool = makeMockPool();
        poolMap.set("node-1", oldPool);

        const failingPool = makeMockPool(true);
        poolFactory = vi.fn().mockReturnValue(failingPool) as any;
        reconnector = new PoolReconnector(
            poolMap,
            async () => ({ host: "localhost" }),
            logger,
            { maxRetries: 2, retryDelayMs: 10, maxRetryDelayMs: 100, validatePool: true },
            poolFactory,
        );

        const result = await reconnector.reconnectPool("node-1");
        expect(result.success).toBe(false);
        expect(result.retries).toBe(2);
        expect(result.error).toContain("connect failed");
    });

    it("reconnectPool returns existing promise for concurrent calls", async () => {
        const oldPool = makeMockPool();
        poolMap.set("node-1", oldPool);
        const expectedNewPool = makeMockPool();
        poolFactory = vi.fn().mockReturnValue(expectedNewPool) as any;
        reconnector = new PoolReconnector(
            poolMap,
            async () => ({ host: "localhost" }),
            logger,
            { maxRetries: 2, retryDelayMs: 10, maxRetryDelayMs: 100, validatePool: true },
            poolFactory,
        );

        const p1 = reconnector.reconnectPool("node-1");
        const p2 = reconnector.reconnectPool("node-1");
        const [r1, r2] = await Promise.all([p1, p2]);
        expect(r1).toBe(r2);
        expect(r1.success).toBe(true);
    });

    it("reconnectPrimaryPool delegates to reconnectPool", async () => {
        const oldPool = makeMockPool();
        poolMap.set("node-1", oldPool);
        const newPool = makeMockPool();
        poolFactory = vi.fn().mockReturnValue(newPool) as any;
        reconnector = new PoolReconnector(
            poolMap,
            async () => ({ host: "localhost" }),
            logger,
            { maxRetries: 2, retryDelayMs: 10, maxRetryDelayMs: 100, validatePool: true },
            poolFactory,
        );

        const result = await reconnector.reconnectPrimaryPool("node-1");
        expect(result.success).toBe(true);
    });

    it("reconnectReplicaPools reconnects all replicas concurrently", async () => {
        const oldPool1 = makeMockPool();
        const oldPool2 = makeMockPool();
        poolMap.set("node-1", oldPool1);
        poolMap.set("node-2", oldPool2);
        const newPool1 = makeMockPool();
        const newPool2 = makeMockPool();
        poolFactory = vi.fn()
            .mockReturnValueOnce(newPool1)
            .mockReturnValueOnce(newPool2) as any;
        reconnector = new PoolReconnector(
            poolMap,
            async (nodeId: string) => ({ host: "localhost" }),
            logger,
            { maxRetries: 2, retryDelayMs: 10, maxRetryDelayMs: 100, validatePool: true },
            poolFactory,
        );

        const results = await reconnector.reconnectReplicaPools(["node-1", "node-2"]);
        expect(results.length).toBe(2);
        expect(results[0].success).toBe(true);
        expect(results[1].success).toBe(true);
    });

    it("getPendingCount returns 0 when no reconnections in flight", () => {
        expect(reconnector.getPendingCount()).toBe(0);
    });

    it("validatePool=false skips test query", async () => {
        const oldPool = makeMockPool();
        poolMap.set("node-1", oldPool);
        const newPool = makeMockPool();
        poolFactory = vi.fn().mockReturnValue(newPool) as any;
        reconnector = new PoolReconnector(
            poolMap,
            async () => ({ host: "localhost" }),
            logger,
            { ...DEFAULT_RECONNECTION_OPTIONS, maxRetries: 1, retryDelayMs: 10, maxRetryDelayMs: 100, validatePool: false },
            poolFactory,
        );

        const result = await reconnector.reconnectPool("node-1");
        expect(result.success).toBe(true);
        expect(newPool.connect).not.toHaveBeenCalled();
    });
});
