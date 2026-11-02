import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    TableBasedFencingProvider,
    FencedError,
    defaultFencingHooks,
} from "../../src/failover/fencing.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

function makeMockPool(rowsByQuery: Record<string, any[]> = {}) {
    const query = vi.fn((text: string) => {
        for (const [pattern, rows] of Object.entries(rowsByQuery)) {
            if (text.includes(pattern)) {
                return Promise.resolve({ rows });
            }
        }
        return Promise.resolve({ rows: [] });
    });
    return {
        query,
        connect: vi.fn().mockResolvedValue({
            query,
            release: vi.fn(),
        }),
    };
}

describe("TableBasedFencingProvider", () => {
    let pool: any;
    let provider: TableBasedFencingProvider;

    beforeEach(() => {
        pool = makeMockPool();
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("activate inserts into fencing_locks and history", async () => {
        const client = await pool.connect();
        // Track insert calls on client
        const clientQueryCalls: string[] = [];
        (client.query as any).mockImplementation((text: string) => {
            clientQueryCalls.push(text);
            return Promise.resolve({ rows: [] });
        });

        await provider.activate("node-1", "failover initiated", { ttlMs: 60_000, priority: 2 });

        expect(clientQueryCalls.some((t) => t.includes("_database_fencing_locks"))).toBe(true);
        expect(clientQueryCalls.some((t) => t.includes("_database_fencing_history"))).toBe(true);
        expect(logger.info).toHaveBeenCalled();
        client.release();
    });

    it("deactivate deletes from fencing_locks and adds history", async () => {
        // Mock pool that has a row when deactivate checks (isFenced / SELECT 1)
        const queryFn = vi.fn((text: string) => {
            if (text.includes("SELECT 1 FROM _database_fencing_locks")) {
                return Promise.resolve({ rows: [{ node_id: "node-1" }] });
            }
            return Promise.resolve({ rows: [] });
        });
        const client: any = { query: queryFn, release: vi.fn() };
        pool = {
            query: queryFn,
            connect: vi.fn().mockResolvedValue(client),
        };
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);

        const calls: string[] = [];
        const originalImpl = client.query as any;
        (client.query as any).mockImplementation((text: string, params?: any[]) => {
            calls.push(text);
            if (text.includes("SELECT 1 FROM _database_fencing_locks")) {
                return Promise.resolve({ rows: [{ node_id: "node-1" }] });
            }
            return Promise.resolve({ rows: [] });
        });

        await provider.deactivate("node-1");
        expect(calls.some((t) => t.includes("DELETE FROM _database_fencing_locks"))).toBe(true);
        expect(calls.some((t) => t.includes("_database_fencing_history"))).toBe(true);
        client.release();
    });

    it("isFenced returns true when row exists", async () => {
        pool = makeMockPool({ "SELECT 1 FROM _database_fencing_locks": [{ node_id: "node-1" }] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        expect(await provider.isFenced("node-1")).toBe(true);
    });

    it("isFenced returns false when no row", async () => {
        pool = makeMockPool({ "SELECT 1 FROM _database_fencing_locks": [] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        expect(await provider.isFenced("node-1")).toBe(false);
    });

    it("check throws FencedError when node is fenced", async () => {
        pool = makeMockPool({ "SELECT 1 FROM _database_fencing_locks": [{ node_id: "node-1" }] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        await expect(provider.check("node-1")).rejects.toBeInstanceOf(FencedError);
        expect(logger.warn).toHaveBeenCalled();
    });

    it("check resolves when node is not fenced", async () => {
        pool = makeMockPool({ "SELECT 1 FROM _database_fencing_locks": [] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        await expect(provider.check("node-1")).resolves.toBeUndefined();
    });

    it("getFencedNodes returns list of fenced nodes", async () => {
        pool = makeMockPool({ "SELECT node_id FROM _database_fencing_locks": [{ node_id: "node-1" }, { node_id: "node-2" }] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        const nodes = await provider.getFencedNodes();
        expect(nodes).toEqual(["node-1", "node-2"]);
    });

    it("getStatus returns fenced status with reason", async () => {
        pool = makeMockPool({
            "SELECT reason, expires_at, priority, activated_at FROM _database_fencing_locks": [
                {
                    reason: "failover",
                    expires_at: null,
                    priority: 1,
                    activated_at: new Date().toISOString(),
                },
            ],
        });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        const status = await provider.getStatus("node-1");
        expect(status.fenced).toBe(true);
        expect(status.reason).toBe("failover");
        expect(status.priority).toBe(1);
        expect(status.nodeId).toBe("node-1");
    });

    it("getStatus returns not fenced when no row", async () => {
        pool = makeMockPool({ "SELECT reason, expires_at, priority, activated_at FROM _database_fencing_locks": [] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        const status = await provider.getStatus("node-1");
        expect(status.fenced).toBe(false);
        expect(status.nodeId).toBe("node-1");
    });

    it("cleanup calls deactivate", async () => {
        pool = makeMockPool({ "SELECT 1 FROM _database_fencing_locks": [{ node_id: "node-1" }] });
        provider = new TableBasedFencingProvider(pool, logger, defaultFencingHooks);
        const client = await pool.connect();
        const calls: string[] = [];
        (client.query as any).mockImplementation((text: string) => {
            calls.push(text);
            if (text.includes("SELECT 1 FROM _database_fencing_locks")) {
                return Promise.resolve({ rows: [{ node_id: "node-1" }] });
            }
            return Promise.resolve({ rows: [] });
        });
        await provider.cleanup("node-1");
        expect(calls.some((t) => t.includes("DELETE FROM _database_fencing_locks"))).toBe(true);
        client.release();
    });

    it("FencedError has nodeId", () => {
        const err = new FencedError("node-1", "Node is fenced");
        expect(err.nodeId).toBe("node-1");
        expect(err.name).toBe("FencedError");
        expect(err.message).toBe("Node is fenced");
    });

    it("defaultFencingHooks is a working no-op hooks object", () => {
        expect(() => {
            defaultFencingHooks.onFencingActivated?.("node-1", "reason");
            defaultFencingHooks.onFencingDeactivated?.("node-1");
            defaultFencingHooks.onFencingCheckFailed?.("node-1", new Error("boom"));
        }).not.toThrow();
    });
});
