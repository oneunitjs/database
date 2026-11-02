import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ConnectionTracker } from "../../src/failover/connection-tracker.js";

function makeMockClient(id: string) {
    return {
        query: vi.fn().mockResolvedValue({ rows: [] }),
        release: vi.fn(),
        [Symbol.for(`client-${id}`)]: id,
    };
}

describe("ConnectionTracker", () => {
    let tracker: ConnectionTracker;

    beforeEach(() => {
        tracker = new ConnectionTracker({
            cleanupIntervalMs: 60_000,
            maxIdleMs: 300_000,
            logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
        });
    });

    afterEach(() => {
        tracker.stop();
    });

    it("registerConnection adds a tracked connection", () => {
        const client = makeMockClient("c1");
        const conn = tracker.registerConnection("node-1", "shard-1", client);
        expect(conn.nodeId).toBe("node-1");
        expect(conn.shardId).toBe("shard-1");
        expect(conn.client).toBe(client);
        expect(conn.inTransaction).toBe(false);
        expect(conn.id).toContain("conn_");
    });

    it("unregisterConnection removes a tracked connection", () => {
        const client = makeMockClient("c1");
        tracker.registerConnection("node-1", "shard-1", client);
        expect(tracker.getConnectionCount()).toBe(1);
        const removed = tracker.unregisterConnection(client);
        expect(removed).toBe(true);
        expect(tracker.getConnectionCount()).toBe(0);
    });

    it("unregisterConnection returns false for unknown client", () => {
        const client = makeMockClient("c-unknown");
        expect(tracker.unregisterConnection(client)).toBe(false);
    });

    it("markInTransaction / markOutOfTransaction toggles transaction state", () => {
        const client = makeMockClient("c1");
        tracker.registerConnection("node-1", "shard-1", client);
        tracker.markInTransaction(client, "tx-1");
        const conns = tracker.getConnectionsForNode("node-1");
        expect(conns[0].inTransaction).toBe(true);
        expect(conns[0].transactionId).toBe("tx-1");

        tracker.markOutOfTransaction(client);
        expect(tracker.getConnectionsForNode("node-1")[0].inTransaction).toBe(false);
    });

    it("touch updates lastUsedAt", () => {
        vi.useFakeTimers();
        const client = makeMockClient("c1");
        tracker.registerConnection("node-1", "shard-1", client);
        const before = tracker.getConnectionsForNode("node-1")[0].lastUsedAt;
        vi.advanceTimersByTime(1000);
        tracker.touch(client);
        const after = tracker.getConnectionsForNode("node-1")[0].lastUsedAt;
        expect(after.getTime()).toBeGreaterThan(before.getTime());
        vi.useRealTimers();
    });

    it("getConnectionsForNode returns empty for unknown node", () => {
        expect(tracker.getConnectionsForNode("node-unknown")).toEqual([]);
    });

    it("getConnectionCount returns total across nodes", () => {
        tracker.registerConnection("node-1", "shard-1", makeMockClient("c1"));
        tracker.registerConnection("node-1", "shard-1", makeMockClient("c2"));
        tracker.registerConnection("node-2", "shard-1", makeMockClient("c3"));
        expect(tracker.getConnectionCount()).toBe(3);
    });

    it("invalidateConnections closes non-transaction connections", async () => {
        const client1 = makeMockClient("c1");
        const client2 = makeMockClient("c2");
        tracker.registerConnection("node-1", "shard-1", client1);
        tracker.registerConnection("node-1", "shard-1", client2);

        const result = await tracker.invalidateConnections("node-1");
        expect(result.invalidatedCount).toBe(2);
        expect(result.activeTransactionCount).toBe(0);
        expect(result.errors).toEqual([]);
        expect(client1.release).toHaveBeenCalled();
        expect(client2.release).toHaveBeenCalled();
        expect(tracker.getConnectionCount()).toBe(0);
    });

    it("invalidateConnections counts but skips in-transaction connections", async () => {
        const client1 = makeMockClient("c1");
        const client2 = makeMockClient("c2");
        tracker.registerConnection("node-1", "shard-1", client1);
        tracker.registerConnection("node-1", "shard-1", client2);
        tracker.markInTransaction(client2, "tx-1");

        const result = await tracker.invalidateConnections("node-1");
        expect(result.invalidatedCount).toBe(1);
        expect(result.activeTransactionCount).toBe(1);
        expect(client1.release).toHaveBeenCalled();
        expect(client2.release).not.toHaveBeenCalled();
        // In-transaction connection still tracked
        expect(tracker.getConnectionCount()).toBe(1);
    });

    it("invalidateConnections returns empty result for unknown node", async () => {
        const result = await tracker.invalidateConnections("node-unknown");
        expect(result.invalidatedCount).toBe(0);
        expect(result.activeTransactionCount).toBe(0);
    });

    it("invalidateConnectionsExcept skips the excluded node", async () => {
        const client1 = makeMockClient("c1");
        const client2 = makeMockClient("c2");
        tracker.registerConnection("node-1", "shard-1", client1);
        tracker.registerConnection("node-2", "shard-1", client2);

        const results = await tracker.invalidateConnectionsExcept("node-2");
        expect(results.length).toBe(1);
        expect(results[0].nodeId).toBe("node-1");
        expect(results[0].invalidatedCount).toBe(1);
        // node-2 connection still tracked
        expect(tracker.getConnectionCount()).toBe(1);
    });

    it("invalidateAllConnections closes all", async () => {
        tracker.registerConnection("node-1", "shard-1", makeMockClient("c1"));
        tracker.registerConnection("node-2", "shard-1", makeMockClient("c2"));

        const results = await tracker.invalidateAllConnections();
        expect(results.length).toBe(2);
        expect(tracker.getConnectionCount()).toBe(0);
    });

    it("cleanupIdleConnections removes idle connections past maxIdleMs", async () => {
        vi.useFakeTimers();
        tracker.stop();
        tracker = new ConnectionTracker({
            cleanupIntervalMs: 1000,
            maxIdleMs: 50,
            logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
        });

        const client = makeMockClient("c1");
        tracker.registerConnection("node-1", "shard-1", client);
        expect(tracker.getConnectionCount()).toBe(1);

        // Manually invoke the cleanup logic via timer advancement
        await vi.advanceTimersByTimeAsync(1200);
        // Idle connection should be cleaned up (registered > 50ms ago)
        expect(tracker.getConnectionCount()).toBe(0);
        vi.useRealTimers();
    });
});
