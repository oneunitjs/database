import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    MemoryFailoverStateStore,
    PgFailoverStateStore,
    FAILOVER_STATE_SCHEMA,
} from "../../src/failover/failover-state-store.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

function makeState(shardId: string, state = "healthy", version = 1) {
    return {
        shardId,
        state,
        currentGeneration: 1,
        failoverEvents: [],
        lastStateChange: new Date().toISOString(),
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        _version: version,
        updatedAt: new Date().toISOString(),
    };
}

describe("MemoryFailoverStateStore", () => {
    let store: MemoryFailoverStateStore;

    beforeEach(() => {
        store = new MemoryFailoverStateStore();
    });

    it("loadState returns null for unknown shard", async () => {
        expect(await store.loadState("shard-1")).toBeNull();
    });

    it("saveState persists and loadState returns it", async () => {
        const state = makeState("shard-1");
        const ok = await store.saveState("shard-1", state, 0);
        expect(ok).toBe(true);
        const loaded = await store.loadState("shard-1");
        expect(loaded?.shardId).toBe("shard-1");
        expect(loaded?.state).toBe("healthy");
        expect(loaded?._version).toBe(1);
    });

    it("saveState rejects version mismatch", async () => {
        const state = makeState("shard-1", "healthy", 0);
        await store.saveState("shard-1", state, 0);
        // Now version is 1; saving with expectedVersion 0 should fail
        const ok = await store.saveState("shard-1", { ...state, state: "suspect" }, 0);
        expect(ok).toBe(false);
    });

    it("transitionState changes state and records history", async () => {
        const state = makeState("shard-1", "healthy", 0);
        await store.saveState("shard-1", state, 0);

        const ok = await store.transitionState(
            "shard-1",
            "healthy",
            "suspect",
            "failure detected",
            { timestamp: new Date(), initiatedBy: "health-check" },
        );
        expect(ok).toBe(true);

        const loaded = await store.loadState("shard-1");
        expect(loaded?.state).toBe("suspect");
        expect(loaded?._version).toBe(2);

        const history = await store.getStateHistory("shard-1");
        expect(history.length).toBeGreaterThanOrEqual(1);
        expect(history[0].shardId).toBe("shard-1");
        expect(history[0].changedBy).toBe("health-check");
    });

    it("transitionState rejects wrong fromState", async () => {
        const state = makeState("shard-1", "healthy", 0);
        await store.saveState("shard-1", state, 0);

        const ok = await store.transitionState(
            "shard-1",
            "suspect",
            "primary_unavailable",
            "reason",
            { timestamp: new Date(), initiatedBy: "x" },
        );
        expect(ok).toBe(false);
    });

    it("listStates returns all shards", async () => {
        await store.saveState("shard-1", makeState("shard-1"), 0);
        await store.saveState("shard-2", makeState("shard-2"), 0);

        const states = await store.listStates();
        expect(states.size).toBe(2);
        expect(states.has("shard-1")).toBe(true);
        expect(states.has("shard-2")).toBe(true);
    });

    it("acquireLock and releaseLock work", async () => {
        expect(await store.acquireLock("shard-1", 5000)).toBe(true);
        // Same shard, lock still valid -> should fail
        expect(await store.acquireLock("shard-1", 5000)).toBe(false);
        await store.releaseLock("shard-1");
        // After release, can acquire again
        expect(await store.acquireLock("shard-1", 5000)).toBe(true);
    });

    it("expired lock can be re-acquired", async () => {
        expect(await store.acquireLock("shard-1", 50)).toBe(true);
        // Wait for expiry
        await new Promise((r) => setTimeout(r, 100));
        expect(await store.acquireLock("shard-1", 5000)).toBe(true);
    });

    it("getStateHistory returns empty for unknown shard", async () => {
        const history = await store.getStateHistory("shard-unknown");
        expect(history).toEqual([]);
    });
});

describe("FAILOVER_STATE_SCHEMA", () => {
    it("defines the three expected tables", () => {
        expect(FAILOVER_STATE_SCHEMA).toContain("_database_ha_state");
        expect(FAILOVER_STATE_SCHEMA).toContain("_database_ha_state_versions");
        expect(FAILOVER_STATE_SCHEMA).toContain("_database_ha_locks");
    });
});

describe("PgFailoverStateStore (mocked pool)", () => {
    let pool: any;
    let store: PgFailoverStateStore;

    beforeEach(() => {
        pool = {
            query: vi.fn(),
        };
        store = new PgFailoverStateStore(pool, logger);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("loadState returns null when no row", async () => {
        pool.query.mockResolvedValue({ rows: [] });
        const result = await store.loadState("shard-1");
        expect(result).toBeNull();
        expect(pool.query).toHaveBeenCalledTimes(1);
    });

    it("loadState returns parsed state with version", async () => {
        const stateJson = {
            shardId: "shard-1",
            state: "healthy",
            currentGeneration: 1,
            failoverEvents: [],
            lastStateChange: new Date().toISOString(),
            consecutiveFailures: 0,
            consecutiveSuccesses: 0,
        };
        pool.query.mockResolvedValue({
            rows: [{ state_json: stateJson, version: 3, updated_at: new Date() }],
        });
        const result = await store.loadState("shard-1");
        expect(result?.shardId).toBe("shard-1");
        expect(result?.state).toBe("healthy");
        expect(result?._version).toBe(3);
    });

    it("saveState updates with optimistic concurrency", async () => {
        pool.query.mockResolvedValue({ rowCount: 1, rows: [{ version: 2 }] });
        const ok = await store.saveState(
            "shard-1",
            makeState("shard-1", "suspect"),
            1,
        );
        expect(ok).toBe(true);
        // saveState: 1 UPDATE + 1 INSERT version history
        expect(pool.query).toHaveBeenCalledTimes(2);
    });

    it("saveState returns false on version conflict (rowCount 0)", async () => {
        pool.query.mockResolvedValue({ rowCount: 0, rows: [] });
        const ok = await store.saveState("shard-1", makeState("shard-1"), 5);
        expect(ok).toBe(false);
        expect(pool.query).toHaveBeenCalledTimes(1);
    });

    it("transitionState updates state column and records history", async () => {
        pool.query
            .mockResolvedValueOnce({ rowCount: 1, rows: [{ version: 2 }] }) // UPDATE
            .mockResolvedValueOnce({ rows: [{ state_json: { shardId: "shard-1", state: "healthy" }, version: 2, updated_at: new Date() }] }) // loadState SELECT
            .mockResolvedValueOnce({ rows: [] }); // logVersionChange INSERT
        const ok = await store.transitionState(
            "shard-1",
            "healthy",
            "primary_unavailable",
            "reason",
            { timestamp: new Date(), initiatedBy: "manager" },
        );
        expect(ok).toBe(true);
        expect(pool.query).toHaveBeenCalledTimes(3);
        const updateCall = pool.query.mock.calls[0];
        expect(updateCall[0]).toContain("state_json->>'state'");
        expect(updateCall[1][3]).toBe("healthy");
    });

    it("transitionState returns false when fromState doesn't match", async () => {
        pool.query.mockResolvedValue({ rowCount: 0, rows: [] });
        const ok = await store.transitionState(
            "shard-1",
            "suspect",
            "primary_unavailable",
            "reason",
            { timestamp: new Date(), initiatedBy: "manager" },
        );
        expect(ok).toBe(false);
    });

    it("listStates returns map of all states", async () => {
        pool.query.mockResolvedValue({
            rows: [
                { shard_id: "shard-1", state_json: { state: "healthy" }, version: 1, updated_at: new Date() },
                { shard_id: "shard-2", state_json: { state: "suspect" }, version: 1, updated_at: new Date() },
            ],
        });
        const states = await store.listStates();
        expect(states.size).toBe(2);
        expect(states.get("shard-1")?.state).toBe("healthy");
        expect(states.get("shard-2")?.state).toBe("suspect");
    });

    it("acquireLock inserts lock row", async () => {
        pool.query.mockResolvedValue({ rowCount: 1, rows: [{ lock_name: "ha_lock_shard-1" }] });
        const ok = await store.acquireLock("shard-1", 30000);
        expect(ok).toBe(true);
        const call = pool.query.mock.calls[0];
        expect(call[0]).toContain("_database_ha_locks");
        expect(call[0]).toContain("ON CONFLICT");
    });

    it("acquireLock returns false when lock already held and not expired", async () => {
        pool.query.mockResolvedValue({ rowCount: 0, rows: [] });
        const ok = await store.acquireLock("shard-1", 30000);
        expect(ok).toBe(false);
    });

    it("releaseLock deletes lock row", async () => {
        pool.query.mockResolvedValue({ rowCount: 1 });
        await store.releaseLock("shard-1");
        const call = pool.query.mock.calls[0];
        expect(call[0]).toContain("DELETE FROM _database_ha_locks");
        expect(call[1]).toEqual(["ha_lock_shard-1"]);
    });

    it("getStateHistory returns ordered versions", async () => {
        pool.query.mockResolvedValue({
            rows: [
                {
                    id: 2,
                    shard_id: "shard-1",
                    state_json: { state: "suspect" },
                    version: 2,
                    previous_version: 1,
                    changed_by: "manager",
                    changed_at: new Date(),
                    reason: "failover",
                },
            ],
        });
        const history = await store.getStateHistory("shard-1", 5);
        expect(history.length).toBe(1);
        expect(history[0].version).toBe(2);
        expect(history[0].reason).toBe("failover");
    });
});
