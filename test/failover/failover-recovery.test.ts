import { describe, it, expect, vi, beforeEach } from "vitest";
import { FailoverRecoveryManager } from "../../src/failover/failover-recovery.js";
import { MemoryFailoverStateStore } from "../../src/failover/failover-state-store.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

function makePersistedState(shardId: string, state = "healthy", version = 1, updatedAt = new Date().toISOString()) {
    return {
        shardId,
        state,
        currentGeneration: 1,
        failoverEvents: [],
        lastStateChange: updatedAt,
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        _version: version,
        updatedAt,
    };
}

describe("FailoverRecoveryManager", () => {
    let store: MemoryFailoverStateStore;
    let recovery: FailoverRecoveryManager;

    beforeEach(() => {
        store = new MemoryFailoverStateStore();
        recovery = new FailoverRecoveryManager(store, logger, {});
    });

    it("returns success with no state when shard has none", async () => {
        const result = await recovery.recoverState("shard-empty");
        expect(result.success).toBe(true);
        expect(result.version).toBe(0);
        expect(result.state).toBeUndefined();
        expect(result.warnings).toContain("No existing state found");
        expect(result.errors).toEqual([]);
    });

    it("recovers valid state with lock acquire/release actions", async () => {
        const state = makePersistedState("shard-1", "healthy", 3);
        await store.saveState("shard-1", state, 0);

        const result = await recovery.recoverState("shard-1");
        expect(result.success).toBe(true);
        expect(result.state?.shardId).toBe("shard-1");
        expect(result.state?.state).toBe("healthy");
        expect(result.version).toBe(1);
        expect(result.actions.some((a) => a.type === "lock_acquired")).toBe(true);
        expect(result.actions.some((a) => a.type === "lock_released")).toBe(true);
        expect(result.actions.some((a) => a.type === "state_loaded")).toBe(true);
        expect(result.actions.some((a) => a.type === "state_validated")).toBe(true);
        expect(result.actions.some((a) => a.type === "state_restored")).toBe(true);
        expect(result.errors).toEqual([]);
    });

    it("fails recovery when state validation finds errors and forceRecovery is false", async () => {
        // Corrupt state: invalid state value
        const badState = makePersistedState("shard-1", "bogus_state" as any, 1);
        await store.saveState("shard-1", badState, 0);

        const result = await recovery.recoverState("shard-1");
        expect(result.success).toBe(false);
        expect(result.errors.length).toBeGreaterThan(0);
        expect(result.state).toBeUndefined();
    });

    it("succeeds with forceRecovery even when validation has errors", async () => {
        const badState = makePersistedState("shard-1", "bogus_state" as any, 1);
        await store.saveState("shard-1", badState, 0);

        const recoveryForce = new FailoverRecoveryManager(store, logger, { forceRecovery: true });
        const result = await recoveryForce.recoverState("shard-1");
        expect(result.success).toBe(true);
        expect(result.state?.state).toBe("bogus_state");
    });

    it("autoReconcile fixes invalid state and persists", async () => {
        const badState = makePersistedState("shard-1", "bogus_state" as any, 1);
        await store.saveState("shard-1", badState, 0);

        const recoveryAuto = new FailoverRecoveryManager(store, logger, { autoReconcile: true });
        const result = await recoveryAuto.recoverState("shard-1");
        expect(result.success).toBe(true);
        expect(result.warnings).toContain("State was automatically reconciled");
        // State should now be healthy after reconciliation
        const reloaded = await store.loadState("shard-1");
        expect(reloaded?.state).toBe("healthy");
    });

    it("validateState identifies corrupted state", () => {
        const bad = makePersistedState("shard-1", "bogus_state" as any, 1);
        const validation = recovery.validateState(bad);
        expect(validation.valid).toBe(false);
        expect(validation.issues.some((i) => i.type === "corrupted_state")).toBe(true);
    });

    it("validateState accepts valid state", () => {
        const good = makePersistedState("shard-1", "promoting", 5);
        const validation = recovery.validateState(good);
        expect(validation.valid).toBe(true);
        expect(validation.issues).toEqual([]);
    });

    it("validateState flags invalid version", () => {
        const badVersion = makePersistedState("shard-1", "healthy", -1);
        const validation = recovery.validateState(badVersion);
        expect(validation.valid).toBe(false);
        expect(validation.issues.some((i) => i.type === "invalid_version")).toBe(true);
    });

    it("reconcileState fixes missing fields", () => {
        const incomplete = {
            shardId: "shard-1",
            state: "healthy",
        } as any;
        const validation = { valid: false, issues: [] };
        const result = recovery.reconcileState(incomplete, validation);
        expect(result).not.toBeNull();
        expect(result?._version).toBe(0);
        expect(result?.updatedAt).toBeDefined();
    });

    it("reconcileState returns null when nothing to fix", () => {
        const good = makePersistedState("shard-1", "healthy", 2);
        const validation = { valid: true, issues: [] };
        const result = recovery.reconcileState(good, validation);
        expect(result).toBeNull();
    });

    it("concurrent recovery serialized via lock", async () => {
        const state = makePersistedState("shard-1", "healthy", 1);
        await store.saveState("shard-1", state, 0);

        // First recovery acquires lock
        const r1 = recovery.recoverState("shard-1");
        // Second recovery should fail to acquire lock (same shard, lock TTL 30s)
        const r2 = recovery.recoverState("shard-1");

        const results = await Promise.all([r1, r2]);
        // One should succeed, one should fail (lock not acquired)
        const successes = results.filter((r) => r.success);
        const failures = results.filter((r) => !r.success);
        expect(successes.length).toBe(1);
        expect(failures.length).toBe(1);
        expect(failures[0].errors.some((e) => e.includes("acquire recovery lock"))).toBe(true);
    });
});
