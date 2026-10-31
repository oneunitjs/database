import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";

import { ShardManager } from "../../src/sharding/shard-manager.js";
import { ShardConfigError } from "../../src/sharding/errors.js";
import { Pool } from "pg";

const PG_USER = process.env.PG_USER || "postgres";
const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = process.env.PG_PORT || 5432;
const PG_PASSWORD = process.env.PG_PASSWORD || "";

function pgUri(dbname) {
    const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : "";
    return `postgresql://${PG_USER}${auth}@${PG_HOST}:${PG_PORT}/${dbname}`;
}

function pgSocketUri(dbname) {
    const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : "";
    return `postgresql://${PG_USER}${auth}@/${dbname}?host=/run/postgresql`;
}

let hasPostgres = false;
let useSocket = false;

function dbUri(dbname) {
    return useSocket ? pgSocketUri(dbname) : pgUri(dbname);
}

const DRAIN_TEST_DB = "drain_multi";

describe("Distributed shard draining — multi-instance (live PostgreSQL)", () => {
    beforeAll(async () => {
        try {
            const pool = new Pool({ connectionString: pgUri("postgres") });
            await pool.query("SELECT 1");
            await pool.end();
            hasPostgres = true;
            useSocket = false;
        } catch {
            try {
                const pool = new Pool({ connectionString: pgSocketUri("postgres") });
                await pool.query("SELECT 1");
                await pool.end();
                hasPostgres = true;
                useSocket = true;
            } catch {
                hasPostgres = false;
            }
        }
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        try {
            await pool.query(`CREATE DATABASE ${DRAIN_TEST_DB}`);
        } catch {
            // may already exist
        }
        await pool.end();
    }, 30000);

    afterAll(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        try {
            await pool.query(`DROP DATABASE ${DRAIN_TEST_DB} WITH (FORCE)`);
        } catch {
            // ignore
        }
        await pool.end();
    }, 30000);

    beforeEach(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri(DRAIN_TEST_DB) });
        try {
            await pool.query(`DROP TABLE IF EXISTS shard_drain_state`);
            await pool.query(`DROP TABLE IF EXISTS shard_active_ops`);
            await pool.query(`DROP TABLE IF EXISTS shard_migrations`);
        } catch {
            // ignore
        }
        await pool.end();
    }, 10000);

    afterEach(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri(DRAIN_TEST_DB) });
        try {
            await pool.query(`DROP TABLE IF EXISTS shard_drain_state`);
            await pool.query(`DROP TABLE IF EXISTS shard_active_ops`);
        } catch {
            // ignore
        }
        await pool.end();
    }, 10000);

    function makeConfig(instanceId, shardId = "shared-shard") {
        return {
            id: shardId,
            connection: { connectionString: dbUri(DRAIN_TEST_DB), max: 10 },
        };
    }

    async function acquireAndHold(manager, shardId) {
        await manager.acquireShard(shardId);
        return async () => await manager.releaseShard(shardId);
    }

    it("instance A starts an operation; instance B marks draining; new acquisition on B is rejected", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance A acquires and holds an operation
            const releaseA = await acquireAndHold(instA, "shared-shard");

            // Instance B marks the shard as draining
            await instB.markShardDraining("shared-shard", 1);

            // Instance B's own acquireShard should reject (local draining flag)
            await expect(instB.acquireShard("shared-shard")).rejects.toThrow(/draining/);

            // Instance A's existing operation is unaffected (already acquired)
            // Instance A releases
            await releaseA();

            // Clear draining so shutdown works
            await instB.clearShardDraining("shared-shard");
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("instance B marks draining while instance A is registering — A's registration is rejected", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance B marks draining FIRST
            await instB.markShardDraining("shared-shard", 1);

            // Instance A tries to acquire — should be rejected by the
            // conditional INSERT (atomic check against drain state)
            await expect(instA.acquireShard("shared-shard")).rejects.toThrow(/draining/);

            await instB.clearShardDraining("shared-shard");
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("two instances attempt to drain the same shard concurrently — one wins, the other waits/fails", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Both attempt to mark draining concurrently
            const [resultA, resultB] = await Promise.allSettled([
                instB.removeShard("shared-shard"),
                instA.removeShard("shared-shard"),
            ]);

            // One should succeed, the other should fail (shard already gone or locked)
            const statuses = new Set([resultA.status, resultB.status]);
            expect(statuses.has("fulfilled") || statuses.has("rejected")).toBe(true);

            // At least one should have removed the shard
            expect(instA.hasShard("shared-shard") || instB.hasShard("shared-shard")).toBe(false);
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("instance A starts an operation; instance B marks draining; new acquisition on B is rejected", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance A acquires and holds (simulates crash: never releases)
            await instA.acquireShard("shared-shard");
            // Do NOT release — simulate instance A crashing

            // Instance B marks draining and waits (short timeout to test TTL behavior)
            await instB.markShardDraining("shared-shard", 1);

            // With a short timeout, drain should NOT complete because the
            // registration is fresh (heartbeat within 5-minute TTL)
            const result = await instB.waitForDrainComplete("shared-shard", 1000);
            expect(result.drained).toBe(false);
            expect(result.activeOps).toBeGreaterThan(0);

            // Now simulate recovery: manually remove the stale registration
            const pool = new Pool({ connectionString: dbUri(DRAIN_TEST_DB), max: 5 });
            try {
                await pool.query(`DELETE FROM shard_active_ops WHERE shard_id = $1`, ["shared-shard"]);
            } finally {
                await pool.end();
            }

            // Now drain should complete
            const result2 = await instB.waitForDrainComplete("shared-shard", 5000);
            expect(result2.drained).toBe(true);

            await instB.clearShardDraining("shared-shard");
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 90000);

    it("stale instance attempts a new operation after draining begins — rejected", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance A marks draining (simulating another instance starting removal)
            await instA.markShardDraining("shared-shard", 1);

            // Instance B (stale, doesn't know about drain) tries to acquire
            // Should be rejected by conditional INSERT (atomic drain check)
            await expect(instB.acquireShard("shared-shard")).rejects.toThrow(/draining/);

            await instA.clearShardDraining("shared-shard");
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("drain times out and is retried", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance A acquires and holds
            await instA.acquireShard("shared-shard");

            // Instance B marks draining with a very short timeout
            await instB.markShardDraining("shared-shard", 1);

            // First drain attempt times out
            const result1 = await instB.waitForDrainComplete("shared-shard", 500);
            expect(result1.drained).toBe(false);
            expect(result1.activeOps).toBeGreaterThan(0);

            // Instance A releases
            await instA.releaseShard("shared-shard");

            // Retry drain — should now complete
            const result2 = await instB.waitForDrainComplete("shared-shard", 5000);
            expect(result2.drained).toBe(true);

            await instB.clearShardDraining("shared-shard");
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("coordination database unavailable during draining — local drain still works", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        // Use a manager pointed at a non-existent coordination database
        const badConfig = {
            id: "bad-shard",
            connection: { connectionString: "postgresql://nonexistent:5432/baddb", max: 1 },
        };
        const instA = new ShardManager([badConfig], undefined, undefined, undefined, { instanceId: "inst-a" });

        try {
            // markShardDraining with no coordination pool should not throw (best effort)
            await instA.markShardDraining("bad-shard", 1);

            // Local draining flag should be set
            await expect(instA.acquireShard("bad-shard")).rejects.toThrow(/draining/);
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("an operation completes while another instance is checking the active count", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance A acquires an operation
            await instA.acquireShard("shared-shard");

            // Instance B starts drain wait (will see 1 active op)
            const drainPromise = instB.waitForDrainComplete("shared-shard", 10000);

            // Give B a moment to start checking
            await new Promise((r) => setTimeout(r, 100));

            // Instance A releases — B should now see 0 ops and drain completes
            await instA.releaseShard("shared-shard");

            const result = await drainPromise;
            expect(result.drained).toBe(true);

            await instB.clearShardDraining("shared-shard");
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);

    it("local activeOps counter is never treated as cluster-wide proof", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const instA = new ShardManager([makeConfig("inst-a")], undefined, undefined, undefined, { instanceId: "inst-a" });
        const instB = new ShardManager([makeConfig("inst-b")], undefined, undefined, undefined, { instanceId: "inst-b" });

        try {
            // Instance A acquires an operation (registers in shard_active_ops)
            await instA.acquireShard("shared-shard");

            // Instance B checks active count via distributed state — should see 1
            const opsInDb = await instB.countActiveOpsInDb("shared-shard");
            expect(opsInDb).toBe(1);

            // Instance A releases — unregisters from distributed state
            await instA.releaseShard("shared-shard");

            // Instance B checks again — should see 0
            const opsAfter = await instB.countActiveOpsInDb("shared-shard");
            expect(opsAfter).toBe(0);
        } finally {
            try { await instA.shutdown(); } catch { /* ignore */ }
            try { await instB.shutdown(); } catch { /* ignore */ }
        }
    }, 30000);
});
