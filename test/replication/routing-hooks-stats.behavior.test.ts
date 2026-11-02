import { describe, it, expect, vi } from "vitest";
import { ReplicaSelectionStatsTracker } from "../../src/replication/replication-stats.js";
import { ReadRoutingHookRunner } from "../../src/replication/replication-hooks.js";
import { ReplicationRouter } from "../../src/replication/replication-router.js";

function replica(id) {
    return {
        endpointId: id,
        role: "replica",
        pool: { totalCount: 0 },
        health: {
            endpointId: id,
            role: "replica",
            status: "up",
            latencyMs: 1,
            lagMs: 0,
            lagUnknown: false,
            lastCheckedAt: new Date().toISOString(),
            consecutiveFailures: 0,
        },
    };
}

describe("replica selection stats", () => {
    it("aggregates success rate, latency, and trims to max events", () => {
        const tracker = new ReplicaSelectionStatsTracker(3);
        tracker.recordSelection({
            timestamp: new Date("2026-01-01T00:00:00Z"),
            replicaId: "r1",
            policy: "first",
            success: true,
            latencyMs: 10,
        });
        tracker.recordSelection({
            timestamp: new Date("2026-01-01T00:00:01Z"),
            replicaId: "r1",
            policy: "first",
            success: false,
            error: "timeout",
        });
        tracker.recordSelection({
            timestamp: new Date("2026-01-01T00:00:02Z"),
            replicaId: "r2",
            policy: "weighted",
            success: true,
            latencyMs: 20,
        });
        tracker.recordSelection({
            timestamp: new Date("2026-01-01T00:00:03Z"),
            replicaId: "r2",
            policy: "weighted",
            success: true,
            latencyMs: 40,
        });
        const stats = tracker.getStats();
        expect(stats.totalSelections).toBe(3);
        expect(stats.byReplica.get("r1")).toBe(1);
        expect(stats.byReplica.get("r2")).toBe(2);
        expect(stats.avgLatencyByReplica.get("r2")?.avgMs).toBe(30);
        expect(tracker.getEventsForPolicy("weighted")).toHaveLength(2);
        tracker.clear();
        expect(tracker.getStats().totalSelections).toBe(0);
    });

    it("ignores non-finite latency", () => {
        const tracker = new ReplicaSelectionStatsTracker();
        tracker.recordSelection({
            timestamp: new Date(),
            replicaId: "r1",
            policy: "first",
            success: true,
            latencyMs: Number.NaN,
        });
        expect(tracker.getStats().avgLatencyByReplica.size).toBe(0);
    });

    it("slices recent events and per-replica history", () => {
        const tracker = new ReplicaSelectionStatsTracker();
        for (let i = 0; i < 5; i++) {
            tracker.recordSelection({
                timestamp: new Date(i),
                replicaId: i % 2 === 0 ? "r1" : "r2",
                policy: "first",
                success: true,
                latencyMs: i,
            });
        }
        expect(tracker.getRecentEvents(2)).toHaveLength(2);
        expect(tracker.getEventsForReplica("r1")).toHaveLength(3);
        expect(tracker.getEventsForReplica("r2", 1)).toHaveLength(1);
    });
});

describe("read routing hooks", () => {
    it("invokes hooks in registration order and isolates failures", async () => {
        const runner = new ReadRoutingHookRunner();
        const seen = [];
        const first = () => {
            seen.push("a");
        };
        runner.registerHook("beforeSelectReplica", first);
        runner.registerHook("beforeSelectReplica", () => {
            throw new Error("hook boom");
        });
        runner.registerHook("beforeSelectReplica", () => {
            seen.push("c");
        });
        await expect(
            runner.invoke("beforeSelectReplica", {
                router: {
                    getAvailableReplicas: () => ["r1"],
                    getCurrentPolicy: () => "first",
                    getReplicaHealth: () => undefined,
                    getSelectionStats: () => ({
                        totalSelections: 0,
                        byReplica: new Map(),
                        byPolicy: new Map(),
                        successRateByReplica: new Map(),
                        avgLatencyByReplica: new Map(),
                        lastSelectionByReplica: new Map(),
                        errorCountByReplica: new Map(),
                    }),
                },
                data: { availableReplicas: ["r1"] },
            }),
        ).resolves.toBeUndefined();
        expect(seen).toEqual(["a", "c"]);
        expect(runner.getHookCount("beforeSelectReplica")).toBe(3);
        expect(runner.unregisterHook("beforeSelectReplica", first)).toBe(true);
        expect(runner.unregisterHook("afterQuery", first)).toBe(false);
        runner.clear();
        expect(runner.getHookCount("beforeSelectReplica")).toBe(0);
    });

    it("router selectReplicaWithHooks fires before and after even when none selected", async () => {
        const before = vi.fn();
        const after = vi.fn();
        const router = new ReplicationRouter({
            primary: {
                endpointId: "primary",
                role: "primary",
                pool: { totalCount: 0 },
            },
            replicas: [replica("r1")],
            mode: "replica-preferred",
        });
        router.replicaEndpoints[0].health.status = "down";
        router.registerHook("beforeSelectReplica", before);
        router.registerHook("afterSelectReplica", after);
        const selected = await router.selectReplicaWithHooks("first", "select 1");
        expect(selected).toBeNull();
        expect(before).toHaveBeenCalledTimes(1);
        expect(after.mock.calls[0][0].data.selectedReplica).toBeNull();
    });
});
