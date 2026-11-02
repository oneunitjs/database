import { describe, it, expect } from "vitest";
import {
    WeightedReplicaSelector,
    RoundRobinReplicaSelector,
    LeastConnectionsReplicaSelector,
    FirstEligibleReplicaSelector,
    createReplicaSelector,
} from "../../src/replication/replica-selectors.js";
import { ReplicationConnectionTracker } from "../../src/replication/replication-connection-tracker.js";
import { ReplicationRouter } from "../../src/replication/replication-router.js";
import { ReplicaUnavailableError, PrimaryUnavailableError } from "../../src/replication/errors.js";

function replica(id, { status = "up", lagMs = null, weight } = {}) {
    return {
        endpointId: id,
        role: "replica",
        pool: { totalCount: 0, idleCount: 0, waitingCount: 0 },
        weight,
        health: {
            endpointId: id,
            role: "replica",
            status,
            latencyMs: 1,
            lagMs,
            lagUnknown: lagMs === null,
            lastCheckedAt: new Date().toISOString(),
            consecutiveFailures: 0,
        },
    };
}

function primary(status = "up") {
    return {
        endpointId: "primary",
        role: "primary",
        pool: { totalCount: 0, idleCount: 0, waitingCount: 0 },
        health: {
            endpointId: "primary",
            role: "primary",
            status,
            latencyMs: 1,
            lagMs: null,
            lagUnknown: true,
            lastCheckedAt: new Date().toISOString(),
            consecutiveFailures: 0,
        },
    };
}

describe("replica selection behavior", () => {
    it("returns null when no replica is eligible", () => {
        const selector = new FirstEligibleReplicaSelector();
        expect(selector.selectReplica([])).toBeNull();
        expect(selector.selectReplicas([], 3)).toEqual([]);
        expect(selector.selectReplicas([{ endpointId: "r1", role: "replica" }], 0)).toEqual([]);
        expect(selector.selectReplicas([{ endpointId: "r1", role: "replica" }], -1)).toEqual([]);
    });

    it("round-robin cycles through eligible replicas and wraps", () => {
        const selector = new RoundRobinReplicaSelector();
        const eligible = [
            { endpointId: "r1", role: "replica" },
            { endpointId: "r2", role: "replica" },
            { endpointId: "r3", role: "replica" },
        ];
        expect(selector.selectReplica(eligible)?.endpointId).toBe("r1");
        expect(selector.selectReplica(eligible)?.endpointId).toBe("r2");
        expect(selector.selectReplica(eligible)?.endpointId).toBe("r3");
        expect(selector.selectReplica(eligible)?.endpointId).toBe("r1");
        selector.reset();
        expect(selector.selectReplica(eligible)?.endpointId).toBe("r1");
    });

    it("round-robin selectReplicas does not return more than available", () => {
        const selector = new RoundRobinReplicaSelector();
        const eligible = [
            { endpointId: "r1", role: "replica" },
            { endpointId: "r2", role: "replica" },
        ];
        expect(selector.selectReplicas(eligible, 5).map((r) => r.endpointId)).toEqual(["r1", "r2"]);
    });

    it("weighted selectReplicas returns unique replicas and empty for non-positive count", () => {
        const selector = new WeightedReplicaSelector();
        const eligible = [
            { endpointId: "r1", role: "replica", weight: 1 },
            { endpointId: "r2", role: "replica", weight: 1 },
        ];
        const selected = selector.selectReplicas(eligible, 2);
        expect(new Set(selected.map((r) => r.endpointId)).size).toBe(selected.length);
        expect(selected.length).toBeLessThanOrEqual(2);
        expect(selector.selectReplicas(eligible, 0)).toEqual([]);
        expect(selector.selectReplicas(eligible, Number.NaN)).toEqual([]);
    });

    it("weighted selection follows probability mass", () => {
        const selector = new WeightedReplicaSelector({}, () => 0.99);
        const eligible = [
            { endpointId: "small", role: "replica", weight: 1 },
            { endpointId: "large", role: "replica", weight: 99 },
        ];
        expect(selector.selectReplica(eligible)?.endpointId).toBe("large");
    });

    it("weighted selection falls back when all weights are zero", () => {
        const selector = new WeightedReplicaSelector({ minWeight: 0 });
        const eligible = [
            { endpointId: "r1", role: "replica", weight: 0 },
            { endpointId: "r2", role: "replica", weight: 0 },
        ];
        expect(selector.selectReplica(eligible)?.endpointId).toBe("r1");
    });

    it("falls back to defaultWeight for non-finite and negative weights", () => {
        const selector = new WeightedReplicaSelector({ defaultWeight: 1 }, () => 0);
        const eligible = [
            { endpointId: "bad", role: "replica", weight: Number.NaN },
            { endpointId: "neg", role: "replica", weight: -5 },
            { endpointId: "ok", role: "replica", weight: 1 },
        ];
        expect(selector.selectReplica(eligible)?.endpointId).toBe("bad");
    });

    it("least-connections prefers the replica with fewer in-flight connections", () => {
        const tracker = new ReplicationConnectionTracker();
        tracker.acquireConnection("busy");
        tracker.acquireConnection("busy");
        tracker.acquireConnection("quiet");
        const selector = new LeastConnectionsReplicaSelector(tracker);
        const eligible = [
            { endpointId: "busy", role: "replica" },
            { endpointId: "quiet", role: "replica" },
        ];
        expect(selector.selectReplica(eligible)?.endpointId).toBe("quiet");
        expect(selector.selectReplicas(eligible, 2).map((r) => r.endpointId)).toEqual(["quiet", "busy"]);
    });

    it("connection tracker never goes negative and ignores empty ids", () => {
        const tracker = new ReplicationConnectionTracker();
        tracker.releaseConnection("missing");
        tracker.releaseConnection("");
        tracker.acquireConnection("");
        expect(tracker.getConnectionCount("missing")).toBe(0);
        tracker.acquireConnection("r1");
        tracker.releaseConnection("r1");
        tracker.releaseConnection("r1");
        expect(tracker.getConnectionCount("r1")).toBe(0);
        expect(tracker.getAllConnectionCounts().has("r1")).toBe(false);
    });

    it("router replica-only fails closed when every replica is down or lagging", () => {
        const router = new ReplicationRouter({
            primary: primary(),
            replicas: [
                replica("r1", { status: "down" }),
                replica("r2", { lagMs: 5000 }),
            ],
            mode: "replica-only",
            lagThresholdMs: 100,
        });
        expect(() => router.routeRead()).toThrow(ReplicaUnavailableError);
    });

    it("router replica-preferred falls back to primary rather than a lagging replica", () => {
        const router = new ReplicationRouter({
            primary: primary(),
            replicas: [replica("r1", { lagMs: 5000 })],
            mode: "replica-preferred",
            lagThresholdMs: 100,
        });
        expect(router.routeRead().endpointId).toBe("primary");
    });

    it("router never writes to a replica even when replica-only is configured", () => {
        const router = new ReplicationRouter({
            primary: primary(),
            replicas: [replica("r1")],
            mode: "replica-only",
        });
        expect(router.routeWrite().role).toBe("primary");
        expect(() =>
            new ReplicationRouter({
                primary: primary("down"),
                replicas: [replica("r1")],
                mode: "replica-only",
            }).routeWrite(),
        ).toThrow(PrimaryUnavailableError);
    });

    it("round-robin on the router spreads reads across eligible replicas", () => {
        const router = new ReplicationRouter({
            primary: primary(),
            replicas: [replica("r1"), replica("r2"), replica("r3")],
            mode: "replica-preferred",
            replicaSelectionPolicy: "round-robin",
        });
        const ids = [router.routeRead(), router.routeRead(), router.routeRead(), router.routeRead()].map(
            (e) => e.endpointId,
        );
        expect(ids).toEqual(["r1", "r2", "r3", "r1"]);
    });

    it("least-connections on the router shifts away from a loaded replica", () => {
        const router = new ReplicationRouter({
            primary: primary(),
            replicas: [replica("r1"), replica("r2")],
            mode: "replica-preferred",
            replicaSelectionPolicy: "least-connections",
        });
        router.acquireConnection("r1");
        router.acquireConnection("r1");
        expect(router.routeRead().endpointId).toBe("r2");
        router.releaseConnection("r1");
        router.releaseConnection("r1");
        expect(router.getConnectionCount("r1")).toBe(0);
    });

    it("unknown selection policy falls back to first eligible", () => {
        const selector = createReplicaSelector("not-a-policy");
        expect(selector.selectReplica([{ endpointId: "r1", role: "replica" }])?.endpointId).toBe("r1");
    });

    it("records selection stats including failed empty selections", () => {
        const router = new ReplicationRouter({
            primary: primary(),
            replicas: [replica("r1", { status: "down" })],
            mode: "replica-preferred",
            replicaSelectionPolicy: "first",
        });
        expect(router.selectReplica()).toBeNull();
        const stats = router.getSelectionStats();
        expect(stats.totalSelections).toBe(1);
        expect(stats.byReplica.get("none")).toBe(1);
        expect(stats.errorCountByReplica.get("none")).toBe(1);
    });
});
