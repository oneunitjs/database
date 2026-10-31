import { describe, it, expect } from "vitest";
import { ReplicationRouter } from "../../src/replication/replication-router.js";
import {
    ReplicaUnavailableError,
    PrimaryUnavailableError,
} from "../../src/replication/errors.js";

function endpoint(id, role, { status = "up", lagMs = null, latencyMs = 1 } = {}) {
    return {
        endpointId: id,
        role,
        pool: { totalCount: 0, idleCount: 0, waitingCount: 0 },
        health: {
            status,
            latencyMs,
            lagMs,
            lagUnknown: lagMs === null,
            lastCheckedAt: new Date().toISOString(),
            consecutiveFailures: 0,
        },
    };
}

describe("ReplicationRouter", () => {
    const primary = endpoint("primary", "primary");

    it("routes writes to the primary only", () => {
        const replicas = [endpoint("replica-0", "replica")];
        const router = new ReplicationRouter({
            primary,
            replicas,
            mode: "replica-preferred",
        });
        const write = router.routeWrite();
        expect(write.endpointId).toBe("primary");
        expect(write.role).toBe("primary");
    });

    it("throws PrimaryUnavailableError when the primary is down for writes", () => {
        const downPrimary = endpoint("primary", "primary", { status: "down" });
        const router = new ReplicationRouter({
            primary: downPrimary,
            replicas: [endpoint("replica-0", "replica")],
            mode: "replica-preferred",
        });
        expect(() => router.routeWrite()).toThrow(PrimaryUnavailableError);
    });

    it("never returns a replica for a write", () => {
        const router = new ReplicationRouter({
            primary,
            replicas: [endpoint("replica-0", "replica")],
            mode: "replica-only",
        });
        expect(router.routeWrite().role).toBe("primary");
    });

    it("routeRead primary always targets the primary", () => {
        const router = new ReplicationRouter({
            primary,
            replicas: [endpoint("replica-0", "replica")],
            mode: "primary",
        });
        expect(router.routeRead("primary").endpointId).toBe("primary");
        expect(router.routeRead().endpointId).toBe("primary");
        expect(router.routeRead("replica-preferred").endpointId).toBe("replica-0");
    });

    it("routeRead replica-preferred targets an eligible replica", () => {
        const router = new ReplicationRouter({
            primary,
            replicas: [endpoint("replica-0", "replica")],
            mode: "primary",
        });
        const r = router.routeRead("replica-preferred");
        expect(r.endpointId).toBe("replica-0");
        expect(r.role).toBe("replica");
    });

    it("routeRead replica-preferred falls back to primary when no eligible replica", () => {
        const downReplica = endpoint("replica-0", "replica", { status: "down" });
        const router = new ReplicationRouter({
            primary,
            replicas: [downReplica],
            mode: "primary",
        });
        const r = router.routeRead("replica-preferred");
        expect(r.endpointId).toBe("primary");
    });

    it("routeRead replica-only throws when no eligible replica", () => {
        const downReplica = endpoint("replica-0", "replica", { status: "down" });
        const router = new ReplicationRouter({
            primary,
            replicas: [downReplica],
            mode: "primary",
        });
        expect(() => router.routeRead("replica-only")).toThrow(ReplicaUnavailableError);
    });

    it("routeRead replica-only targets an eligible replica", () => {
        const router = new ReplicationRouter({
            primary,
            replicas: [endpoint("replica-0", "replica")],
            mode: "primary",
        });
        const r = router.routeRead("replica-only");
        expect(r.endpointId).toBe("replica-0");
        expect(r.role).toBe("replica");
    });

    it("excludes replicas whose lag exceeds the threshold", () => {
        const lagging = endpoint("replica-0", "replica", { lagMs: 2000 });
        const fresh = endpoint("replica-1", "replica", { lagMs: 10 });
        const router = new ReplicationRouter({
            primary,
            replicas: [lagging, fresh],
            mode: "replica-preferred",
            lagThresholdMs: 500,
        });
        const r = router.routeRead("replica-preferred");
        expect(r.endpointId).toBe("replica-1");
    });

    it("does not exclude replicas with unknown lag", () => {
        const unknown = endpoint("replica-0", "replica", { lagMs: null });
        const router = new ReplicationRouter({
            primary,
            replicas: [unknown],
            mode: "replica-preferred",
            lagThresholdMs: 500,
        });
        expect(router.routeRead("replica-preferred").endpointId).toBe("replica-0");
    });

    it("replica-only throws when the only replica is lagging beyond threshold", () => {
        const lagging = endpoint("replica-0", "replica", { lagMs: 2000 });
        const router = new ReplicationRouter({
            primary,
            replicas: [lagging],
            mode: "primary",
            lagThresholdMs: 500,
        });
        expect(() => router.routeRead("replica-only")).toThrow(ReplicaUnavailableError);
    });

    it("routeForIntent write never targets a replica", () => {
        const router = new ReplicationRouter({
            primary,
            replicas: [endpoint("replica-0", "replica")],
            mode: "replica-preferred",
        });
        expect(router.routeForIntent("write").role).toBe("primary");
        expect(router.routeForIntent("read", "replica-preferred").role).toBe("replica");
    });

    it("throws for an invalid read preference", () => {
        const router = new ReplicationRouter({
            primary,
            replicas: [],
            mode: "primary",
        });
        expect(() => router.routeRead("invalid")).toThrow();
    });
});
