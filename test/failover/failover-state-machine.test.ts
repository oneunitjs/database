import { describe, it, expect } from "vitest";

import {
    nextFailoverState,
    createFailoverEvent,
    createInitialFailoverState,
    applyFailoverTransition,
    isTerminalFailoverState,
    isTransientFailoverState,
} from "../../src/failover/failover-state-machine.js";

describe("Failover state machine", () => {
    describe("nextFailoverState", () => {
        it("healthy -> suspect on failure_detected", () => {
            expect(nextFailoverState("healthy", "failure_detected")).toBe("suspect");
        });

        it("healthy ignores failover_requested", () => {
            expect(nextFailoverState("healthy", "failover_requested")).toBeNull();
        });

        it("suspect -> primary_unavailable on failure_detected", () => {
            expect(nextFailoverState("suspect", "failure_detected")).toBe("primary_unavailable");
        });

        it("suspect -> healthy on recovery_detected", () => {
            expect(nextFailoverState("suspect", "recovery_detected")).toBe("healthy");
        });

        it("primary_unavailable -> failover_requested on failover_requested", () => {
            expect(nextFailoverState("primary_unavailable", "failover_requested")).toBe("failover_requested");
        });

        it("primary_unavailable -> healthy on primary_returned", () => {
            expect(nextFailoverState("primary_unavailable", "primary_returned")).toBe("healthy");
        });

        it("failover_requested -> promoting on promotion_started", () => {
            expect(nextFailoverState("failover_requested", "promotion_started")).toBe("promoting");
        });

        it("failover_requested -> failed on promotion_failed", () => {
            expect(nextFailoverState("failover_requested", "promotion_failed")).toBe("failed");
        });

        it("promoting -> verifying on promotion_verified", () => {
            expect(nextFailoverState("promoting", "promotion_verified")).toBe("verifying");
        });

        it("promoting -> failed on promotion_failed", () => {
            expect(nextFailoverState("promoting", "promotion_failed")).toBe("failed");
        });

        it("verifying -> primary_changed on promotion_verified", () => {
            expect(nextFailoverState("verifying", "promotion_verified")).toBe("primary_changed");
        });

        it("verifying -> failed on promotion_failed", () => {
            expect(nextFailoverState("verifying", "promotion_failed")).toBe("failed");
        });

        it("primary_changed -> recovering on recovery_detected", () => {
            expect(nextFailoverState("primary_changed", "recovery_detected")).toBe("recovering");
        });

        it("recovering -> healthy on recovery_detected", () => {
            expect(nextFailoverState("recovering", "recovery_detected")).toBe("healthy");
        });

        it("failed -> recovering on recovery_detected", () => {
            expect(nextFailoverState("failed", "recovery_detected")).toBe("recovering");
        });

        it("failed -> recovering on primary_returned", () => {
            expect(nextFailoverState("failed", "primary_returned")).toBe("recovering");
        });
    });

    describe("createFailoverEvent", () => {
        it("creates event with all fields", () => {
            const event = createFailoverEvent(
                "shard-0",
                "healthy",
                "suspect",
                "connection lost",
                "health check",
            );
            expect(event.shardId).toBe("shard-0");
            expect(event.fromState).toBe("healthy");
            expect(event.toState).toBe("suspect");
            expect(event.reason).toBe("connection lost");
            expect(event.triggeredBy).toBe("health check");
            expect(event.id).toBeDefined();
            expect(event.timestamp).toBeDefined();
        });
    });

    describe("applyFailoverTransition", () => {
        it("applies valid transition", () => {
            const state = createInitialFailoverState("shard-0");
            const event = createFailoverEvent("shard-0", "healthy", "suspect", "failure", "hc");
            const newState = applyFailoverTransition(state, "suspect", event);
            expect(newState.state).toBe("suspect");
            expect(newState.failoverEvents).toHaveLength(1);
            expect(newState.failoverEvents[0]).toBe(event);
            expect(newState.lastStateChange).toBeDefined();
        });

        it("throws on invalid transition", () => {
            const state = createInitialFailoverState("shard-0");
            const event = createFailoverEvent("shard-0", "healthy", "failed", "boom", "hc");
            expect(() => applyFailoverTransition(state, "failed", event)).toThrow(
                /Invalid failover transition/,
            );
        });

        it("increments generation on primary_changed", () => {
            const state = createInitialFailoverState("shard-0");
            state.state = "verifying";
            state.currentGeneration = 5;
            // verifying -> primary_changed (generation increments)
            const event1 = createFailoverEvent("shard-0", "verifying", "primary_changed", "verified", "hc");
            const intermediate = applyFailoverTransition(state, "primary_changed", event1);
            expect(intermediate.currentGeneration).toBe(6);
            // primary_changed -> recovering (generation stays)
            const event2 = createFailoverEvent("shard-0", "primary_changed", "recovering", "recovery", "hc");
            const final = applyFailoverTransition(intermediate, "recovering", event2);
            expect(final.currentGeneration).toBe(6);
        });
    });

    describe("createInitialFailoverState", () => {
        it("creates healthy initial state", () => {
            const state = createInitialFailoverState("shard-0");
            expect(state.shardId).toBe("shard-0");
            expect(state.state).toBe("healthy");
            expect(state.currentGeneration).toBe(1);
            expect(state.failoverEvents).toEqual([]);
            expect(state.consecutiveFailures).toBe(0);
            expect(state.consecutiveSuccesses).toBe(0);
        });
    });

    describe("isTerminalFailoverState", () => {
        it("failed is terminal", () => {
            expect(isTerminalFailoverState("failed")).toBe(true);
        });

        it("healthy is not terminal", () => {
            expect(isTerminalFailoverState("healthy")).toBe(false);
        });
    });

    describe("isTransientFailoverState", () => {
        it("identifies transient states", () => {
            expect(isTransientFailoverState("suspect")).toBe(true);
            expect(isTransientFailoverState("primary_unavailable")).toBe(true);
            expect(isTransientFailoverState("failover_requested")).toBe(true);
            expect(isTransientFailoverState("promoting")).toBe(true);
            expect(isTransientFailoverState("verifying")).toBe(true);
        });

        it("identifies non-transient states", () => {
            expect(isTransientFailoverState("healthy")).toBe(false);
            expect(isTransientFailoverState("primary_changed")).toBe(false);
            expect(isTransientFailoverState("recovering")).toBe(false);
            expect(isTransientFailoverState("failed")).toBe(false);
        });
    });
});
