import type { FailoverState, FailoverEvent, ShardFailoverState, FailoverConfig } from "./failover-types.js";

const STATE_TRANSITIONS: Record<FailoverState, FailoverState[]> = {
    healthy: ["suspect"],
    suspect: ["primary_unavailable", "healthy"],
    primary_unavailable: ["failover_requested", "recovering", "healthy"],
    failover_requested: ["promoting", "failed"],
    promoting: ["verifying", "failed"],
    verifying: ["primary_changed", "failed"],
    primary_changed: ["recovering", "healthy"],
    recovering: ["healthy", "failed"],
    failed: ["recovering"],
};

function isValidTransition(from: FailoverState, to: FailoverState): boolean {
    return STATE_TRANSITIONS[from]?.includes(to) ?? false;
}

export function nextFailoverState(
    current: FailoverState,
    event: "failure_detected" | "recovery_detected" | "failover_requested" | "promotion_started" | "promotion_verified" | "promotion_failed" | "primary_returned" | "operator_intervention",
): FailoverState | null {
    const transitionMap: Record<FailoverState, Record<string, FailoverState | null>> = {
        healthy: {
            failure_detected: "suspect",
            recovery_detected: "healthy",
            failover_requested: null,
            promotion_started: null,
            promotion_verified: null,
            promotion_failed: null,
            primary_returned: null,
            operator_intervention: null,
        },
        suspect: {
            failure_detected: "primary_unavailable",
            recovery_detected: "healthy",
            failover_requested: null,
            promotion_started: null,
            promotion_verified: null,
            promotion_failed: null,
            primary_returned: "healthy",
            operator_intervention: null,
        },
        primary_unavailable: {
            failure_detected: "primary_unavailable",
            recovery_detected: "healthy",
            failover_requested: "failover_requested",
            promotion_started: null,
            promotion_verified: null,
            promotion_failed: null,
            primary_returned: "healthy",
            operator_intervention: "failed",
        },
        failover_requested: {
            failure_detected: "failover_requested",
            recovery_detected: "failover_requested",
            failover_requested: "failover_requested",
            promotion_started: "promoting",
            promotion_verified: null,
            promotion_failed: "failed",
            primary_returned: "recovering",
            operator_intervention: "failed",
        },
        promoting: {
            failure_detected: "failed",
            recovery_detected: "promoting",
            failover_requested: "promoting",
            promotion_started: "promoting",
            promotion_verified: "verifying",
            promotion_failed: "failed",
            primary_returned: "failed",
            operator_intervention: "failed",
        },
        verifying: {
            failure_detected: "failed",
            recovery_detected: "verifying",
            failover_requested: "verifying",
            promotion_started: null,
            promotion_verified: "primary_changed",
            promotion_failed: "failed",
            primary_returned: "failed",
            operator_intervention: "failed",
        },
        primary_changed: {
            failure_detected: "primary_changed",
            recovery_detected: "recovering",
            failover_requested: null,
            promotion_started: null,
            promotion_verified: null,
            promotion_failed: null,
            primary_returned: null,
            operator_intervention: null,
        },
        recovering: {
            failure_detected: "suspect",
            recovery_detected: "healthy",
            failover_requested: null,
            promotion_started: null,
            promotion_verified: null,
            promotion_failed: null,
            primary_returned: "healthy",
            operator_intervention: null,
        },
        failed: {
            failure_detected: "failed",
            recovery_detected: "recovering",
            failover_requested: null,
            promotion_started: null,
            promotion_verified: null,
            promotion_failed: null,
            primary_returned: "recovering",
            operator_intervention: null,
        },
    };

    return transitionMap[current]?.[event] ?? null;
}

export function createFailoverEvent(
    shardId: string,
    fromState: FailoverState,
    toState: FailoverState,
    reason: string,
    triggeredBy: string,
): FailoverEvent {
    return {
        id: `${shardId}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
        shardId,
        fromState,
        toState,
        reason,
        triggeredBy,
        timestamp: new Date().toISOString(),
    };
}

export function createInitialFailoverState(shardId: string): ShardFailoverState {
    return {
        shardId,
        state: "healthy",
        currentGeneration: 1,
        failoverEvents: [],
        lastStateChange: new Date().toISOString(),
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
    };
}

export function applyFailoverTransition(
    state: ShardFailoverState,
    toState: FailoverState,
    event: FailoverEvent,
): ShardFailoverState {
    if (!isValidTransition(state.state, toState)) {
        throw new Error(
            `Invalid failover transition for shard "${state.shardId}": ${state.state} -> ${toState}`,
        );
    }

    return {
        ...state,
        state: toState,
        currentGeneration: toState === "primary_changed" ? state.currentGeneration + 1 : state.currentGeneration,
        failoverEvents: [...state.failoverEvents, event],
        lastStateChange: new Date().toISOString(),
        consecutiveFailures: toState === "healthy" ? 0 : state.consecutiveFailures,
        consecutiveSuccesses: toState === "healthy" ? state.consecutiveSuccesses + 1 : 0,
    };
}

export function isTerminalFailoverState(state: FailoverState): boolean {
    return state === "failed";
}

export function isTransientFailoverState(state: FailoverState): boolean {
    return ["suspect", "primary_unavailable", "failover_requested", "promoting", "verifying"].includes(state);
}
