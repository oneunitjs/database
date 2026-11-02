import type { FailoverConfig } from "./failover-types.js";

/**
 * Health status of a single node, tracked by the HealthCheckTracker.
 */
export interface NodeHealthStatus {
    nodeId: string;
    consecutiveFailures: number;
    consecutiveSuccesses: number;
    lastFailureTime: Date | null;
    lastSuccessTime: Date | null;
    currentStatus: "healthy" | "degraded" | "unhealthy";
    statusSince: Date;
}

/**
 * Per-node threshold overrides. Any undefined field falls back to the
 * corresponding global config value.
 */
export interface NodeFailureThresholds {
    consecutiveFailures?: number;
    unhealthyDurationMs?: number;
    consecutiveSuccesses?: number;
    healthCheckIntervalMs?: number;
    unhealthyHealthCheckMultiplier?: number;
}

/**
 * Logger type compatible with FailoverManager's RequiredFailoverLogger.
 */
export type RequiredFailoverLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

/**
 * Tracks per-node health check results and determines when failover should
 * be initiated based on configurable thresholds.
 */
export class HealthCheckTracker {
    private readonly statuses = new Map<string, NodeHealthStatus>();

    constructor(
        private readonly config: RequiredFailoverConfig,
        private readonly logger: RequiredFailoverLogger,
    ) {}

    /**
     * Record a failed health check for a node. Returns the updated status.
     */
    recordFailure(nodeId: string): NodeHealthStatus {
        let status = this.statuses.get(nodeId) ?? this.createStatus(nodeId);
        const nodeThresholds = this.config.nodeThresholds?.[nodeId] ?? {};
        const threshold = nodeThresholds.consecutiveFailures ?? this.config.consecutiveFailureThreshold;

        status.consecutiveFailures++;
        status.consecutiveSuccesses = 0;
        status.lastFailureTime = new Date();

        if (status.consecutiveFailures >= threshold) {
            status.currentStatus = "unhealthy";
            status.statusSince = new Date();
            this.logger.info(
                { nodeId, consecutiveFailures: status.consecutiveFailures },
                "Node marked unhealthy",
            );
        } else if (
            status.consecutiveFailures >= threshold / 2 &&
            status.currentStatus === "healthy"
        ) {
            status.currentStatus = "degraded";
            status.statusSince = new Date();
            this.logger.warn(
                { nodeId, consecutiveFailures: status.consecutiveFailures },
                "Node degraded",
            );
        }

        this.statuses.set(nodeId, status);
        return status;
    }

    /**
     * Record a successful health check for a node. Returns the updated status.
     */
    recordSuccess(nodeId: string): NodeHealthStatus {
        let status = this.statuses.get(nodeId) ?? this.createStatus(nodeId);
        const nodeThresholds = this.config.nodeThresholds?.[nodeId] ?? {};
        const successThreshold = nodeThresholds.consecutiveSuccesses ?? this.config.consecutiveSuccessThreshold;
        const durationThreshold = nodeThresholds.unhealthyDurationMs ?? this.config.unhealthyDurationThresholdMs;

        status.consecutiveSuccesses++;
        status.consecutiveFailures = 0;
        status.lastSuccessTime = new Date();

        if (
            status.currentStatus === "degraded" &&
            status.consecutiveSuccesses >= successThreshold
        ) {
            status.currentStatus = "healthy";
            status.statusSince = new Date();
            this.logger.info(
                { nodeId, consecutiveSuccesses: status.consecutiveSuccesses },
                "Node recovered to healthy",
            );
        } else if (
            status.currentStatus === "unhealthy" &&
            status.consecutiveSuccesses >= successThreshold &&
            (!status.lastFailureTime ||
                Date.now() - status.lastFailureTime.getTime() >= durationThreshold)
        ) {
            status.currentStatus = "healthy";
            status.statusSince = new Date();
            this.logger.info(
                { nodeId, consecutiveSuccesses: status.consecutiveSuccesses },
                "Node recovered",
            );
        }

        this.statuses.set(nodeId, status);
        return status;
    }

    /**
     * Returns whether failover should be initiated for a node based on
     * consecutive failures and the unhealthy duration threshold.
     */
    shouldInitiateFailover(nodeId: string): boolean {
        const status = this.statuses.get(nodeId);
        if (!status) return false;

        const nodeThresholds = this.config.nodeThresholds?.[nodeId] ?? {};
        const failureThreshold = nodeThresholds.consecutiveFailures ?? this.config.consecutiveFailureThreshold;
        const durationThreshold = nodeThresholds.unhealthyDurationMs ?? this.config.unhealthyDurationThresholdMs;

        return (
            status.consecutiveFailures >= failureThreshold &&
            status.lastFailureTime !== null &&
            Date.now() - status.lastFailureTime.getTime() >= durationThreshold
        );
    }

    /**
     * Returns the effective health check interval for a node, applying the
     * unhealthy multiplier when the node is unhealthy.
     */
    getEffectiveHealthCheckInterval(nodeId: string): number {
        const status = this.statuses.get(nodeId);
        if (!status) return this.config.healthCheckIntervalMs;

        const nodeThresholds = this.config.nodeThresholds?.[nodeId] ?? {};
        const baseInterval =
            nodeThresholds.healthCheckIntervalMs ?? this.config.healthCheckIntervalMs;

        if (status.currentStatus === "unhealthy") {
            return Math.round(
                baseInterval *
                    (nodeThresholds.unhealthyHealthCheckMultiplier ??
                        this.config.unhealthyHealthCheckMultiplier),
            );
        }

        return baseInterval;
    }

    /**
     * Get the current health status for a node, if tracked.
     */
    getStatus(nodeId: string): NodeHealthStatus | undefined {
        return this.statuses.get(nodeId);
    }

    /**
     * Get all tracked node health statuses.
     */
    getAllStatuses(): Map<string, NodeHealthStatus> {
        return new Map(this.statuses);
    }

    private createStatus(nodeId: string): NodeHealthStatus {
        return {
            nodeId,
            consecutiveFailures: 0,
            consecutiveSuccesses: 0,
            lastFailureTime: null,
            lastSuccessTime: null,
            currentStatus: "healthy",
            statusSince: new Date(),
        };
    }
}

/**
 * Failover configuration extended with failure detection threshold fields.
 * These are additive to the existing `FailoverConfig` in failover-types.ts.
 */
export interface RequiredFailoverConfig {
    consecutiveFailureThreshold: number;
    unhealthyDurationThresholdMs: number;
    consecutiveSuccessThreshold: number;
    healthCheckIntervalMs: number;
    unhealthyHealthCheckMultiplier: number;
    nodeThresholds?: Record<string, NodeFailureThresholds>;
}

export const DEFAULT_FAIL_OVER_CONFIG: Required<RequiredFailoverConfig> = {
    consecutiveFailureThreshold: 3,
    unhealthyDurationThresholdMs: 30_000,
    consecutiveSuccessThreshold: 2,
    healthCheckIntervalMs: 5_000,
    unhealthyHealthCheckMultiplier: 0.5,
    nodeThresholds: {},
};
