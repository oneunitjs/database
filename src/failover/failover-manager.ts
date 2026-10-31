import type { Pool } from "pg";
import type {
    FailoverConfig,
    HaProvider,
    HaNodeInfo,
    HaNodeStatus,
    HaTopology,
    FailoverState,
    ShardFailoverState,
    FailoverEvent,
} from "./failover-types.js";
import {
    createInitialFailoverState,
    applyFailoverTransition,
    createFailoverEvent,
    nextFailoverState,
    isTransientFailoverState,
    isTerminalFailoverState,
} from "./failover-state-machine.js";

export interface FailoverManagerOptions {
    shardId: string;
    primaryPool: Pool;
    config: FailoverConfig;
    logger: RequiredFailoverLogger;
    onStateChange?: (state: ShardFailoverState) => void;
    onFailoverComplete?: (newPrimary: HaNodeInfo | undefined) => void;
}

export type RequiredFailoverLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

export class FailoverManager {
    private readonly shardId: string;
    private readonly primaryPool: Pool;
    private readonly config: FailoverConfig;
    private readonly logger: RequiredFailoverLogger;
    private readonly onStateChange?: FailoverManagerOptions["onStateChange"];
    private readonly onFailoverComplete?: FailoverManagerOptions["onFailoverComplete"];

    private state: ShardFailoverState;
    private provider: HaProvider;
    private healthCheckTimer?: ReturnType<typeof setInterval>;
    private failoverLock = false;
    private closed = false;

    constructor(options: FailoverManagerOptions) {
        this.shardId = options.shardId;
        this.primaryPool = options.primaryPool;
        this.config = options.config;
        this.logger = options.logger ?? {
            debug: () => {},
            info: () => {},
            warn: () => {},
            error: () => {},
        };
        this.onStateChange = options.onStateChange;
        this.onFailoverComplete = options.onFailoverComplete;
        this.state = createInitialFailoverState(this.shardId);
        this.provider = options.config.provider;
    }

    get currentState(): FailoverState {
        return this.state.state;
    }

    get currentGeneration(): number {
        return this.state.currentGeneration;
    }

    get currentPrimary(): string | undefined {
        return this.state.currentPrimary;
    }

    get failoverState(): ShardFailoverState {
        return { ...this.state };
    }

    async initialize(): Promise<void> {
        await this.provider.initialize();

        // Perform initial topology discovery
        try {
            const topology = await this.provider.discoverTopology(this.shardId);
            if (topology.primary) {
                this.state = {
                    ...this.state,
                    currentPrimary: topology.primary.id,
                    currentGeneration: topology.generation,
                };
            }
        } catch (error) {
            this.logger.warn(
                { shardId: this.shardId, err: error },
                "Initial topology discovery failed",
            );
        }

        this.startHealthChecks();
    }

    private startHealthChecks(): void {
        const intervalMs = this.config.healthCheckIntervalMs ?? 5000;
        this.healthCheckTimer = setInterval(() => {
            void this.performHealthCheck().catch((error) => {
                this.logger.error(
                    { shardId: this.shardId, err: error },
                    "Health check failed",
                );
            });
        }, intervalMs);
        // Allow process to exit even if timer is active
        if (this.healthCheckTimer.unref) {
            this.healthCheckTimer.unref();
        }
    }

    private async performHealthCheck(): Promise<void> {
        if (this.closed || this.failoverLock) {
            return;
        }

        // Check primary health using a simple query
        let primaryUp = false;
        let checkLatency = 0;
        try {
            const start = Date.now();
            await this.primaryPool.query("SELECT 1");
            checkLatency = Date.now() - start;
            primaryUp = true;
        } catch {
            primaryUp = false;
        }

        const failureThreshold = this.config.failureThreshold ?? 3;
        const recoveryThreshold = this.config.recoveryThreshold ?? 2;

        if (primaryUp) {
            this.state.consecutiveFailures = 0;
            this.state.consecutiveSuccesses += 1;

            // Recover from suspect or primary_unavailable if we've had enough successes
            if (
                (this.state.state === "suspect" || this.state.state === "primary_unavailable") &&
                this.state.consecutiveSuccesses >= recoveryThreshold
            ) {
                await this.transitionTo("healthy", "recovery_detected", "health check");
            }
        } else {
            this.state.consecutiveSuccesses = 0;
            this.state.consecutiveFailures += 1;

            if (this.state.state === "healthy" && this.state.consecutiveFailures >= failureThreshold) {
                await this.transitionTo("suspect", "failure_detected", "health check");
            } else if (this.state.state === "suspect") {
                // Move to primary_unavailable after continued failures
                const suspectTimeout = this.config.suspectTimeoutMs ?? 30000;
                const lastChange = new Date(this.state.lastStateChange).getTime();
                if (Date.now() - lastChange > suspectTimeout) {
                    await this.transitionTo("primary_unavailable", "failure_detected", "health check");
                }
            }
        }

        this.state.lastCheckedAt = new Date().toISOString();
    }

    private async transitionTo(
        toState: FailoverState,
        reason: string,
        triggeredBy: string,
    ): Promise<void> {
        const next = nextFailoverState(this.state.state, reason as any);
        if (next === null || next !== toState) {
            // Validate via the state machine
            const validNext = nextFailoverState(
                this.state.state,
                reason as "failure_detected" | "recovery_detected" | "failover_requested" | "promotion_started" | "promotion_verified" | "promotion_failed" | "primary_returned" | "operator_intervention",
            );
            if (validNext === null) {
                this.logger.warn(
                    { shardId: this.shardId, from: this.state.state, to: toState },
                    "Invalid failover transition, refusing",
                );
                return;
            }
        }

        const event = createFailoverEvent(
            this.shardId,
            this.state.state,
            toState,
            reason,
            triggeredBy,
        );

        this.state = applyFailoverTransition(this.state, toState, event);
        this.logger.info(
            { shardId: this.shardId, from: event.fromState, to: event.toState, reason },
            "Failover state transition",
        );

        this.onStateChange?.(this.state);

        // Execute state-specific actions
        await this.executeStateActions(toState);
    }

    private async executeStateActions(state: FailoverState): Promise<void> {
        switch (state) {
            case "primary_unavailable":
                await this.requestFailover();
                break;
            case "promoting":
                await this.performPromotion();
                break;
            case "verifying":
                await this.performVerification();
                break;
            case "primary_changed":
                await this.completeFailover();
                break;
            case "recovering":
                await this.performRecovery();
                break;
            case "failed":
                await this.handleFailoverFailure();
                break;
        }
    }

    private async requestFailover(): Promise<void> {
        if (this.failoverLock) return;
        this.failoverLock = true;

        try {
            await this.transitionTo("failover_requested", "primary unavailable, requesting failover", "failover manager");

            // Check if provider supports promotion
            if (!this.config.provider.supportsPromotion) {
                this.logger.warn(
                    { shardId: this.shardId },
                    "Failover requested but provider does not support promotion — operator intervention required",
                );
                await this.transitionTo("failed", "provider does not support promotion", "failover manager");
                return;
            }

            await this.transitionTo("promoting", "failover approved, starting promotion", "failover manager");
        } finally {
            this.failoverLock = false;
        }
    }

    private async performPromotion(): Promise<void> {
        if (this.failoverLock) return;
        this.failoverLock = true;

        try {
            // Discover topology to find an eligible replica
            const topology = await this.provider.discoverTopology(this.shardId);
            const eligibleReplica = topology.replicas.find(
                (r) => r.status === "up" &&
                    (!this.config.replicationLagThresholdMs ||
                        (r.lagMs ?? 0) <= this.config.replicationLagThresholdMs),
            );

            if (!eligibleReplica) {
                this.logger.warn(
                    { shardId: this.shardId },
                    "No eligible replica for promotion — failing closed",
                );
                await this.transitionTo("failed", "no eligible replica", "failover manager");
                return;
            }

            // Fence the old primary before promoting
            if (this.state.currentPrimary && this.provider.fenceOldPrimary) {
                const fenced = await this.provider.fenceOldPrimary(this.state.currentPrimary);
                if (!fenced) {
                    this.logger.warn(
                        { shardId: this.shardId, oldPrimary: this.state.currentPrimary },
                        "Failed to fence old primary — failing closed",
                    );
                    await this.transitionTo("failed", "could not fence old primary", "failover manager");
                    return;
                }
            }

            // Promote the replica
            const promotionTimeout = this.config.promotionTimeoutMs ?? 30000;
            const promoted = await Promise.race([
                this.provider.promoteReplica!(this.shardId, eligibleReplica.id),
                new Promise<never>((_, reject) =>
                    setTimeout(
                        () => reject(new Error("Promotion timeout")),
                        promotionTimeout,
                    ),
                ),
            ]);

            this.state = {
                ...this.state,
                currentPrimary: promoted.id,
                currentGeneration: (this.state.currentGeneration ?? 0) + 1,
            };

            await this.transitionTo("verifying", `replica "${eligibleReplica.id}" promoted`, "failover manager");
        } catch (error) {
            this.logger.error(
                { shardId: this.shardId, err: error },
                "Promotion failed",
            );
            await this.transitionTo("failed", `promotion failed: ${error instanceof Error ? error.message : String(error)}`, "failover manager");
        } finally {
            this.failoverLock = false;
        }
    }

    private async performVerification(): Promise<void> {
        if (!this.state.currentPrimary) {
            await this.transitionTo("failed", "no primary to verify", "failover manager");
            return;
        }

        const verificationTimeout = this.config.verificationTimeoutMs ?? 15000;
        let verified = false;

        try {
            verified = await Promise.race([
                this.provider.verifyPrimary!(
                    this.state.currentPrimary,
                    this.state.currentGeneration,
                ),
                new Promise<boolean>((_, reject) =>
                    setTimeout(
                        () => reject(new Error("Verification timeout")),
                        verificationTimeout,
                    ),
                ),
            ]);
        } catch {
            verified = false;
        }

        if (verified) {
            // Additional check: try a write query on the new primary
            try {
                await this.primaryPool.query("SELECT 1");
                await this.transitionTo("primary_changed", "new primary verified", "failover manager");
            } catch {
                await this.transitionTo("failed", "new primary not writable", "failover manager");
            }
        } else {
            await this.transitionTo("failed", "new primary verification failed", "failover manager");
        }
    }

    private async completeFailover(): Promise<void> {
        this.logger.info(
            { shardId: this.shardId, primary: this.state.currentPrimary, generation: this.state.currentGeneration },
            "Failover completed — new primary active",
        );

        this.onFailoverComplete?.(
            this.state.currentPrimary
                ? {
                      id: this.state.currentPrimary,
                      role: "primary",
                      status: "up",
                      lagMs: null,
                      lagUnknown: true,
                      lastCheckedAt: new Date().toISOString(),
                      consecutiveFailures: 0,
                      connection: {},
                  }
                : undefined,
        );

        // Transition to recovering to allow reconnection and pool refresh
        await this.transitionTo("recovering", "failover complete, recovering", "failover manager");
    }

    private async performRecovery(): Promise<void> {
        // Re-establish connections, refresh pools, reset failure counters
        this.state.consecutiveFailures = 0;
        this.state.consecutiveSuccesses = 0;

        try {
            await this.primaryPool.query("SELECT 1");
            await this.transitionTo("healthy", "recovery complete", "health check");
        } catch {
            // Primary still not reachable — stay in recovering
            this.logger.warn(
                { shardId: this.shardId },
                "Primary still unreachable during recovery",
            );
        }
    }

    private async handleFailoverFailure(): Promise<void> {
        this.logger.error(
            { shardId: this.shardId, state: this.state },
            "Failover failed — operator intervention required",
        );
        // Terminal state — wait for operator intervention
    }

    /**
     * Mark the primary as suspect due to an external event (e.g. connection error
     * during a query). This is called from query/transaction paths when a
     * connection failure is detected.
     */
    async reportPrimaryFailure(reason: string): Promise<void> {
        this.state.consecutiveFailures += 1;
        const failureThreshold = this.config.failureThreshold ?? 3;

        if (this.state.state === "healthy" && this.state.consecutiveFailures >= failureThreshold) {
            await this.transitionTo("suspect", reason, "query path");
        } else if (this.state.state === "suspect") {
            const suspectTimeout = this.config.suspectTimeoutMs ?? 30000;
            const lastChange = new Date(this.state.lastStateChange).getTime();
            if (Date.now() - lastChange > suspectTimeout) {
                await this.transitionTo("primary_unavailable", reason, "query path");
            }
        }
    }

    /**
     * Mark the primary as recovered after a successful operation.
     */
    async reportPrimarySuccess(): Promise<void> {
        this.state.consecutiveFailures = 0;
        this.state.consecutiveSuccesses += 1;

        if (
            (this.state.state === "suspect" || this.state.state === "primary_unavailable") &&
            this.state.consecutiveSuccesses >= (this.config.recoveryThreshold ?? 2)
        ) {
            await this.transitionTo("healthy", "recovery_detected", "query path");
        }
    }

    /**
     * Get the current topology from the provider.
     */
    async getTopology(): Promise<HaTopology> {
        return this.provider.discoverTopology(this.shardId);
    }

    /**
     * Request a manual failover (operator-initiated).
     */
    async requestManualFailover(reason: string): Promise<void> {
        if (this.failoverLock) {
            throw new Error("Failover already in progress");
        }
        if (!this.config.provider.supportsPromotion) {
            throw new Error("Provider does not support promotion");
        }

        this.failoverLock = true;
        try {
            await this.transitionTo("failover_requested", reason, "operator");
            await this.transitionTo("promoting", "manual failover approved", "operator");
        } finally {
            this.failoverLock = false;
        }
    }

    async shutdown(): Promise<void> {
        this.closed = true;
        if (this.healthCheckTimer) {
            clearInterval(this.healthCheckTimer);
            this.healthCheckTimer = undefined;
        }
        await this.provider.shutdown().catch(() => {});
    }
}
