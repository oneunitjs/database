import type { Pool, PoolClient } from "pg";
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
import type {
    FailoverStateStore,
    FailoverContext,
} from "./failover-state-store.js";
import type { FailoverRecoveryOptions } from "./failover-recovery.js";
import { FailoverRecoveryManager } from "./failover-recovery.js";
import { ConnectionTracker } from "./connection-tracker.js";
import type { ConnectionInvalidationResult } from "./connection-tracker.js";
import { HealthCheckClassifier } from "./health-classifier.js";
import { FailoverHookRunner } from "./failover-hooks.js";
import type { FailoverManagerHookAPI } from "./failover-hooks.js";
import { HealthCheckTracker } from "./health-check-tracker.js";
import { PoolReconnector } from "./pool-reconnector.js";
import { FencingManager } from "./fencing-manager.js";
import type {
    FailoverEventStore,
    FailoverEvent as PlanFailoverEvent,
    FailoverEventType,
} from "./failover-event-store.js";
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
    stateStore?: FailoverStateStore;
    recoveryOptions?: FailoverRecoveryOptions;
    connectionTracker?: ConnectionTracker;
    eventStore?: FailoverEventStore;
    healthTracker?: HealthCheckTracker;
    hooks?: FailoverHookRunner;
    poolReconnector?: PoolReconnector;
    fencingManager?: FencingManager;
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
    private readonly stateStore?: FailoverStateStore;
    private readonly recoveryManager?: FailoverRecoveryManager;
    private readonly connectionTracker?: ConnectionTracker;
    private readonly eventStore?: FailoverEventStore;
    private readonly classifier: HealthCheckClassifier;
    private readonly healthTracker?: HealthCheckTracker;
    private readonly hookRunner: FailoverHookRunner;
    private readonly poolReconnector?: PoolReconnector;
    private readonly fencingManager?: FencingManager;

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
        this.stateStore = options.stateStore;
        this.recoveryManager = options.stateStore
            ? new FailoverRecoveryManager(options.stateStore, this.logger, options.recoveryOptions)
            : undefined;
        this.connectionTracker = options.connectionTracker ?? undefined;
        this.eventStore = options.eventStore ?? undefined;
        this.classifier = new HealthCheckClassifier();
        this.healthTracker = options.healthTracker
            ? options.healthTracker
            : new HealthCheckTracker(
                {
                    consecutiveFailureThreshold: this.config.failureThreshold ?? 3,
                    unhealthyDurationThresholdMs: this.config.suspectTimeoutMs ?? 30_000,
                    consecutiveSuccessThreshold: this.config.recoveryThreshold ?? 2,
                    healthCheckIntervalMs: this.config.healthCheckIntervalMs ?? 5_000,
                    unhealthyHealthCheckMultiplier: 0.5,
                    nodeThresholds: undefined,
                },
                this.logger,
            );
        this.hookRunner = options.hooks ?? new FailoverHookRunner(this.logger);
        this.poolReconnector = options.poolReconnector ?? undefined;
        this.fencingManager = options.fencingManager ?? undefined;
        this.state = createInitialFailoverState(this.shardId);
        this.provider = options.config.provider;
    }

    private get hookAPI(): FailoverManagerHookAPI {
        return {
            getCurrentState: () => this.currentState,
            getCurrentPrimary: () => this.currentPrimary,
            getCurrentGeneration: () => this.currentGeneration,
            getShardId: () => this.shardId,
        };
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

        // Recover persisted state from the state store, if one is configured.
        // When a recovery manager is available it performs locking, validation
        // and optional reconciliation before restoring state. Otherwise fall back
        // to a direct load so existing behaviour is preserved.
        if (this.stateStore) {
            try {
                let savedState: ShardFailoverState | null = null;
                if (this.recoveryManager) {
                    const result = await this.recoveryManager.recoverState(this.shardId);
                    if (result.success && result.state) {
                        savedState = result.state;
                        this.logger.info(
                            {
                                shardId: this.shardId,
                                state: result.state.state,
                                version: result.version,
                                warnings: result.warnings,
                            },
                            "Recovered failover state via recovery manager",
                        );
                        if (result.warnings.length > 0) {
                            this.logger.warn(
                                { shardId: this.shardId, warnings: result.warnings },
                                "Recovery warnings",
                            );
                        }
                    } else {
                        this.logger.warn(
                            { shardId: this.shardId, errors: result.errors },
                            "Failed to recover failover state via recovery manager",
                        );
                    }
                } else {
                    savedState = await this.stateStore.loadState(this.shardId);
                    if (savedState) {
                        this.logger.info(
                            { shardId: this.shardId, state: savedState.state },
                            "Recovered failover state from persistent store",
                        );
                    }
                }
                if (savedState) {
                    this.state = savedState;
                }
            } catch (error) {
                this.logger.warn(
                    { shardId: this.shardId, err: error },
                    "Failed to load persisted failover state",
                );
            }
        }

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

        const nodeId = this.state.currentPrimary ?? "unknown";
        await this.hookRunner.invoke("beforeHealthCheck", {
            shardId: this.shardId,
            manager: this.hookAPI,
            data: { nodeId },
        });

        // Check primary health using a simple query
        let primaryUp = false;
        let checkLatency = 0;
        const start = Date.now();

        // Check if the node is fenced before performing the health check.
        if (this.fencingManager) {
            try {
                await this.fencingManager.check(nodeId);
            } catch {
                primaryUp = false;
                this.state.consecutiveSuccesses = 0;
                this.state.consecutiveFailures += 1;
                this.state.lastCheckedAt = new Date().toISOString();
                return;
            }
        }

        try {
            await this.primaryPool.query("SELECT 1");
            checkLatency = Date.now() - start;
            primaryUp = true;
            const nodeId = this.state.currentPrimary ?? "unknown";
            this.healthTracker?.recordSuccess(nodeId);
        } catch (error) {
            primaryUp = false;
            const nodeId = this.state.currentPrimary ?? "unknown";
            this.healthTracker?.recordFailure(nodeId);
            const err = error instanceof Error ? error : new Error(String(error));
            const classification = this.classifier.classify({
                error: err,
                nodeId: this.state.currentPrimary ?? "unknown",
                shardId: this.shardId,
                timestamp: new Date(),
                durationMs: Date.now() - start,
            });

            const logPayload = {
                shardId: this.shardId,
                nodeId: this.state.currentPrimary,
                classification,
                durationMs: Date.now() - start,
            };

            switch (classification.severity) {
                case "critical":
                    this.logger.error(logPayload, classification.description);
                    break;
                case "persistent":
                    this.logger.warn(logPayload, classification.description);
                    break;
                default:
                    this.logger.info(logPayload, classification.description);
            }

            if (this.classifier.shouldFailoverImmediately(classification)) {
                await this.transitionTo(
                    "suspect",
                    `immediate failover: ${classification.category}`,
                    "health check",
                );
            }
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

        // Check if the health tracker recommends initiating failover.
        const trackerNodeId = this.state.currentPrimary ?? "unknown";
        if (this.healthTracker?.shouldInitiateFailover(trackerNodeId)) {
            await this.transitionTo("suspect", "failure_detected", "health check");
        }

        const checkLatencyMs = Date.now() - start;
        await this.hookRunner.invoke("afterHealthCheck", {
            shardId: this.shardId,
            manager: this.hookAPI,
            data: { nodeId: trackerNodeId, healthy: primaryUp, durationMs: checkLatencyMs },
        });

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

        const oldState = this.state;

        // Invoke beforeStateChange hooks.
        await this.hookRunner.invoke("beforeStateChange", {
            shardId: this.shardId,
            manager: this.hookAPI,
            data: {
                fromState: oldState,
                toState: { ...oldState, state: toState },
                reason,
            },
        });

        this.state = applyFailoverTransition(this.state, toState, event);
        this.logger.info(
            { shardId: this.shardId, from: event.fromState, to: event.toState, reason },
            "Failover state transition",
        );

        // Persist the state transition to the durable store, if configured.
        if (this.stateStore) {
            try {
                await this.stateStore.transitionState(
                    this.shardId,
                    event.fromState,
                    event.toState,
                    reason,
                    { timestamp: new Date(), initiatedBy: triggeredBy },
                );
            } catch (error) {
                this.logger.warn(
                    { shardId: this.shardId, err: error },
                    "Failed to persist failover state transition",
                );
            }
        }

        // Record the state change event in the event history store, if configured.
        if (this.eventStore) {
            try {
                await this.eventStore.recordEvent({
                    type: "state_change",
                    shardId: this.shardId,
                    nodeId: this.state.currentPrimary ?? undefined,
                    details: { reason, fromState: event.fromState, toState: event.toState },
                    severity: "info",
                    previousState: event.fromState,
                    relatedState: event.toState,
                });
            } catch (error) {
                this.logger.warn(
                    { shardId: this.shardId, err: error },
                    "Failed to record failover event",
                );
            }
        }

        // Invoke afterStateChange hooks.
        await this.hookRunner.invoke("afterStateChange", {
            shardId: this.shardId,
            manager: this.hookAPI,
            data: { fromState: oldState, toState: this.state, reason },
        });

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
            await this.hookRunner.invoke("beforeFailover", {
                shardId: this.shardId,
                manager: this.hookAPI,
                data: { currentPrimary: undefined, reason: "primary unavailable, requesting failover" },
            });

            // Activate fencing on the old primary before failover proceeds.
            const oldPrimary = this.state.currentPrimary;
            if (oldPrimary && this.fencingManager) {
                this.logger.info(
                    { nodeId: oldPrimary, shardId: this.shardId },
                    "Activating fencing on old primary before failover",
                );
                await this.fencingManager.activateAll(
                    oldPrimary,
                    "failover initiated",
                );
            }

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

            // Invalidate connections to the old primary before promoting a replica.
            const currentPrimary = this.state.currentPrimary;
            if (currentPrimary && this.connectionTracker) {
                this.logger.info(
                    { nodeId: currentPrimary, shardId: this.shardId },
                    "Invalidating connections to old primary before failover",
                );
                const result = await this.connectionTracker.invalidateConnections(currentPrimary);
                if (result.activeTransactionCount > 0) {
                    this.logger.warn(
                        { nodeId: currentPrimary, activeTransactions: result.activeTransactionCount },
                        "Active transactions remain on old primary connections",
                    );
                }
            }

            await this.transitionTo("promoting", "failover approved, starting promotion", "failover manager");

            await this.hookRunner.invoke("afterFailover", {
                shardId: this.shardId,
                manager: this.hookAPI,
                data: { newPrimary: undefined, reason: "failover approved" },
            });
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

            await this.hookRunner.invoke("beforePromotion", {
                shardId: this.shardId,
                manager: this.hookAPI,
                data: { replicaId: eligibleReplica.id },
            });

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

            const previousPrimary = this.state.currentPrimary;
            this.state = {
                ...this.state,
                currentPrimary: promoted.id,
                currentGeneration: (this.state.currentGeneration ?? 0) + 1,
            };

            await this.hookRunner.invoke("afterPromotion", {
                shardId: this.shardId,
                manager: this.hookAPI,
                data: { newPrimary: promoted, previousPrimary: undefined },
            });

            // Reconnect pools after successful promotion.
            if (this.poolReconnector) {
                await this.poolReconnector.reconnectPrimaryPool(promoted.id);
                try {
                    const topology = await this.provider.discoverTopology(this.shardId);
                    const replicaIds = topology.replicas.map((r) => r.id);
                    if (replicaIds.length > 0) {
                        await this.poolReconnector.reconnectReplicaPools(replicaIds);
                    }
                } catch (error) {
                    this.logger.warn(
                        { shardId: this.shardId, err: error },
                        "Failed to discover topology for replica pool reconnection",
                    );
                }
            }

            await this.transitionTo("verifying", `replica "${eligibleReplica.id}" promoted`, "failover manager");
        } catch (error) {
            const promotionError = error instanceof Error ? error : new Error(String(error));
            this.logger.error(
                { shardId: this.shardId, err: promotionError },
                "Promotion failed",
            );
            await this.hookRunner.invoke("promotionFailed", {
                shardId: this.shardId,
                manager: this.hookAPI,
                data: { reason: promotionError.message },
            });
            await this.transitionTo("failed", `promotion failed: ${promotionError.message}`, "failover manager");
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

    /**
     * Register a connection with the tracker so it can be invalidated on failover.
     * Callers should invoke this when acquiring a client from the pool.
     */
    registerConnection(nodeId: string, shardId: string, client: PoolClient): void {
        if (this.connectionTracker) {
            this.connectionTracker.registerConnection(nodeId, shardId, client);
        }
    }

    /**
     * Unregister a connection from the tracker, e.g. when releasing it to the pool.
     */
    unregisterConnection(client: PoolClient): boolean {
        return this.connectionTracker?.unregisterConnection(client) ?? false;
    }

    /**
     * Mark a tracked connection as participating in an active transaction.
     * Connections in transactions are not forcibly closed during invalidation.
     */
    markInTransaction(client: PoolClient, transactionId: string): void {
        this.connectionTracker?.markInTransaction(client, transactionId);
    }

    /**
     * Mark a tracked connection as no longer in a transaction.
     */
    markOutOfTransaction(client: PoolClient): void {
        this.connectionTracker?.markOutOfTransaction(client);
    }

    /**
     * Touch a tracked connection's last-used timestamp.
     */
    touch(client: PoolClient): void {
        this.connectionTracker?.touch(client);
    }

    /**
     * Invalidate all connections to the current primary (if any).
     * Returns the invalidation result.
     */
    async invalidatePrimaryConnections(): Promise<ConnectionInvalidationResult | undefined> {
        const currentPrimary = this.state.currentPrimary;
        if (!currentPrimary || !this.connectionTracker) return undefined;
        return this.connectionTracker.invalidateConnections(currentPrimary);
    }

    /**
     * Get the total number of tracked connections across all nodes.
     */
    getConnectionCount(): number {
        return this.connectionTracker?.getConnectionCount() ?? 0;
    }

    /**
     * Check whether a node is fenced. Callers can invoke this before executing
     * queries on a node to avoid sending work to a fenced-out node.
     * Returns false when no fencing manager is configured.
     */
    async isFenced(nodeId: string): Promise<boolean> {
        if (!this.fencingManager) return false;
        return this.fencingManager.isFenced(nodeId);
    }

    /**
     * Get the fencing status for a node, if a fencing manager is configured.
     */
    async getFencingStatus(nodeId: string): Promise<{ nodeId: string; fenced: boolean; reason?: string } | undefined> {
        if (!this.fencingManager) return undefined;
        const status = await this.fencingManager.getStatus(nodeId);
        return {
            nodeId: status.nodeId,
            fenced: status.fenced,
            reason: status.reason,
        };
    }

    async shutdown(): Promise<void> {
        this.closed = true;
        if (this.healthCheckTimer) {
            clearInterval(this.healthCheckTimer);
            this.healthCheckTimer = undefined;
        }
        this.connectionTracker?.stop();
        await this.provider.shutdown().catch(() => {});
    }
}
