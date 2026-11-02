import { Pool } from "pg";
import type { ReadPreference, ReplicationEndpointHealth } from "../types.js";
import {
    PrimaryUnavailableError,
    ReadPreferenceError,
    ReplicaUnavailableError,
    isReplicaUnavailableError,
    isPrimaryUnavailableError,
} from "./errors.js";
import {
    createReplicaSelector,
    type ReplicaSelectionPolicy,
    type ReplicaSelector,
    type WeightedReplicaSelectionOptions,
} from "./replica-selectors.js";
import { ReplicationConnectionTracker } from "./replication-connection-tracker.js";
import {
    ReplicaSelectionStatsTracker,
    type ReplicaSelectionStats,
    type ReplicaSelectionEvent,
} from "./replication-stats.js";
import {
    ReadRoutingHookRunner,
    type ReadRoutingHook,
    type ReadRoutingHookType,
    type ReadRoutingHooks,
    type ReplicationRouterHookAPI,
} from "./replication-hooks.js";

export interface ReplicationEndpoint {
    endpointId: string;
    role: "primary" | "replica";
    pool: Pool;
    health?: ReplicationEndpointHealth;
    /** Optional selection weight used by the weighted policy. */
    weight?: number;
}

export interface ReplicationRouterOptions {
    primary: ReplicationEndpoint;
    replicas: ReplicationEndpoint[];
    mode: ReadPreference;
    lagThresholdMs?: number;
    /** Replica selection policy. Defaults to `"first"` (first eligible). */
    replicaSelectionPolicy?: ReplicaSelectionPolicy;
    weightedOptions?: Partial<WeightedReplicaSelectionOptions>;
    connectionTracker?: ReplicationConnectionTracker;
    statsTracker?: ReplicaSelectionStatsTracker;
    hooks?: ReadRoutingHooks | ReadRoutingHookRunner;
}

const VALID_PREFERENCES = new Set<ReadPreference>([
    "primary",
    "replica-preferred",
    "replica-only",
]);

const VALID_POLICIES = new Set<ReplicaSelectionPolicy>([
    "first",
    "weighted",
    "round-robin",
    "least-connections",
]);

function validateReadPreference(value: unknown): ReadPreference {
    if (value === undefined || value === null) {
        return "primary";
    }
    if (typeof value !== "string" || !VALID_PREFERENCES.has(value as ReadPreference)) {
        throw new ReadPreferenceError(
            `Invalid readPreference "${String(value)}". Expected "primary", "replica-preferred", or "replica-only".`,
        );
    }
    return value as ReadPreference;
}

function validateSelectionPolicy(value: unknown): ReplicaSelectionPolicy {
    if (value === undefined || value === null) {
        return "first";
    }
    if (typeof value !== "string" || !VALID_POLICIES.has(value as ReplicaSelectionPolicy)) {
        return "first";
    }
    return value as ReplicaSelectionPolicy;
}

/**
 * Determines whether a replica endpoint is currently eligible for read
 * routing. A replica is eligible when it is up and its known lag (if any) does
 * not exceed the configured threshold. Unknown lag is not treated as excessive.
 *
 * The primary is always eligible.
 */
export function isReplicaEligible(
    endpoint: ReplicationEndpoint,
    lagThresholdMs?: number,
): boolean {
    const health = endpoint.health;
    if (!health || health.status !== "up") {
        return false;
    }
    if (health.lagMs !== null) {
        if (lagThresholdMs !== undefined && Number.isFinite(lagThresholdMs)) {
            if (health.lagMs > lagThresholdMs) {
                return false;
            }
        }
    }
    return true;
}

export function isReadPreference(value: unknown): value is ReadPreference {
    return typeof value === "string" && VALID_PREFERENCES.has(value as ReadPreference);
}

/**
 * Pure routing logic for primary / replica replication. Never inspects SQL
 * text to infer intent — routing is driven entirely by the explicit read
 * preference supplied by the caller. Writes always target the primary.
 */
export class ReplicationRouter {
    private readonly primary: ReplicationEndpoint;
    private readonly replicas: ReplicationEndpoint[];
    private readonly defaultMode: ReadPreference;
    private readonly lagThresholdMs: number | undefined;
    private readonly defaultPolicy: ReplicaSelectionPolicy;
    private readonly selectors = new Map<ReplicaSelectionPolicy, ReplicaSelector>();
    private readonly connectionTracker: ReplicationConnectionTracker;
    private readonly statsTracker: ReplicaSelectionStatsTracker;
    private readonly hookRunner: ReadRoutingHookRunner;
    private readonly hookAPI: ReplicationRouterHookAPI;

    constructor(options: ReplicationRouterOptions) {
        this.primary = options.primary;
        this.replicas = options.replicas;
        this.defaultMode = validateReadPreference(options.mode);
        this.lagThresholdMs = options.lagThresholdMs;
        this.defaultPolicy = validateSelectionPolicy(options.replicaSelectionPolicy);
        this.connectionTracker = options.connectionTracker ?? new ReplicationConnectionTracker();
        this.statsTracker = options.statsTracker ?? new ReplicaSelectionStatsTracker();
        this.hookRunner =
            options.hooks instanceof ReadRoutingHookRunner
                ? options.hooks
                : new ReadRoutingHookRunner(options.hooks ?? {});

        const selectorOptions = {
            weighted: options.weightedOptions,
            connectionTracker: this.connectionTracker,
        };
        this.selectors.set("first", createReplicaSelector("first", selectorOptions));
        this.selectors.set("weighted", createReplicaSelector("weighted", selectorOptions));
        this.selectors.set("round-robin", createReplicaSelector("round-robin", selectorOptions));
        this.selectors.set(
            "least-connections",
            createReplicaSelector("least-connections", selectorOptions),
        );

        this.hookAPI = {
            getAvailableReplicas: () => this.getEligibleReplicas().map((r) => r.endpointId),
            getCurrentPolicy: () => this.defaultPolicy,
            getReplicaHealth: (replicaId: string) =>
                this.replicas.find((r) => r.endpointId === replicaId)?.health,
            getSelectionStats: () => this.statsTracker.getStats(),
        };
    }

    get primaryEndpoint(): ReplicationEndpoint {
        return this.primary;
    }

    get replicaEndpoints(): ReadonlyArray<ReplicationEndpoint> {
        return this.replicas;
    }

    get mode(): ReadPreference {
        return this.defaultMode;
    }

    get replicaSelectionPolicy(): ReplicaSelectionPolicy {
        return this.defaultPolicy;
    }

    getEligibleReplicas(): ReplicationEndpoint[] {
        return this.replicas.filter((r) => isReplicaEligible(r, this.lagThresholdMs));
    }

    /**
     * Resolve the endpoint for a write. Always returns the primary. Throws
     * PrimaryUnavailableError when the primary is down. Never returns a
     * replica.
     */
    routeWrite(): ReplicationEndpoint {
        if (this.primary.health && this.primary.health.status !== "up") {
            throw new PrimaryUnavailableError(
                `Primary endpoint "${this.primary.endpointId}" is unavailable for writes`,
            );
        }
        return this.primary;
    }

    /**
     * Resolve the endpoint for a read according to the explicit read
     * preference. Defaults to `"primary"` when no preference is supplied.
     *
     * - `"primary"` → primary.
     * - `"replica-preferred"` → selected eligible replica, else primary.
     * - `"replica-only"` → selected eligible replica, else ReplicaUnavailableError.
     */
    routeRead(
        readPreference?: ReadPreference,
        selectionPolicy?: ReplicaSelectionPolicy,
    ): ReplicationEndpoint {
        const preference = readPreference === undefined ? this.defaultMode : validateReadPreference(readPreference);

        if (preference === "primary") {
            return this.primary;
        }

        const selected = this.selectReplica(selectionPolicy);

        if (selected) {
            return selected;
        }

        if (preference === "replica-only") {
            throw new ReplicaUnavailableError(
                "No eligible replica available for replica-only read",
            );
        }

        return this.primary;
    }

    /**
     * Select an eligible replica using the configured (or supplied) policy.
     * Returns null when no replica is eligible. Does not fall back to primary.
     */
    selectReplica(selectionPolicy?: ReplicaSelectionPolicy): ReplicationEndpoint | null {
        const policy = validateSelectionPolicy(selectionPolicy ?? this.defaultPolicy);
        const eligible = this.getEligibleReplicas();
        const start = Date.now();
        try {
            const selector = this.selectors.get(policy) ?? this.selectors.get("first")!;
            const selectedId = selector.selectReplica(eligible)?.endpointId ?? null;
            const selected = selectedId
                ? eligible.find((r) => r.endpointId === selectedId) ?? null
                : null;
            this.statsTracker.recordSelection({
                timestamp: new Date(),
                replicaId: selected?.endpointId ?? "none",
                policy,
                success: selected !== null,
                latencyMs: Date.now() - start,
                error: selected ? undefined : "no eligible replica",
            });
            return selected;
        } catch (error) {
            this.statsTracker.recordSelection({
                timestamp: new Date(),
                replicaId: "unknown",
                policy,
                success: false,
                latencyMs: Date.now() - start,
                error: error instanceof Error ? error.message : String(error),
            });
            throw error;
        }
    }

    /**
     * Invoke before/after select hooks around replica selection. Callers that
     * need hook dispatch should use this instead of `selectReplica`.
     */
    async selectReplicaWithHooks(
        selectionPolicy?: ReplicaSelectionPolicy,
        query = "",
    ): Promise<ReplicationEndpoint | null> {
        const policy = validateSelectionPolicy(selectionPolicy ?? this.defaultPolicy);
        await this.hookRunner.invoke("beforeSelectReplica", {
            query,
            router: this.hookAPI,
            data: {
                policy,
                availableReplicas: this.getEligibleReplicas().map((r) => r.endpointId),
            },
        });
        const replica = this.selectReplica(policy);
        await this.hookRunner.invoke("afterSelectReplica", {
            query,
            replica: replica ?? undefined,
            router: this.hookAPI,
            data: {
                policy,
                selectedReplica: replica?.endpointId ?? null,
            },
        });
        return replica;
    }

    /**
     * Resolve the endpoint for an operation, asserting it never targets a
     * replica when the operation is a write.
     */
    routeForIntent(
        intent: "read" | "write",
        readPreference?: ReadPreference,
        selectionPolicy?: ReplicaSelectionPolicy,
    ): ReplicationEndpoint {
        if (intent === "write") {
            return this.routeWrite();
        }
        return this.routeRead(readPreference, selectionPolicy);
    }

    acquireConnection(endpointId: string): void {
        this.connectionTracker.acquireConnection(endpointId);
    }

    releaseConnection(endpointId: string): void {
        this.connectionTracker.releaseConnection(endpointId);
    }

    getConnectionCount(endpointId: string): number {
        return this.connectionTracker.getConnectionCount(endpointId);
    }

    getSelectionStats(): ReplicaSelectionStats {
        return this.statsTracker.getStats();
    }

    getSelectionEvents(limit?: number): ReplicaSelectionEvent[] {
        return this.statsTracker.getRecentEvents(limit);
    }

    registerHook<T extends ReadRoutingHookType>(type: T, hook: ReadRoutingHook<T>): void {
        this.hookRunner.registerHook(type, hook);
    }

    unregisterHook<T extends ReadRoutingHookType>(type: T, hook: ReadRoutingHook<T>): boolean {
        return this.hookRunner.unregisterHook(type, hook);
    }

    getHookRunner(): ReadRoutingHookRunner {
        return this.hookRunner;
    }
}

export {
    isReplicaUnavailableError,
    isPrimaryUnavailableError,
};
