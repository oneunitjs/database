import { Pool } from "pg";
import type { ReadPreference, ReplicationEndpointHealth } from "../types.js";
import {
    PrimaryUnavailableError,
    ReadPreferenceError,
    ReplicaUnavailableError,
    isReplicaUnavailableError,
    isPrimaryUnavailableError,
} from "./errors.js";

export interface ReplicationEndpoint {
    endpointId: string;
    role: "primary" | "replica";
    pool: Pool;
    health?: ReplicationEndpointHealth;
}

export interface ReplicationRouterOptions {
    primary: ReplicationEndpoint;
    replicas: ReplicationEndpoint[];
    mode: ReadPreference;
    lagThresholdMs?: number;
}

const VALID_PREFERENCES = new Set<ReadPreference>([
    "primary",
    "replica-preferred",
    "replica-only",
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

    constructor(options: ReplicationRouterOptions) {
        this.primary = options.primary;
        this.replicas = options.replicas;
        this.defaultMode = validateReadPreference(options.mode);
        this.lagThresholdMs = options.lagThresholdMs;
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
     * - `"replica-preferred"` → first eligible replica, else primary.
     * - `"replica-only"` → first eligible replica, else ReplicaUnavailableError.
     */
    routeRead(readPreference?: ReadPreference): ReplicationEndpoint {
        const preference = readPreference === undefined ? this.defaultMode : validateReadPreference(readPreference);

        if (preference === "primary") {
            return this.primary;
        }

        const eligible = this.replicas.filter((r) => isReplicaEligible(r, this.lagThresholdMs));

        if (eligible.length > 0) {
            return eligible[0];
        }

        if (preference === "replica-only") {
            throw new ReplicaUnavailableError(
                "No eligible replica available for replica-only read",
            );
        }

        // replica-preferred: fall back to primary
        return this.primary;
    }

    /**
     * Resolve the endpoint for an operation, asserting it never targets a
     * replica when the operation is a write.
     */
    routeForIntent(
        intent: "read" | "write",
        readPreference?: ReadPreference,
    ): ReplicationEndpoint {
        if (intent === "write") {
            return this.routeWrite();
        }
        return this.routeRead(readPreference);
    }
}

export {
    isReplicaUnavailableError,
    isPrimaryUnavailableError,
};
