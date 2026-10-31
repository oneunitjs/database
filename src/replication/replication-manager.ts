import { Pool } from "pg";
import type { PoolConfig } from "pg";
import type {
    Logger,
    ReadPreference,
    ReplicationConfig,
    ReplicationEndpointHealth,
    ReplicationHealthResult,
    ReplicationHealthCheckConfig,
} from "../types.js";
import { createCheck } from "../client/check.js";
import {
    PrimaryUnavailableError,
    ReplicationConfigError,
    WriteToReplicaError,
} from "./errors.js";
import { ReplicationRouter } from "./replication-router.js";

const DEFAULT_PRIMARY_ENDPOINT_ID = "primary";
const DEFAULT_LAG_QUERY =
    "SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000 AS lag_ms";

const DEFAULT_HEALTH_CHECK: Required<ReplicationHealthCheckConfig> = {
    enabled: true,
    intervalMs: 30_000,
    timeoutMs: 5_000,
    lagQuery: DEFAULT_LAG_QUERY,
};

interface EndpointEntry {
    endpointId: string;
    role: "primary" | "replica";
    pool: Pool;
    connection: PoolConfig;
    owned: boolean;
}

interface EndpointLifecycle {
    activeOps: number;
    draining: boolean;
}

/**
 * Simple promise-based mutex mirroring ShardManager's, used to serialize
 * addReplica / removeReplica so concurrent topology changes cannot corrupt
 * endpoint state.
 */
class Mutex {
    private locked = false;
    private queue: (() => void)[] = [];

    async acquire(): Promise<void> {
        if (!this.locked) {
            this.locked = true;
            return;
        }
        await new Promise<void>((resolve) => this.queue.push(resolve));
    }

    release(): void {
        const next = this.queue.shift();
        if (next) {
            next();
        } else {
            this.locked = false;
        }
    }
}

function validatePoolConfig(connection: PoolConfig, endpointId: string): void {
    if (!connection) {
        throw new ReplicationConfigError(
            `Endpoint "${endpointId}" is missing connection configuration`,
        );
    }
    if (
        !connection.connectionString &&
        !connection.host
    ) {
        throw new ReplicationConfigError(
            `Endpoint "${endpointId}" connection must specify connectionString or host`,
        );
    }
    const max = connection.max;
    if (max !== undefined && (typeof max !== "number" || !Number.isFinite(max) || max < 0)) {
        throw new ReplicationConfigError(
            `Endpoint "${endpointId}" pool max must be a non-negative number`,
        );
    }
    const min = connection.min;
    if (min !== undefined && (typeof min !== "number" || !Number.isFinite(min) || min < 0)) {
        throw new ReplicationConfigError(
            `Endpoint "${endpointId}" pool min must be a non-negative number`,
        );
    }
    if (connection.ssl && connection.ssl !== true) {
        const ssl = connection.ssl as { ca?: string; rejectUnauthorized?: boolean };
        if (ssl.ca !== undefined && (typeof ssl.ca !== "string" || ssl.ca.trim().length === 0)) {
            throw new ReplicationConfigError(
                `Endpoint "${endpointId}" ssl.ca must be a non-empty string`,
            );
        }
    }
}

function validateReplicationConfig(config: ReplicationConfig): void {
    if (!config) {
        throw new ReplicationConfigError("Replication configuration is required");
    }
    if (!config.enabled) {
        return;
    }
    const validModes = new Set<ReadPreference>([
        "primary",
        "replica-preferred",
        "replica-only",
    ]);
    if (!validModes.has(config.mode)) {
        throw new ReplicationConfigError(
            `Invalid replication mode "${config.mode}". Expected "primary", "replica-preferred", or "replica-only".`,
        );
    }
    const seen = new Set<string>();
    const primaryId = config.primary?.id ?? DEFAULT_PRIMARY_ENDPOINT_ID;
    seen.add(primaryId);

    if (config.primary) {
        validatePoolConfig(config.primary.connection, config.primary.id || DEFAULT_PRIMARY_ENDPOINT_ID);
    }

    for (const replica of config.replicas) {
        if (!replica.id || replica.id.trim().length === 0) {
            throw new ReplicationConfigError("Replica endpoint id must be a non-empty string");
        }
        if (seen.has(replica.id)) {
            throw new ReplicationConfigError(`Duplicate endpoint id: ${replica.id}`);
        }
        seen.add(replica.id);
        validatePoolConfig(replica.connection, replica.id);
    }

    if (
        config.lagThresholdMs !== undefined &&
        (typeof config.lagThresholdMs !== "number" || !Number.isFinite(config.lagThresholdMs) || config.lagThresholdMs < 0)
    ) {
        throw new ReplicationConfigError("lagThresholdMs must be a non-negative number");
    }
}

export interface ReplicationManagerOptions {
    primary: {
        endpointId: string;
        pool: Pool;
        connection: PoolConfig;
        /** Whether this manager owns (and should close) the primary pool. */
        owned?: boolean;
    };
    /** Replica endpoints keyed by their `id` (matches ReplicationEndpointConfig). */
    replicas: Array<{
        id: string;
        connection: PoolConfig;
    }>;
    mode: ReadPreference;
    lagThresholdMs?: number;
    healthCheck?: ReplicationHealthCheckConfig;
    logger?: Logger;
}

/**
 * Manages the connection pools and health for one logical database's
 * replication topology: one primary pool plus zero or more replica pools.
 *
 * The primary pool may be supplied externally (e.g. owned by ShardManager for
 * a sharded database); only pools created here are closed on shutdown.
 */
export class ReplicationManager {
    private readonly endpoints = new Map<string, EndpointEntry>();
    private readonly healthStates = new Map<string, ReplicationEndpointHealth>();
    private readonly lifecycle = new Map<string, EndpointLifecycle>();
    private readonly checks = new Map<string, ReturnType<typeof createCheck>>;
    private router: ReplicationRouter;
    private readonly logger?: Logger;
    private closed = false;
    private readonly mutex = new Mutex();

    constructor(private readonly options: ReplicationManagerOptions) {
        this.logger = options.logger;
        const healthCheck = { ...DEFAULT_HEALTH_CHECK, ...options.healthCheck };

        validateReplicationConfig({
            enabled: true,
            mode: options.mode,
            primary: {
                id: options.primary.endpointId,
                connection: options.primary.connection,
            },
            replicas: options.replicas.map((r) => ({
                id: r.id,
                connection: r.connection,
            })),
            lagThresholdMs: options.lagThresholdMs,
            healthCheck,
        });

        this.router = new ReplicationRouter({
            primary: {
                endpointId: options.primary.endpointId,
                role: "primary",
                pool: options.primary.pool,
            },
            replicas: [],
            mode: options.mode,
            lagThresholdMs: options.lagThresholdMs,
        });

        // Register primary endpoint (pool supplied externally; not owned unless configured)
        this.endpoints.set(options.primary.endpointId, {
            endpointId: options.primary.endpointId,
            role: "primary",
            pool: options.primary.pool,
            connection: options.primary.connection,
            owned: options.primary.owned === true,
        });
        this.lifecycle.set(options.primary.endpointId, { activeOps: 0, draining: false });

        for (const replica of options.replicas) {
            this.endpoints.set(replica.id, {
                endpointId: replica.id,
                role: "replica",
                pool: null as unknown as Pool,
                connection: replica.connection,
                owned: true,
            });
            this.lifecycle.set(replica.id, { activeOps: 0, draining: false });
        }
    }

    get primaryEndpointId(): string {
        return this.options.primary.endpointId;
    }

    get replicaEndpointIds(): ReadonlyArray<string> {
        return Array.from(this.endpoints.keys()).filter(
            (id) => this.endpoints.get(id)?.role === "replica",
        );
    }

    get enabled(): boolean {
        return true;
    }

    get mode(): ReadPreference {
        return this.options.mode;
    }

    get isClosed(): boolean {
        return this.closed;
    }

    get primaryPool(): Pool {
        return this.options.primary.pool;
    }

    /**
     * Create replica pools synchronously. `new Pool()` is lazy and does not
     * connect until the first query, so no `await` is required here.
     * Throws ReplicationConfigError on invalid config; cleans up any pools
     * created before a failure.
     */
    initialize(): void {
        const errors: Error[] = [];
        const createdPools: Pool[] = [];

        for (const replica of this.options.replicas) {
            const entry = this.endpoints.get(replica.id);
            if (!entry || entry.role !== "replica") {
                continue;
            }
            if (entry.pool) {
                continue;
            }
            try {
                const pool = new Pool(replica.connection);
                createdPools.push(pool);
                entry.pool = pool;
                this.checks.set(replica.id, createCheck(pool));

                pool.on("error", (error) => {
                    this.logger?.error?.(
                        {
                            endpointId: replica.id,
                            role: "replica",
                            err: error,
                        },
                        "Replica pool error",
                    );
                    this.recordFailure(replica.id, error);
                });

                this.logger?.info?.(
                    { endpointId: replica.id, role: "replica" },
                    "Replica pool initialized",
                );
            } catch (error) {
                const err = error instanceof Error ? error : new Error(String(error));
                this.logger?.error?.(
                    { endpointId: replica.id, role: "replica", err: error },
                    "Failed to initialize replica pool",
                );
                errors.push(err);
            }
        }

        if (errors.length > 0) {
            // Clean up replica pools that were created before the failure
            for (const pool of createdPools) {
                try {
                    pool.end();
                } catch {
                    // Ignore cleanup errors
                }
            }
            throw new ReplicationConfigError(
                `Failed to initialize ${errors.length} replica(s): ${errors
                    .map((e) => e.message)
                    .join("; ")}`,
                { cause: errors[0] },
            );
        }

        this.syncRouter();
    }

    private getEntry(endpointId: string): EndpointEntry | undefined {
        return this.endpoints.get(endpointId);
    }

    /**
     * Return the endpoint to read from for the given preference, or null if
     * none is eligible. Does not acquire a connection.
     */
    readEndpoint(readPreference?: ReadPreference): {
        endpointId: string;
        role: "primary" | "replica";
        pool: Pool;
    } | null {
        if (this.closed) {
            return null;
        }
        const resolved = this.router.routeRead(readPreference);
        return {
            endpointId: resolved.endpointId,
            role: resolved.role,
            pool: resolved.pool,
        };
    }

    /**
     * Return the primary endpoint for writes. Throws PrimaryUnavailableError
     * when the primary is down. Never returns a replica.
     */
    writeEndpoint(): {
        endpointId: string;
        role: "primary";
        pool: Pool;
    } {
        const resolved = this.router.routeWrite();
        if (resolved.role !== "primary") {
            throw new WriteToReplicaError(
                `Write routed to replica endpoint "${resolved.endpointId}"`,
            );
        }
        return {
            endpointId: resolved.endpointId,
            role: "primary",
            pool: resolved.pool,
        };
    }

    /**
     * Acquire an endpoint for an operation, incrementing active operations.
     * Call releaseEndpoint when done.
     */
    async acquireEndpoint(endpointId: string): Promise<void> {
        if (this.closed) {
            throw new PrimaryUnavailableError("Replication manager is shut down");
        }
        const lifecycle = this.lifecycle.get(endpointId);
        if (!lifecycle) {
            throw new ReplicationConfigError(`Endpoint "${endpointId}" does not exist`);
        }
        if (lifecycle.draining) {
            throw new ReplicationConfigError(
                `Endpoint "${endpointId}" is draining and cannot accept new operations`,
            );
        }
        lifecycle.activeOps += 1;
    }

    releaseEndpoint(endpointId: string): void {
        const lifecycle = this.lifecycle.get(endpointId);
        if (!lifecycle) {
            return;
        }
        if (lifecycle.activeOps > 0) {
            lifecycle.activeOps -= 1;
        }
    }

    recordFailure(endpointId: string, error: Error): void {
        const state = this.healthStates.get(endpointId);
        if (!state) {
            return;
        }
        state.consecutiveFailures += 1;
        state.error = error.message;
        state.errorCode = undefined;
        state.lastCheckedAt = new Date().toISOString();
        this.logger?.warn?.(
            {
                endpointId,
                consecutiveFailures: state.consecutiveFailures,
                err: error,
            },
            "Replication endpoint failure recorded",
        );
    }

    recordSuccess(endpointId: string, _latencyMs: number): void {
        const state = this.healthStates.get(endpointId);
        if (!state) {
            return;
        }
        state.consecutiveFailures = 0;
        state.error = undefined;
        state.errorCode = undefined;
        state.lastCheckedAt = new Date().toISOString();
    }

    /**
     * Health-check a single endpoint. For replicas this also measures
     * replication lag. Results never throw.
     */
    async endpointHealth(endpointId: string): Promise<ReplicationEndpointHealth | undefined> {
        const entry = this.getEntry(endpointId);
        if (!entry) {
            return undefined;
        }

        const start = Date.now();
        let status: "up" | "down" = "up";
        let latencyMs = 0;
        let error: string | undefined;
        let errorCode: string | undefined;
        let lagMs: number | null = null;
        let lagUnknown = true;

        try {
            const check =
                this.checks.get(endpointId) ?? createCheck(entry.pool);
            if (!this.checks.has(endpointId)) {
                this.checks.set(endpointId, check);
            }
            const result = await check.check({
                timeout: this.options.healthCheck?.timeoutMs,
            });
            latencyMs = result.latency.value;
            if (result.status === "down") {
                status = "down";
                error = result.error;
                errorCode = result.code;
            }
        } catch (err) {
            status = "down";
            latencyMs = Date.now() - start;
            error = err instanceof Error ? err.message : String(err);
            errorCode = (err as { code?: string }).code;
        }

        // Measure replication lag on replicas only.
        if (status === "up" && entry.role === "replica") {
            const lagQuery =
                this.options.healthCheck?.lagQuery ?? DEFAULT_LAG_QUERY;
            try {
                const result = await entry.pool.query<{
                    lag_ms: string | null;
                }>(lagQuery);
                const raw = result.rows?.[0]?.lag_ms ?? null;
                lagMs = raw === null || raw === undefined ? null : Number(raw);
                lagUnknown = lagMs === null || !Number.isFinite(lagMs);
                if (!lagUnknown && lagMs !== null && lagMs < 0) {
                    lagMs = 0;
                }
            } catch {
                // Lag could not be measured — keep lagUnknown true; the
                // endpoint may still be healthy for connectivity purposes.
                lagMs = null;
                lagUnknown = true;
            }
        } else if (entry.role === "primary") {
            lagUnknown = true;
        }

        if (status === "up") {
            this.recordSuccess(endpointId, latencyMs);
        } else {
            this.recordFailure(endpointId, new Error(error ?? "unknown error"));
        }

        const healthState: ReplicationEndpointHealth = {
            endpointId,
            role: entry.role,
            status,
            latencyMs,
            lagMs: entry.role === "replica" ? lagMs : null,
            lagUnknown: entry.role === "replica" ? lagUnknown : true,
            lastCheckedAt: new Date().toISOString(),
            consecutiveFailures: this.healthStates.get(endpointId)?.consecutiveFailures ?? 0,
            error,
            errorCode,
        };
        this.healthStates.set(endpointId, healthState);

        // Rebuild router so read routing sees updated health / lag
        this.syncRouter();
        return healthState;
    }

    /**
     * Health-check every endpoint (primary + replicas). Always resolves.
     */
    async health(): Promise<ReplicationHealthResult> {
        const checkedAt = new Date().toISOString();
        const primaryHealth =
            (await this.endpointHealth(this.options.primary.endpointId)) ??
            this.healthStates.get(this.options.primary.endpointId);
        const replicaResults: ReplicationEndpointHealth[] = [];
        const downReplicas: string[] = [];
        const upReplicas: string[] = [];
        const laggingReplicas: string[] = [];

        for (const endpointId of this.replicaEndpointIds) {
            const result = await this.endpointHealth(endpointId);
            if (!result) {
                continue;
            }
            replicaResults.push(result);
            if (result.status === "down") {
                downReplicas.push(endpointId);
            } else {
                upReplicas.push(endpointId);
                const lagMs = result.lagMs;
                if (
                    lagMs !== null &&
                    this.options.lagThresholdMs !== undefined &&
                    lagMs > this.options.lagThresholdMs
                ) {
                    laggingReplicas.push(endpointId);
                }
            }
        }

        const healthy =
            (!primaryHealth || primaryHealth.status === "up") && downReplicas.length === 0;

        return {
            healthy,
            checkedAt,
            primary: primaryHealth,
            replicas: replicaResults,
            downReplicas,
            upReplicas,
            laggingReplicas,
        };
    }

    /**
     * Get the last reported health for an endpoint without running a check.
     */
    getEndpointHealth(endpointId: string): ReplicationEndpointHealth | undefined {
        return this.healthStates.get(endpointId);
    }

    getConsecutiveFailures(endpointId: string): number {
        const state = this.healthStates.get(endpointId);
        return state?.consecutiveFailures ?? 0;
    }

    private syncRouter(): void {
        const replicaEndpoints = this.replicaEndpointIds.map((id) => {
            const entry = this.endpoints.get(id)!;
            return {
                endpointId: id,
                role: "replica" as const,
                pool: entry.pool!,
                health: this.healthStates.get(id),
            };
        });
        const primaryEntry = this.endpoints.get(this.options.primary.endpointId)!;
        this.router = new ReplicationRouter({
            primary: {
                endpointId: primaryEntry.endpointId,
                role: "primary",
                pool: primaryEntry.pool!,
                health: this.healthStates.get(this.options.primary.endpointId),
            },
            replicas: replicaEndpoints,
            mode: this.options.mode,
            lagThresholdMs: this.options.lagThresholdMs,
        });
    }

    /**
     * Add a replica at runtime. Serialized via mutex. Validates the config and
     * creates its pool.
     */
    async addReplica(config: { id: string; connection: PoolConfig }): Promise<void> {
        await this.mutex.acquire();
        try {
            return await this.addReplicaUnlocked(config);
        } finally {
            this.mutex.release();
        }
    }

    private async addReplicaUnlocked(config: {
        id: string;
        connection: PoolConfig;
    }): Promise<void> {
        if (this.closed) {
            throw new ReplicationConfigError("Cannot add replica: manager is shut down");
        }
        if (!config.id || config.id.trim().length === 0) {
            throw new ReplicationConfigError("Replica endpoint id must be a non-empty string");
        }
        if (this.endpoints.has(config.id)) {
            throw new ReplicationConfigError(`Endpoint "${config.id}" already exists`);
        }
        validatePoolConfig(config.connection, config.id);

        validateReplicationConfig({
            enabled: true,
            mode: this.options.mode,
            primary: {
                id: this.options.primary.endpointId,
                connection: this.options.primary.connection,
            },
            replicas: [
                ...this.options.replicas.map((r) => ({
                    id: r.id,
                    connection: r.connection,
                })),
                { id: config.id, connection: config.connection },
            ],
            lagThresholdMs: this.options.lagThresholdMs,
        });

        const lifecycle: EndpointLifecycle = { activeOps: 0, draining: false };
        this.lifecycle.set(config.id, lifecycle);

        let poolCreated = false;
        try {
            const pool = new Pool(config.connection);
            poolCreated = true;
            this.endpoints.set(config.id, {
                endpointId: config.id,
                role: "replica",
                pool,
                connection: config.connection,
                owned: true,
            });
            this.checks.set(config.id, createCheck(pool));

            pool.on("error", (error) => {
                this.logger?.error?.(
                    { endpointId: config.id, role: "replica", err: error },
                    "Replica pool error",
                );
                this.recordFailure(config.id, error);
            });

            this.options.replicas.push({ id: config.id, connection: config.connection });
            this.syncRouter();

            this.logger?.info?.(
                { endpointId: config.id, role: "replica" },
                "Replica added",
            );
        } catch (error) {
            this.lifecycle.delete(config.id);
            if (poolCreated) {
                try {
                    const p = this.endpoints.get(config.id)?.pool;
                    if (p) {
                        await p.end();
                    }
                } catch {
                    // Ignore cleanup errors
                }
                this.endpoints.delete(config.id);
            }
            const err = error instanceof Error ? error : new Error(String(error));
            throw new ReplicationConfigError(
                `Failed to add replica "${config.id}": ${err.message}`,
                { cause: err },
            );
        }
    }

    /**
     * Remove a replica at runtime. Marks it draining, waits for active
     * operations to complete (up to 30s), then closes its pool. Serialized
     * via mutex.
     */
    async removeReplica(endpointId: string): Promise<void> {
        await this.mutex.acquire();
        try {
            return await this.removeReplicaUnlocked(endpointId);
        } finally {
            this.mutex.release();
        }
    }

    private async removeReplicaUnlocked(endpointId: string): Promise<void> {
        const entry = this.endpoints.get(endpointId);
        if (!entry || entry.role !== "replica") {
            throw new ReplicationConfigError(`Replica "${endpointId}" does not exist`);
        }
        if (this.closed) {
            throw new ReplicationConfigError("Cannot remove replica: manager is shut down");
        }

        const lifecycle = this.lifecycle.get(endpointId);
        if (lifecycle) {
            lifecycle.draining = true;
        }

        // Wait for active operations to complete (up to 30 seconds)
        const maxWaitMs = 30000;
        const startTime = Date.now();
        while (true) {
            const currentOps = this.lifecycle.get(endpointId)?.activeOps ?? 0;
            if (currentOps === 0) {
                break;
            }
            if (Date.now() - startTime > maxWaitMs) {
                if (lifecycle) {
                    lifecycle.draining = false;
                }
                throw new ReplicationConfigError(
                    `Failed to remove replica "${endpointId}": ${currentOps} operation(s) still in flight after ${maxWaitMs}ms`,
                );
            }
            await new Promise((resolve) => setTimeout(resolve, 50));
        }

        let poolEnded = false;
        try {
            await entry.pool?.end();
            poolEnded = true;
            if (entry.owned) {
                this.endpoints.delete(endpointId);
            } else {
                entry.pool = null as unknown as Pool;
            }
            this.checks.delete(endpointId);
            this.healthStates.delete(endpointId);
            this.lifecycle.delete(endpointId);
            this.options.replicas = this.options.replicas.filter(
                (r) => r.id !== endpointId,
            );
            this.syncRouter();

            this.logger?.info?.({ endpointId }, "Replica removed");
        } catch (error) {
            if (!poolEnded) {
                const lifecycle = this.lifecycle.get(endpointId);
                if (lifecycle) {
                    lifecycle.draining = false;
                }
            }
            const err = error instanceof Error ? error : new Error(String(error));
            throw new ReplicationConfigError(
                `Failed to remove replica "${endpointId}": ${err.message}`,
                { cause: err },
            );
        }
    }

    getEndpoint(endpointId: string): {
        endpointId: string;
        role: "primary" | "replica";
        pool: Pool;
    } | undefined {
        const entry = this.endpoints.get(endpointId);
        if (!entry || !entry.pool) {
            return undefined;
        }
        return { endpointId: entry.endpointId, role: entry.role, pool: entry.pool };
    }

    getStats(): Record<string, { total: number; idle: number; waiting: number }> {
        const stats: Record<string, { total: number; idle: number; waiting: number }> = {};
        for (const [endpointId, entry] of this.endpoints) {
            if (!entry.pool) {
                continue;
            }
            stats[endpointId] = {
                total: entry.pool.totalCount,
                idle: entry.pool.idleCount,
                waiting: entry.pool.waitingCount,
            };
        }
        return stats;
    }

    async shutdown(): Promise<void> {
        if (this.closed) {
            return;
        }
        this.closed = true;

        for (const entry of this.endpoints.values()) {
            if (!entry.owned || !entry.pool) {
                continue;
            }
            const lifecycle = this.lifecycle.get(entry.endpointId);
            if (lifecycle) {
                lifecycle.draining = true;
            }
        }

        const errors: Error[] = [];
        for (const [endpointId, entry] of this.endpoints) {
            if (!entry.owned || !entry.pool) {
                continue;
            }
            try {
                await entry.pool.end();
                this.logger?.info?.(
                    { endpointId, role: entry.role },
                    "Replication endpoint pool shut down",
                );
            } catch (error) {
                errors.push(
                    error instanceof Error ? error : new Error(String(error)),
                );
                this.logger?.error?.(
                    { endpointId, role: entry.role, err: error },
                    "Failed to shut down replication endpoint pool",
                );
            }
        }

        for (const entry of this.endpoints.values()) {
            if (entry.owned) {
                entry.pool = null as unknown as Pool;
            }
        }
        this.endpoints.clear();

        if (errors.length > 0) {
            throw new Error(
                `Failed to shut down ${errors.length} replication endpoint pool(s)`,
                { cause: errors[0] },
            );
        }
    }
}

export { validateReplicationConfig };
