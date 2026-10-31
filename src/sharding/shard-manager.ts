import { Pool } from "pg";
import type { PoolConfig } from "pg";
import type { ShardConfig, ShardInfo, Logger, ShardMapVersion, ReshardingHooks } from "../types.js";
import { ShardConfigError } from "./errors.js";

function validateShardConfig(shard: ShardConfig): void {
    if (!shard.id || typeof shard.id !== "string" || shard.id.trim().length === 0) {
        throw new ShardConfigError("Shard id must be a non-empty string");
    }
    if (!shard.connection) {
        throw new ShardConfigError(`Shard "${shard.id}" is missing connection configuration`);
    }
    if (
        !shard.connection.connectionString &&
        !shard.connection.host
    ) {
        throw new ShardConfigError(
            `Shard "${shard.id}" connection must specify connectionString or host`,
        );
    }

    // Validate SSL configuration if present
    if (shard.connection.ssl && shard.connection.ssl !== true) {
        const ssl = shard.connection.ssl as { ca?: string; rejectUnauthorized?: boolean };
        if (ssl.ca !== undefined && (typeof ssl.ca !== "string" || ssl.ca.trim().length === 0)) {
            throw new ShardConfigError(
                `Shard "${shard.id}" ssl.ca must be a non-empty string (file path or certificate contents)`,
            );
        }
        if (ssl.rejectUnauthorized !== undefined && typeof ssl.rejectUnauthorized !== "boolean") {
            throw new ShardConfigError(
                `Shard "${shard.id}" ssl.rejectUnauthorized must be a boolean`,
            );
        }
    }
}

function sanitizeConnectionForLogging(connection: PoolConfig): Record<string, unknown> {
    const safe: Record<string, unknown> = {};
    if (connection.host) safe.host = connection.host;
    if (connection.port) safe.port = connection.port;
    if (connection.database) safe.database = connection.database;
    if (connection.user) safe.user = connection.user;
    // ssl may contain CA certificate contents — never log it
    if (connection.ssl !== undefined && connection.ssl !== false) {
        safe.ssl = connection.ssl === true ? true : { present: true };
    }
    return safe;
}

function generateInstanceId(): string {
    return `inst-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

interface ShardHealthState {
    consecutiveFailures: number;
    lastError?: string;
    lastCheckedAt?: string;
}

interface ShardLifecycleState {
    /** Number of in-flight operations using this shard's pool */
    activeOps: number;
    /** Set of registered operation IDs for distributed drain tracking */
    operationIds: Set<string>;
    /** Drain state machine: ACTIVE | DRAINING | DRAINED | REMOVED */
    state: "ACTIVE" | "DRAINING" | "DRAINED" | "REMOVED";
}

export interface ShardManagerOptions {
    /** Shard configs (redundant with constructor param; kept for options-bag style calls) */
    shards?: ShardConfig[];
    logger?: Logger;
    onError?: (error: Error) => void;
    hooks?: ReshardingHooks;
    /** Unique instance ID for distributed drain coordination. Auto-generated if omitted. */
    instanceId?: string;
    /** Coordination mode for distributed drain. "strict" requires coordination DB,
     * "local-only" uses only local state, "fail-closed" rejects operations when
     * coordination is unavailable. */
    coordinationMode?: "strict" | "local-only" | "fail-closed";
    /** Operations without a heartbeat after this interval are treated as abandoned.
     * Default: 300000ms (5 minutes). */
    abandonedOperationTimeoutMs?: number;
    /**
     * Existing pool pointing at a dedicated coordination database.
     * Takes precedence over `coordination`. The caller retains ownership
     * and must close the pool.
     */
    coordinationPool?: Pool;
    /**
     * Connection config for a dedicated coordination database (independently
     * addressable from shard databases). When set, drain state is stored
     * here instead of on a shard pool. The manager owns and closes this pool.
     */
    coordination?: PoolConfig;
}

/**
 * Simple promise-based mutex for serializing addShard/removeShard operations.
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

export class ShardManager {
    private pools = new Map<string, Pool>();
    private health = new Map<string, ShardHealthState>();
    private lifecycle = new Map<string, ShardLifecycleState>();
    private _shardVersions = new Map<string, ShardMapVersion>();
    private closed = false;
    private _routingVersion: ShardMapVersion = 0;
    private readonly mutex = new Mutex();
    private readonly instanceId: string;
    private readonly drainLocks = new Map<string, Promise<void>>();
    private readonly coordinationMode: "strict" | "local-only" | "fail-closed";
    private readonly abandonedOperationTimeoutMs: number;
    private dedicatedCoordinationPool: Pool | undefined;
    private ownsDedicatedCoordinationPool = false;

    constructor(
        private readonly shardConfigs: ShardConfig[],
        private readonly logger?: Logger,
        private readonly onError?: (error: Error) => void,
        private readonly hooks?: ReshardingHooks,
        options: ShardManagerOptions = {},
    ) {
        this.instanceId = options.instanceId ?? generateInstanceId();
        this.coordinationMode = options.coordinationMode ?? "local-only";
        this.abandonedOperationTimeoutMs =
            options.abandonedOperationTimeoutMs ?? 300_000;
        if (options.coordinationPool) {
            this.dedicatedCoordinationPool = options.coordinationPool;
            this.ownsDedicatedCoordinationPool = false;
        } else if (options.coordination) {
            this.dedicatedCoordinationPool = new Pool(options.coordination);
            this.ownsDedicatedCoordinationPool = true;
            this.dedicatedCoordinationPool.on("error", (error) => {
                this.logger?.error?.(
                    { err: error },
                    "Coordination pool error",
                );
                this.onError?.(error);
            });
        }
        const seen = new Set<string>();
        for (const shard of shardConfigs) {
            if (seen.has(shard.id)) {
                throw new ShardConfigError(
                    `Duplicate shard id: ${shard.id}`,
                );
            }
            seen.add(shard.id);
            validateShardConfig(shard);
            this._shardVersions.set(shard.id, shard.version ?? 1);
            this.health.set(shard.id, {
                consecutiveFailures: 0,
            });
            this.lifecycle.set(shardIdKey(shard.id), {
                activeOps: 0,
                operationIds: new Set(),
                state: "ACTIVE",
            });
        }
        this._routingVersion = shardConfigs.length > 0 ? 1 : 0;
        try {
            this.initialize();
        } catch (error) {
            if (this.ownsDedicatedCoordinationPool) {
                this.dedicatedCoordinationPool?.end().catch(() => {});
                this.dedicatedCoordinationPool = undefined;
                this.ownsDedicatedCoordinationPool = false;
            }
            throw error;
        }
    }

    get shardIds(): string[] {
        return Array.from(this.pools.keys());
    }

    get isClosed(): boolean {
        return this.closed;
    }

    get shardCount(): number {
        return this.pools.size;
    }

    get routingVersion(): ShardMapVersion {
        return this._routingVersion;
    }

    get shardVersions(): ReadonlyMap<string, ShardMapVersion> {
        return this._shardVersions;
    }

    private initialize(): void {
        if (this.pools.size > 0) {
            throw new ShardConfigError("ShardManager pools already initialized");
        }

        const errors: Error[] = [];

        for (const shard of this.shardConfigs) {
            try {
                const pool = new Pool(shard.connection);
                this.pools.set(shard.id, pool);

                pool.on("error", (error) => {
                    this.logger?.error?.(
                        {
                            shardId: shard.id,
                            err: error,
                        },
                        "Shard pool error",
                    );
                    this.onError?.(error);
                    this.recordFailure(shard.id, error);
                });

                this.logger?.info?.(
                    {
                        shardId: shard.id,
                        ...sanitizeConnectionForLogging(shard.connection),
                    },
                    "Shard pool initialized",
                );
            } catch (error) {
                const err = error instanceof Error ? error : new Error(String(error));
                this.logger?.error?.(
                    {
                        shardId: shard.id,
                        err: error,
                    },
                    "Failed to initialize shard pool",
                );
                errors.push(err);
            }
        }

        if (errors.length > 0) {
            // Clean up any pools that were initialized before the failure
            for (const [shardId, pool] of this.pools) {
                try {
                    pool.end();
                } catch {
                    // Ignore cleanup errors
                }
                this.pools.delete(shardId);
            }
            throw new ShardConfigError(
                `Failed to initialize ${errors.length} shard(s): ${errors
                    .map((e) => e.message)
                    .join("; ")}`,
                { cause: errors[0] },
            );
        }
    }

    getShard(shardId: string): ShardInfo | undefined {
        const pool = this.pools.get(shardId);
        if (!pool) {
            return undefined;
        }
        const state = this.health.get(shardId);
        const lifecycle = this.lifecycle.get(shardIdKey(shardId));
        return {
            id: shardId,
            connected: !this.closed && lifecycle?.state !== "DRAINING" && lifecycle?.state !== "REMOVED",
            pool,
            lastCheckedAt: state?.lastCheckedAt,
            lastError: state?.lastError,
            consecutiveFailures: state?.consecutiveFailures ?? 0,
        };
    }

    hasShard(shardId: string): boolean {
        return this.pools.has(shardId);
    }

    /** Return the durable lifecycle state of a shard (ACTIVE/DRAINING/DRAINED/REMOVED). */
    getLifecycleState(shardId: string): "ACTIVE" | "DRAINING" | "DRAINED" | "REMOVED" {
        return this.lifecycle.get(shardIdKey(shardId))?.state ?? "ACTIVE";
    }

    /** Return the local in-flight operation count for a shard. */
    getActiveOps(shardId: string): number {
        return this.lifecycle.get(shardIdKey(shardId))?.activeOps ?? 0;
    }

    /** Return the shard configuration (including optional weight). */
    getShardConfig(shardId: string): ShardConfig | undefined {
        return this.shardConfigs.find((c) => c.id === shardId);
    }

    /**
     * Acquire a shard for an operation. Increments the active operation count.
     * Throws ShardConfigError if the shard doesn't exist, is draining, or the manager is closed.
     * Callers must call releaseShard() when done.
     *
     * Registration is atomic with respect to the distributed drain state:
     * the INSERT into shard_active_ops is conditioned on the shard not being
     * draining, so a stale instance cannot register after draining begins.
     */
    async acquireShard(shardId: string): Promise<void> {
        if (this.closed) {
            throw new ShardConfigError("Cannot acquire shard: manager is shut down");
        }
        const lifecycle = this.lifecycle.get(shardIdKey(shardId));
        if (!lifecycle) {
            throw new ShardConfigError(`Shard "${shardId}" does not exist`);
        }
        if (lifecycle.state === "DRAINING" || lifecycle.state === "REMOVED") {
            throw new ShardConfigError(
                `Shard "${shardId}" is ${lifecycle.state.toLowerCase()} and cannot accept new operations`,
            );
        }

        // Atomic registration: reject if another instance has already marked
        // this shard as draining. A single SQL statement observes a consistent
        // snapshot, so there is no TOCTOU between the drain check and the
        // operation registration.
        const operationId = `${this.instanceId}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
        const coordPool = this.getCoordinationPool(shardId);
        let registered = false;
        if (coordPool) {
            try {
                await this.ensureDrainSchema(coordPool);
                const result = await coordPool.query(
                    `INSERT INTO shard_active_ops (shard_id, instance_id, operation_id, started_at, heartbeat_at)
                     SELECT $1, $2, $3, NOW(), NOW()
                     WHERE NOT EXISTS (
                         SELECT 1 FROM shard_drain_state WHERE shard_id = $1 AND draining = TRUE
                     )`,
                    [shardId, this.instanceId, operationId],
                );
                registered = (result.rowCount ?? 0) > 0;
            } catch {
                if (this.coordinationMode === "fail-closed") {
                    throw new ShardConfigError(
                        `Shard "${shardId}" coordination database unavailable and coordination mode is fail-closed`,
                    );
                }
                // Coordination DB unavailable — fall back to local-only check.
                registered = true;
            }
        } else if (this.coordinationMode === "fail-closed") {
            throw new ShardConfigError(
                `Shard "${shardId}" no coordination database available and coordination mode is fail-closed`,
            );
        } else {
            registered = true;
        }

        if (!registered) {
            throw new ShardConfigError(
                `Shard "${shardId}" is draining (distributed state) and cannot accept new operations`,
            );
        }

        lifecycle.activeOps += 1;
        lifecycle.operationIds.add(operationId);
    }

    /**
     * Release a shard after an operation completes. Decrements the active operation count
     * and unregisters the operation from distributed state.
     * Awaits the unregister so that distributed drain state is consistent when this returns.
     */
    async releaseShard(shardId: string): Promise<void> {
        const lifecycle = this.lifecycle.get(shardIdKey(shardId));
        if (!lifecycle) {
            return;
        }
        if (lifecycle.activeOps > 0) {
            lifecycle.activeOps -= 1;
        }

        // Unregister one in-flight operation from distributed state.
        // Each releaseShard() pairs with one acquireShard(); since DELETE is
        // idempotent, removing any one operation ID from the set is correct —
        // all IDs eventually get unregistered as operations complete.
        const opId = lifecycle.operationIds.keys().next().value;
        if (opId !== undefined) {
            lifecycle.operationIds.delete(opId);
            await this.unregisterOperation(shardId, opId);
        }
    }

    recordFailure(shardId: string, error: Error): void {
        const state = this.health.get(shardId);
        if (!state) {
            return;
        }
        state.consecutiveFailures += 1;
        state.lastError = error.message;
        state.lastCheckedAt = new Date().toISOString();
        this.logger?.warn?.(
            {
                shardId,
                consecutiveFailures: state.consecutiveFailures,
                err: error,
            },
            "Shard failure recorded",
        );
    }

    recordSuccess(shardId: string, _latencyMs: number): void {
        const state = this.health.get(shardId);
        if (!state) {
            return;
        }
        state.consecutiveFailures = 0;
        state.lastError = undefined;
        state.lastCheckedAt = new Date().toISOString();
    }

    recordError(shardId: string, error: Error, latencyMs: number): void {
        const state = this.health.get(shardId);
        if (!state) {
            return;
        }
        state.consecutiveFailures += 1;
        state.lastError = error.message;
        state.lastCheckedAt = new Date().toISOString();
        this.logger?.warn?.(
            {
                shardId,
                consecutiveFailures: state.consecutiveFailures,
                latencyMs,
                err: error,
            },
            "Shard health check failed",
        );
    }

    /**
     * Add a new shard to the manager. Bumps the routing version and runs hooks.
     * Throws if the shard ID already exists.
     * Serialized via mutex to prevent concurrent modifications.
     */
    async addShard(config: ShardConfig, hooks?: ReshardingHooks): Promise<void> {
        await this.mutex.acquire();
        try {
            return await this.addShardUnlocked(config, hooks);
        } finally {
            this.mutex.release();
        }
    }

    private async addShardUnlocked(config: ShardConfig, hooks?: ReshardingHooks): Promise<void> {
        if (this.closed) {
            throw new ShardConfigError("Cannot add shard: manager is shut down");
        }
        if (this.pools.has(config.id)) {
            throw new ShardConfigError(`Shard "${config.id}" already exists`);
        }
        validateShardConfig(config);

        const oldVersion = this._routingVersion;
        const newVersion = oldVersion + 1;
        const added = [config];
        const removed: string[] = [];

        // Run before-reshard hook (from manager hooks or per-call hooks)
        const beforeHook = hooks?.onBeforeReshard ?? this.hooks?.onBeforeReshard;
        if (beforeHook) {
            await beforeHook(oldVersion, newVersion, added, removed);
        }

        let poolCreated = false;
        try {
            const pool = new Pool(config.connection);
            poolCreated = true;
            this.pools.set(config.id, pool);
            this._shardVersions.set(config.id, config.version ?? newVersion);
            this.health.set(config.id, {
                consecutiveFailures: 0,
            });
            this.lifecycle.set(shardIdKey(config.id), {
                activeOps: 0,
                operationIds: new Set(),
                state: "ACTIVE",
            });
            this._routingVersion = newVersion;

            pool.on("error", (error) => {
                this.logger?.error?.(
                    {
                        shardId: config.id,
                        err: error,
                    },
                    "Shard pool error",
                );
                this.onError?.(error);
                this.recordFailure(config.id, error);
            });

            this.logger?.info?.(
                {
                    shardId: config.id,
                    ...sanitizeConnectionForLogging(config.connection),
                    version: config.version ?? newVersion,
                },
                "Shard added",
            );
        } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));
            // Clean up if pool was created but something else failed
            if (poolCreated) {
                try {
                    const p = this.pools.get(config.id);
                    if (p) {
                        await p.end();
                    }
                } catch {
                    // Ignore cleanup errors
                }
                this.pools.delete(config.id);
            }
            throw new ShardConfigError(
                `Failed to add shard "${config.id}": ${err.message}`,
                { cause: err },
            );
        }

        // Run after-reshard hook
        const afterHook = hooks?.onAfterReshard ?? this.hooks?.onAfterReshard;
        if (afterHook) {
            try {
                await afterHook(oldVersion, newVersion, added, removed);
            } catch (hookError) {
                // If after-reshard hook fails, roll back the shard addition
                this.logger?.error?.(
                    {
                        shardId: config.id,
                        err: hookError,
                    },
                    "After-reshard hook failed, rolling back shard addition",
                );
                try {
                    const p = this.pools.get(config.id);
                    if (p) {
                        await p.end();
                    }
                } catch {
                    // Ignore cleanup errors
                }
                this.pools.delete(config.id);
                this._shardVersions.delete(config.id);
                this.health.delete(config.id);
                this.lifecycle.delete(shardIdKey(config.id));
                this._routingVersion = oldVersion;
                const err = hookError instanceof Error ? hookError : new Error(String(hookError));
                throw new ShardConfigError(
                    `After-reshard hook failed for shard "${config.id}": ${err.message}`,
                    { cause: err },
                );
            }
        }
    }

    /**
     * Remove a shard from the manager. Bumps the routing version and runs hooks.
     * Marks the shard as draining first, waits for active operations to complete
     * (up to the configured timeout), then closes the pool.
     * Throws if the shard ID does not exist.
     * Serialized via mutex to prevent concurrent modifications.
     */
    async removeShard(shardId: string, hooks?: ReshardingHooks): Promise<void> {
        await this.mutex.acquire();
        try {
            return await this.removeShardUnlocked(shardId, hooks);
        } finally {
            this.mutex.release();
        }
    }

    private async removeShardUnlocked(shardId: string, hooks?: ReshardingHooks): Promise<void> {
        if (this.closed) {
            throw new ShardConfigError("Cannot remove shard: manager is shut down");
        }
        if (!this.pools.has(shardId)) {
            throw new ShardConfigError(`Shard "${shardId}" does not exist`);
        }

        const oldVersion = this._routingVersion;
        const newVersion = oldVersion + 1;
        const added: ShardConfig[] = [];
        const removed = [shardId];
        const drainVersion = newVersion;

        // Mark shard as DRAINING locally first so new acquisitions are
        // rejected immediately, even if distributed persistence fails.
        const lifecycle = this.lifecycle.get(shardIdKey(shardId));
        if (lifecycle) {
            lifecycle.state = "DRAINING";
        }

        // Persist draining state in the distributed store so other instances
        // observe it and stop routing new operations to this shard.
        // Best effort — failure to persist must not block removal.
        await this.markShardDraining(shardId, drainVersion).catch(() => {});

        // Run before-reshard hook
        const beforeHook = hooks?.onBeforeReshard ?? this.hooks?.onBeforeReshard;
        if (beforeHook) {
            await beforeHook(oldVersion, newVersion, added, removed);
        }

        // Wait for drain to complete across all instances:
        //   - Local activeOps reaches 0
        //   - Distributed shard_active_ops is empty for this shard
        //   - Shard is not being re-marked draining by another instance
        const maxWaitMs = this.abandonedOperationTimeoutMs;
        const drainResult = await this.waitForDrainComplete(shardId, maxWaitMs);

        if (!drainResult.drained) {
            // Clear the local draining flag so the shard can accept operations again
            const lc = this.lifecycle.get(shardIdKey(shardId));
            if (lc) lc.state = "ACTIVE";
            await this.clearShardDraining(shardId).catch(() => {});
            throw new ShardConfigError(
                `Failed to remove shard "${shardId}": ${drainResult.activeOps} operation(s) still in flight after ${maxWaitMs}ms`,
            );
        }

        // Transition to DRAINED before closing the pool.
        const lc = this.lifecycle.get(shardIdKey(shardId));
        if (lc) lc.state = "DRAINED";

        let poolEnded = false;
        try {
            const pool = this.pools.get(shardId)!;
            await pool.end();
            poolEnded = true;
            this.pools.delete(shardId);
            this._shardVersions.delete(shardId);
            this.health.delete(shardId);
            this.lifecycle.delete(shardIdKey(shardId));
            this._routingVersion = newVersion;

            // Clear the distributed drain state now that the pool is closed.
            await this.clearShardDraining(shardId).catch(() => {});

            this.logger?.info?.(
                { shardId, version: newVersion },
                "Shard removed",
            );
        } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));
            // If pool.end() failed, mark the shard as no longer draining so it can accept operations again
            if (!poolEnded) {
                const lc = this.lifecycle.get(shardIdKey(shardId));
                if (lc) lc.state = "ACTIVE";
                await this.clearShardDraining(shardId).catch(() => {});
            }
            throw new ShardConfigError(
                `Failed to remove shard "${shardId}": ${err.message}`,
                { cause: err },
            );
        }

        // Run after-reshard hook
        const afterHook = hooks?.onAfterReshard ?? this.hooks?.onAfterReshard;
        if (afterHook) {
            await afterHook(oldVersion, newVersion, added, removed);
        }
    }

    async shutdown(): Promise<void> {
        if (this.closed) {
            return;
        }
        this.closed = true;

        // Mark all shards as draining so new acquisitions are rejected.
        for (const shardId of this.pools.keys()) {
            const lifecycle = this.lifecycle.get(shardIdKey(shardId));
            if (lifecycle) {
                lifecycle.state = "DRAINING";
            }
        }

        // Wait for in-flight operations to complete before closing pools,
        // so uncommitted transactions are not killed mid-flight.
        const shutdownTimeoutMs = 30_000;
        const startTime = Date.now();
        while (Date.now() - startTime < shutdownTimeoutMs) {
            let allDrained = true;
            for (const shardId of this.pools.keys()) {
                const ops = this.getActiveOps(shardId);
                if (ops > 0) {
                    allDrained = false;
                    break;
                }
            }
            if (allDrained) {
                break;
            }
            await new Promise((resolve) => setTimeout(resolve, 100));
        }

        const errors: Error[] = [];
        for (const [shardId, pool] of this.pools) {
            try {
                await pool.end();
                this.logger?.info?.(
                    { shardId },
                    "Shard pool shut down",
                );
            } catch (error) {
                errors.push(
                    error instanceof Error
                        ? error
                        : new Error(String(error)),
                );
                this.logger?.error?.(
                    { shardId, err: error },
                    "Failed to shut down shard pool",
                );
            }
        }
        this.pools.clear();

        if (this.ownsDedicatedCoordinationPool && this.dedicatedCoordinationPool) {
            try {
                await this.dedicatedCoordinationPool.end();
            } catch (error) {
                errors.push(
                    error instanceof Error
                        ? error
                        : new Error(String(error)),
                );
                this.logger?.error?.(
                    { err: error },
                    "Failed to shut down coordination pool",
                );
            }
            this.dedicatedCoordinationPool = undefined;
            this.ownsDedicatedCoordinationPool = false;
        }

        if (errors.length > 0) {
            throw new Error(
                `Failed to shut down ${errors.length} shard pool(s)`,
                { cause: errors[0] },
            );
        }
    }

    getStats(): Record<string, { total: number; idle: number; waiting: number }> {
        const stats: Record<string, { total: number; idle: number; waiting: number }> = {};
        for (const [shardId, pool] of this.pools) {
            stats[shardId] = {
                total: pool.totalCount,
                idle: pool.idleCount,
                waiting: pool.waitingCount,
            };
        }
        return stats;
    }

    getHealthStates(): ReadonlyMap<string, ShardHealthState> {
        return this.health;
    }

    /**
     * Return a pool suitable for coordination (distributed drain state).
     * Prefers a dedicated coordination database when configured; otherwise
     * uses the first shard that is not the one being drained/removed.
     */
    private getCoordinationPool(excludeShardId?: string): Pool | undefined {
        if (this.dedicatedCoordinationPool) {
            return this.dedicatedCoordinationPool;
        }
        for (const shardId of this.pools.keys()) {
            if (excludeShardId && shardId === excludeShardId) continue;
            const pool = this.pools.get(shardId);
            if (pool) return pool;
        }
        // Fallback: any available pool (even the excluded one, used before close)
        if (excludeShardId) {
            return this.pools.get(excludeShardId);
        }
        return undefined;
    }

    /**
     * Ensure the distributed drain schema tables exist on the coordination pool.
     * Idempotent. Safe for concurrent calls.
     */
    private async ensureDrainSchema(coordPool: Pool): Promise<void> {
        const lockKey = 0x73686472; // "shdr" — drain schema namespace
        await coordPool.query(`SELECT pg_advisory_lock($1)`, [lockKey]);
        try {
            await coordPool.query(
                `CREATE TABLE IF NOT EXISTS shard_drain_state (
                    shard_id TEXT PRIMARY KEY,
                    draining BOOLEAN NOT NULL DEFAULT FALSE,
                    drain_version INTEGER NOT NULL DEFAULT 0,
                    drain_started_at TIMESTAMPTZ,
                    instance_id TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )`,
            );
            await coordPool.query(
                `CREATE TABLE IF NOT EXISTS shard_active_ops (
                    shard_id TEXT NOT NULL,
                    instance_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (shard_id, operation_id)
                )`,
            );
            await coordPool.query(
                `CREATE INDEX IF NOT EXISTS idx_shard_active_ops_shard ON shard_active_ops(shard_id)`,
            );
            await coordPool.query(
                `CREATE INDEX IF NOT EXISTS idx_shard_active_ops_heartbeat ON shard_active_ops(heartbeat_at)`,
            );
        } finally {
            await coordPool.query(`SELECT pg_advisory_unlock($1)`, [lockKey]).catch(() => {});
        }
    }

    /**
     * Check whether a shard is marked as draining in the distributed state.
     * Returns false if the coordination table doesn't exist yet.
     */
    async isShardDraining(shardId: string): Promise<boolean> {
        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return false;
        try {
            const result = await coordPool.query(
                `SELECT draining FROM shard_drain_state WHERE shard_id = $1`,
                [shardId],
            );
            return result.rows[0]?.draining === true;
        } catch {
            // Table may not exist yet; treat as not draining
            return false;
        }
    }

    /**
     * Mark a shard as draining in the distributed state so other instances
     * stop routing new operations to it. Idempotent.
     *
     * Also sets the local draining flag so this instance rejects new
     * acquisitions immediately, even if the coordination database is
     * unavailable and the distributed write fails.
     */
    async markShardDraining(shardId: string, drainVersion: number): Promise<void> {
        // Set local DRAINING state immediately — this instance rejects new
        // acquisitions regardless of coordination database availability.
        const lifecycle = this.lifecycle.get(shardIdKey(shardId));
        if (lifecycle) {
            lifecycle.state = "DRAINING";
        }

        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return;
        try {
            await this.ensureDrainSchema(coordPool);
        } catch {
            // Coordination database unavailable — local flag is the fallback.
            return;
        }
        try {
            await coordPool.query(
                `INSERT INTO shard_drain_state (shard_id, draining, drain_version, drain_started_at, instance_id, updated_at)
                 VALUES ($1, TRUE, $2, NOW(), $3, NOW())
                 ON CONFLICT (shard_id)
                 DO UPDATE SET draining = TRUE, drain_version = $2,
                               drain_started_at = COALESCE(NULLIF(shard_drain_state.drain_started_at, $4), shard_drain_state.drain_started_at),
                               instance_id = $3, updated_at = NOW()`,
                [shardId, drainVersion, this.instanceId, new Date().toISOString()],
            );
        } catch {
            // Best effort — schema exists but write failed
        }
    }

    /**
     * Clear the draining state for a shard (e.g. after failed removal).
     */
    async clearShardDraining(shardId: string): Promise<void> {
        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return;
        try {
            await coordPool.query(
                `UPDATE shard_drain_state SET draining = FALSE, updated_at = NOW() WHERE shard_id = $1`,
                [shardId],
            );
        } catch {
            // Ignore — best effort cleanup
        }
    }

    /**
     * Register an in-flight operation for a shard in the distributed state.
     * Other instances will see this and wait for it to complete before
     * declaring the shard safe to remove.
     */
    async registerOperation(shardId: string, operationId: string): Promise<void> {
        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return;
        try {
            await this.ensureDrainSchema(coordPool);
            await coordPool.query(
                `INSERT INTO shard_active_ops (shard_id, instance_id, operation_id, started_at, heartbeat_at)
                 VALUES ($1, $2, $3, NOW(), NOW())
                 ON CONFLICT (shard_id, operation_id) DO NOTHING`,
                [shardId, this.instanceId, operationId],
            );
        } catch {
            // Best effort — failure to register should not block the operation
        }
    }

    /**
     * Unregister an in-flight operation after it completes.
     */
    async unregisterOperation(shardId: string, operationId: string): Promise<void> {
        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return;
        try {
            await coordPool.query(
                `DELETE FROM shard_active_ops WHERE shard_id = $1 AND operation_id = $2`,
                [shardId, operationId],
            );
        } catch {
            // Best effort
        }
    }

    /**
     * Refresh the heartbeat for an in-flight operation.
     */
    async heartbeatOperation(shardId: string, operationId: string): Promise<void> {
        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return;
        try {
            await coordPool.query(
                `UPDATE shard_active_ops SET heartbeat_at = NOW() WHERE shard_id = $1 AND operation_id = $2`,
                [shardId, operationId],
            );
        } catch {
            // Best effort
        }
    }

    /**
     * Count active (non-stale) operations for a shard in the distributed state.
     * Rows without a heartbeat in the last `abandonedOperationTimeoutMs` are
     * treated as abandoned (instance crashed) and ignored.
     */
    async countActiveOpsInDb(shardId: string): Promise<number> {
        const coordPool = this.getCoordinationPool(shardId);
        if (!coordPool) return 0;
        try {
            const result = await coordPool.query(
                `SELECT COUNT(*) AS c FROM shard_active_ops
                 WHERE shard_id = $1
                   AND heartbeat_at > NOW() - ($2 || ' milliseconds')::interval`,
                [shardId, String(this.abandonedOperationTimeoutMs)],
            );
            return Number(result.rows[0]?.c ?? 0);
        } catch {
            return 0;
        }
    }

    /**
     * Wait until the shard has no active operations across all instances,
     * up to the given timeout. Returns the number of remaining active ops
     * if the timeout is reached.
     *
     * The distributed shard_active_ops table is the source of truth — each
     * operation is registered there atomically with acquireShard(), so
     * counting local activeOps would double-count. Rows without a heartbeat
     * in the last 5 minutes are treated as abandoned (instance crashed).
     */
    async waitForDrainComplete(
        shardId: string,
        timeoutMs: number,
    ): Promise<{ drained: boolean; activeOps: number }> {
        const startTime = Date.now();
        while (true) {
            const localOps = this.getActiveOps(shardId);
            const dbOps = await this.countActiveOpsInDb(shardId);
            const remaining = Math.max(localOps, dbOps);

            if (remaining === 0) {
                return { drained: true, activeOps: 0 };
            }

            if (Date.now() - startTime > timeoutMs) {
                return { drained: false, activeOps: remaining };
            }

            await new Promise((resolve) => setTimeout(resolve, 100));
        }
    }
}

function shardIdKey(shardId: string): string {
    return `shard:${shardId}`;
}
