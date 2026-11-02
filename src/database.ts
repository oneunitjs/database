import type {
    Pool,
    PoolClient,
    Logger,
    Database,
    CreateDatabaseOptions,
    DatabaseConfig,
    ShardingApi,
    QueryResultRow,
    ShardingConfig,
    LegacyShardingConfig,
    ShardConfig,
    ShardStrategyName,
    ShardRange,
    ShardMapVersion,
    ReshardingHooks,
    QueryOptions,
    QueryContext,
    TransactionOptions,
    ReadPreference,
    ReplicationApi,
    ReplicationEndpointHealth,
    ReplicationHealthResult,
    PreparedStatement,
    InsertManyOptions,
    ShardStrategy,
    ShardRoutingDiagnostics,
} from "./types.js";
import type { PoolConfig } from "pg";
import { ShardExecutor } from "./sharding/shard-executor.js";
import { createShardMetrics } from "./sharding/shard-metrics.js";
import { ShardRebalanceTracker } from "./sharding/shard-rebalance-tracker.js";
import { createPool, clearActiveConnections } from "./client/pool.js";
import { createClient, clearManualClients } from "./client/client.js";
import { createTransaction } from "./client/transaction.js";
import { createStream } from "./client/stream.js";
import { createCursor } from "./client/cursor.js";
import { createCheck } from "./client/check.js";
import { createShutdown } from "./client/shutdown.js";
import { createBatch } from "./client/batch.js";
import { createHooks } from "./client/hooks.js";
import { createRetry } from "./client/retry.js";

import { createSchemaManager } from "./schema/schema.js";
import { createMigrator } from "./schema/migrate.js";

import { createModelFactory } from "./model/model.js";
import { createQueryBuilder } from "./query/builder.js";

import { ShardManager } from "./sharding/shard-manager.js";
import { ShardRouter } from "./sharding/shard-router.js";
import { ShardHealth } from "./sharding/shard-health.js";
import { ShardMigrationTracker } from "./sharding/shard-migration.js";
import { createReshardingManager } from "./resharding/resharding-manager.js";
import { SchemaSyncService } from "./sharding/schema-sync-service.js";
import { SchemaSyncWorker } from "./sharding/schema-sync-worker.js";
import type { ReshardingApi } from "./resharding/resharding-types.js";
import { fencingLockKey } from "./resharding/resharding-schema.js";
import {
    DistributedTransactionCoordinator,
    DistributedTransactionContext,
    MaxPreparedTransactionsError,
} from "./distributed-tx/index.js";
import {
    ShardingDisabledError,
    ShardNotFoundError,
    ShardRoutingError,
    ShardConfigError,
    CrossShardTransactionError,
} from "./sharding/errors.js";
import { ReshardingError, WriteFencedError } from "./resharding/resharding-errors.js";

import { ReplicationManager } from "./replication/replication-manager.js";
import { ReplicationConfigError } from "./replication/errors.js";
import { validateReplicationConfig } from "./replication/replication-manager.js";

/**
 * Normalize a legacy flat sharding config into the discriminated union format.
 * Preserves backward compatibility with existing `routing.directory` format.
 */
function normalizeShardingConfig(
    config: LegacyShardingConfig | undefined,
): ShardingConfig | undefined {
    if (!config) {
        return undefined;
    }

    const knownStrategies: ShardStrategyName[] = [
        "hash",
        "consistent-hash",
        "rendezvous",
        "range",
        "directory",
        "tenant",
        "composite",
        "geographic",
        "weighted",
    ];

    const strategy = config.strategy as ShardStrategyName | undefined;
    if (strategy && knownStrategies.includes(strategy)) {
        const result = { ...config } as ShardingConfig;
        // P2-2: a default shard is required when sharding is enabled so un-keyed
        // writes cannot silently land on an arbitrary shard.
        if (config.enabled && !result.defaultShardId) {
            throw new ShardConfigError(
                `Sharding is enabled but no defaultShardId is configured. ` +
                `Configure defaultShardId explicitly (one of: ${config.shards?.map((s) => s.id).join(", ")}).`,
            );
        }
        if (strategy === "directory") {
            const legacyDir = (config as LegacyShardingConfig).routing?.directory;
            if (legacyDir) {
                (result as { directory?: Record<string, string> }).directory = legacyDir;
            }
        }
        return result;
    }

    if (config.enabled && config.shards && config.shards.length > 0) {
        const defaultShardId = config.defaultShardId;
        if (!defaultShardId) {
            throw new ShardConfigError(
                `Sharding is enabled but no defaultShardId is configured. ` +
                `Configure defaultShardId explicitly (one of: ${config.shards.map((s) => s.id).join(", ")}) ` +
                "to avoid implicit routing ambiguity.",
            );
        }
        return {
            enabled: true,
            strategy: "consistent-hash",
            shards: config.shards,
            defaultShardId: defaultShardId ?? config.shards[0].id,
            coordination: (config as LegacyShardingConfig).coordination as PoolConfig | undefined,
            coordinationMode: (config as LegacyShardingConfig).coordinationMode as
                | "strict"
                | "local-only"
                | "fail-closed"
                | undefined,
            abandonedOperationTimeoutMs: (config as LegacyShardingConfig)
                .abandonedOperationTimeoutMs as number | undefined,
        } as ShardingConfig;
    }

    return {
        enabled: false,
        strategy: "consistent-hash",
        shards: [],
    } as ShardingConfig;
}

/**
 * Validate sharding configuration before pools are created.
 * Detects duplicate shard IDs, missing connection details, invalid routing
 * strategy, unknown default shard, invalid weights, invalid range definitions,
 * missing coordination configuration, invalid timeout values, and incompatible
 * feature combinations.
 */
function validateShardingConfig(
    config: LegacyShardingConfig | ShardingConfig,
    logger?: Logger,
): void {
    const enabled = config.enabled === true;
    if (!enabled) {
        return;
    }

    const shards = config.shards;
    if (!shards || !Array.isArray(shards) || shards.length === 0) {
        throw new ShardConfigError(
            "Sharding is enabled but no shards are configured",
        );
    }

    // Detect duplicate shard IDs.
    const seen = new Set<string>();
    for (const shard of shards) {
        if (seen.has(shard.id)) {
            throw new ShardConfigError(
                `Duplicate shard id: ${shard.id}`,
            );
        }
        seen.add(shard.id);

        // Validate connection details.
        if (!shard.connection) {
            throw new ShardConfigError(
                `Shard "${shard.id}" is missing connection configuration`,
            );
        }
        if (
            !shard.connection.connectionString &&
            !shard.connection.host
        ) {
            throw new ShardConfigError(
                `Shard "${shard.id}" connection must specify connectionString or host`,
            );
        }

        // Validate weight if present (WeightedShardConfig extends ShardConfig with weight).
        const weightedShard = shard as { weight?: number };
        if (weightedShard.weight !== undefined) {
            if (
                typeof weightedShard.weight !== "number" ||
                !Number.isFinite(weightedShard.weight) ||
                weightedShard.weight <= 0
            ) {
                throw new ShardConfigError(
                    `Shard "${shard.id}" weight must be a positive number`,
                );
            }
        }
    }

    // Validate default shard ID refers to an existing shard.
    const defaultShardId = config.defaultShardId;
    if (defaultShardId && !seen.has(defaultShardId)) {
        throw new ShardConfigError(
            `Default shard "${defaultShardId}" is not in the configured shards list`,
        );
    }

    // Validate timeout values.
    const timeoutMs = (config as Record<string, unknown>).timeout;
    if (timeoutMs !== undefined) {
        const t = Number(timeoutMs);
        if (!Number.isFinite(t) || t < 0) {
            throw new ShardConfigError(
                `Sharding timeout must be a non-negative number`,
            );
        }
    }

    logger?.debug?.(
        { shardCount: shards.length, defaultShardId },
        "Sharding configuration validated",
    );
}

interface HashShardingConfig {
    enabled: boolean;
    strategy: "hash";
    shards: ShardConfig[];
    defaultShardId?: string;
    coordination?: PoolConfig;
    coordinationMode?: "strict" | "local-only" | "fail-closed";
    abandonedOperationTimeoutMs?: number;
}

/**
 * Resolve the target pool for a query/transaction. Returns the primary pool
 * when sharding is disabled or no shardKey is provided; otherwise routes
 * to the shard matching the key.
 * Throws ShardRoutingError if the target shard is draining (distributed state).
 */
function buildShardingApi(
    manager: ShardManager | undefined,
    router: ShardRouter | undefined,
    health: ShardHealth | undefined,
    logger?: Logger,
    defaultHooks?: ReshardingHooks,
    shardReplicationManagers?: Map<string, ReplicationManager>,
    reshardingApi?: ReshardingApi,
    schemaSyncService?: import("./sharding/schema-sync-service.js").SchemaSyncService,
    distributedTxCoordinator?: DistributedTransactionCoordinator,
    shardExecutor?: ShardExecutor,
    shardMetrics?: ReturnType<typeof createShardMetrics>,
    batch?: ReturnType<typeof createBatch>,
    rebalanceTracker?: ShardRebalanceTracker,
): ShardingApi {
    const enabled = !!manager && !!router;
    const migrate = manager ? new ShardMigrationTracker(manager) : undefined;

    /**
     * Deprecated compatibility wrapper: resolves the shard and executes
     * directly against the shard's pool, preserving the original call shape
     * (text, values passed separately, no QueryOptions normalization).
     */
    async function query<T extends QueryResultRow = QueryResultRow>(
        shardKey: string,
        text: string | object,
        values?: unknown[] | QueryOptions,
        _options?: QueryOptions,
    ): Promise<import("pg").QueryResult<T>> {
        if (!enabled || !router) {
            throw new ShardingDisabledError();
        }
        const { id: shardId, pool: shardPool } = router.resolveShard(shardKey);

        // Atomic write fencing: wrap in a transaction with a shared advisory
        // lock acquired before the gate check, eliminating TOCTOU races.
        const client = await shardPool.connect();
        try {
            await client.query("BEGIN");

            if (reshardingApi) {
                const [lockKey1, lockKey2] = fencingLockKey(shardId);
                await client.query(`SELECT pg_advisory_xact_lock_shared($1, $2)`, [lockKey1, lockKey2]);

                const fenced = await reshardingApi.isShardWriteFenced(shardId);
                if (fenced) {
                    await client.query("ROLLBACK");
                    throw new WriteFencedError(shardKey, shardId);
                }
            }

            const result = await client.query(
                text as string,
                values as unknown[],
            );
            await client.query("COMMIT");
            return result as unknown as import("pg").QueryResult<T>;
        } catch (error) {
            try {
                await client.query("ROLLBACK");
            } catch {
                // Ignore rollback errors.
            }
            throw error;
        } finally {
            client.release();
        }
    }

    async function transaction<T>(
        shardKey: string,
        callback: (client: PoolClient) => Promise<T> | T,
        txOptions?: TransactionOptions,
    ): Promise<T> {
        // Deprecated sharding API: transaction is pinned to a single shard.
        // Cross-shard transactions are not supported.
        if (!enabled || !router) {
            throw new ShardingDisabledError();
        }
        const { id: shardId, pool: shardPool } = router.resolveShard(shardKey);

        // Atomic write fencing: manage the connection directly so the shared
        // advisory lock is held for the entire transaction.
        const client = await shardPool.connect();
        try {
            await client.query("BEGIN");

            if (reshardingApi) {
                const [lockKey1, lockKey2] = fencingLockKey(shardId);
                await client.query(`SELECT pg_advisory_xact_lock_shared($1, $2)`, [lockKey1, lockKey2]);

                const fenced = await reshardingApi.isShardWriteFenced(shardId);
                if (fenced) {
                    await client.query("ROLLBACK");
                    throw new WriteFencedError(shardKey, shardId);
                }
            }

            const tx = createTransaction({
                pool: shardPool,
                logger,
            });
            // We already have a BEGIN in progress; use the client directly
            // rather than starting a nested transaction.
            try {
                const result = await callback(client);
                await client.query("COMMIT");
                return result as T;
            } catch (error) {
                await client.query("ROLLBACK").catch(() => {});
                throw error;
            }
        } finally {
            client.release();
        }
    }

    async function shutdown(): Promise<void> {
        if (!manager) {
            return;
        }
        await manager.shutdown();
        for (const mgr of shardReplicationManagers?.values() ?? []) {
            try {
                await mgr.shutdown();
            } catch {
                // Continue shutting down remaining shard replicas.
            }
        }
    }

    async function shardHealthApi(shardId: string): Promise<import("./types.js").ShardHealthCheckResult | undefined> {
        if (!health) {
            return undefined;
        }
        return health.shardHealth(shardId);
    }

    async function healthApi(_options?: { timeout?: number }): Promise<import("./types.js").ShardHealthResult> {
        if (!health) {
            return {
                healthy: true,
                checkedAt: new Date().toISOString(),
                shards: [],
                downShards: [],
                upShards: [],
            };
        }
        return health.health();
    }

    async function addShardApi(config: ShardConfig, hooks?: ReshardingHooks): Promise<void> {
        if (!manager) {
            throw new ShardingDisabledError();
        }
        if (config.replication) {
            validateReplicationConfig({
                enabled: true,
                mode: config.replication.mode,
                primary: { id: "primary", connection: config.connection },
                replicas: config.replication.replicas,
                lagThresholdMs: config.replication.lagThresholdMs,
                healthCheck: config.replication.healthCheck,
            });
        }
        await manager.addShard(config, hooks ?? defaultHooks);
        migrate?.registerShard(config.id);

        if (
            config.replication &&
            config.replication.enabled &&
            config.replication.replicas.length > 0
        ) {
            const shardPool = manager.getShard(config.id)?.pool;
            if (!shardPool) {
                throw new ReplicationConfigError(
                    `Primary pool for shard "${config.id}" was not created`,
                );
            }
            const replicationManager = new ReplicationManager({
                primary: {
                    endpointId: "primary",
                    pool: shardPool,
                    connection: config.connection,
                    owned: false,
                },
                replicas: config.replication.replicas.map((r) => ({
                    id: r.id,
                    connection: r.connection,
                })),
                mode: config.replication.mode,
                lagThresholdMs: config.replication.lagThresholdMs,
                healthCheck: config.replication.healthCheck,
                logger,
            });
            replicationManager.initialize();
            shardReplicationManagers!.set(config.id, replicationManager);
        }
    }

    async function removeShardApi(shardId: string, hooks?: ReshardingHooks): Promise<void> {
        if (!manager) {
            throw new ShardingDisabledError();
        }
        const replicaMgr = shardReplicationManagers!.get(shardId);
        if (replicaMgr) {
            await replicaMgr.shutdown();
            shardReplicationManagers!.delete(shardId);
        }
        await manager.removeShard(shardId, hooks ?? defaultHooks);
        health?.unregisterShard(shardId);
        migrate?.unregisterShard(shardId);
    }

    /**
     * Execute a callback with a dedicated connection on a specific shard.
     * Intended for DDL and administrative operations that must target a
     * particular shard. Acquires a shard lease for distributed drain tracking.
     */
    async function executeOnShardApi(
        shardId: string,
        callback: (client: PoolClient) => Promise<void> | void,
    ): Promise<void> {
        if (!manager) {
            throw new ShardingDisabledError();
        }
        const shard = manager.getShard(shardId);
        if (!shard || !shard.pool) {
            throw new ShardNotFoundError(`Shard "${shardId}" not found or pool unavailable`);
        }

        if (!shardExecutor) {
            throw new Error("Shard executor is not available");
        }
        await shardExecutor.acquireShard(shardId);
        const client = await shard.pool.connect();
        try {
            await callback(client);
        } finally {
            client.release();
            await shardExecutor.releaseShard(shardId);
        }
    }

    /**
     * Multi-shard batch insert: each group executes independently on its
     * target shard. No cross-shard atomicity is claimed.
     */
    async function batchInsertManyApi(groups: Array<{
        table: string;
        rows: Record<string, unknown>[];
        shardKey?: string;
        shardId?: string;
        chunkSize?: number;
        onConflict?: InsertManyOptions["onConflict"];
        returning?: string | string[];
    }>): Promise<{
        successful: Array<{ shardId: string; shardKey?: string; rowsAffected: number }>;
        failed: Array<{ shardId: string; shardKey?: string; error: string }>;
    }> {
        if (!batch) {
            throw new Error("Batch API is not available");
        }
        const successful: Array<{ shardId: string; shardKey?: string; rowsAffected: number }> = [];
        const failed: Array<{ shardId: string; shardKey?: string; error: string }> = [];

        for (const group of groups) {
            let targetShardId: string;
            let targetShardKey: string | undefined;

            if (group.shardId) {
                const shard = manager?.getShard(group.shardId);
                if (!shard || !shard.pool) {
                    failed.push({
                        shardId: group.shardId,
                        shardKey: group.shardKey,
                        error: `Shard "${group.shardId}" not found or pool unavailable`,
                    });
                    continue;
                }
                targetShardId = group.shardId;
            } else if (group.shardKey) {
                try {
                    const { id } = router!.resolveShard(group.shardKey);
                    targetShardId = id;
                    targetShardKey = group.shardKey;
                } catch (error) {
                    failed.push({
                        shardId: "unknown",
                        shardKey: group.shardKey,
                        error: error instanceof Error ? error.message : String(error),
                    });
                    continue;
                }
            } else {
                failed.push({
                    shardId: "unknown",
                    shardKey: undefined,
                    error: "batchInsertMany requires shardKey or shardId per group",
                });
                continue;
            }

            try {
                const result = await batch.insertMany(
                    group.table,
                    group.rows,
                    {
                        shardKey: group.shardKey,
                        shardId: group.shardId,
                        chunkSize: group.chunkSize,
                        onConflict: group.onConflict,
                        returning: group.returning,
                    },
                );
                const rowsAffected = typeof result === "number" ? result : (result as { length?: number })?.length ?? 0;
                successful.push({
                    shardId: targetShardId,
                    shardKey: targetShardKey,
                    rowsAffected,
                });
            } catch (error) {
                failed.push({
                    shardId: targetShardId,
                    shardKey: targetShardKey,
                    error: error instanceof Error ? error.message : String(error),
                });
            }
        }

        return { successful, failed };
    }

    /**
     * Execute a query on multiple shards independently and merge results.
     * Does not claim global snapshot consistency across independent shard
     * transactions. Partial results are returned when explicitly requested.
     */
    async function fanOutApi<T extends QueryResultRow = QueryResultRow>({
        query,
        values,
        shardIds,
        concurrency = 8,
        merge = "none",
        onShardError = "continue",
    }: {
        query: string;
        values?: unknown[];
        shardIds?: string[];
        concurrency?: number;
        merge?: "none" | "count" | "sum" | "min" | "max" | "application";
        onShardError?: "continue" | "stop";
    }): Promise<{
        results: T[];
        successfulShards: string[];
        failedShards: Array<{ shardId: string; error: string }>;
        partial: boolean;
    }> {
        const mgr = manager;
        const exec = shardExecutor;
        if (!mgr || !exec) {
            throw new ShardingDisabledError();
        }
        const targetShardIds = shardIds ?? mgr.shardIds;
        const results: T[] = [];
        const successfulShards: string[] = [];
        const failedShards: Array<{ shardId: string; error: string }> = [];

        // Semaphore for concurrency limiting.
        const semaphore = new Array(Math.min(concurrency, targetShardIds.length)).fill(null);

        async function runOnShard(shardId: string): Promise<void> {
            const shard = mgr!.getShard(shardId);
            if (!shard || !shard.pool) {
                failedShards.push({ shardId, error: `Shard "${shardId}" not found or pool unavailable` });
                
                return;
            }

            try {
                await exec!.acquireShard(shardId);
                try {
                    const client = await shard.pool.connect();
                    try {
                        const result = await client.query(query, values ?? []);
                        results.push(...((result.rows ?? []) as T[]));
                        successfulShards.push(shardId);
                    } finally {
                        client.release();
                    }
                } finally {
                    await exec!.releaseShard(shardId);
                }
            } catch (error) {
                
                if (onShardError === "stop") {
                    throw error;
                }
                failedShards.push({
                    shardId,
                    error: error instanceof Error ? error.message : String(error),
                });
            }
        }

        // Execute with concurrency limiting
        const queue = [...targetShardIds];
        const workers = semaphore.map(() => {
            return (async () => {
                while (queue.length > 0) {
                    const shardId = queue.shift();
                    if (shardId !== undefined) {
                        await runOnShard(shardId);
                    }
                }
            })();
        });
        await Promise.all(workers);

        // Apply merge strategy. "application" leaves results unmerged so the
        // caller performs application-side aggregation.
        let merged: T[] = results;
        if (merge !== "none" && merge !== "application" && results.length > 0) {
            const first = results[0] as Record<string, unknown>;
            const numericKeys = Object.keys(first).filter(
                (k) => typeof first[k] === "number",
            );

            if (merge === "count") {
                merged = [{ count: results.length }] as unknown as T[];
            } else if (numericKeys.length > 0) {
                const mergedRow: Record<string, unknown> = {};
                for (const key of numericKeys) {
                    const values = results.map((r) => (r as Record<string, unknown>)[key] as number);
                    switch (merge) {
                        case "sum":
                            mergedRow[key] = values.reduce((a, b) => a + b, 0);
                            break;
                        case "min":
                            mergedRow[key] = Math.min(...values);
                            break;
                        case "max":
                            mergedRow[key] = Math.max(...values);
                            break;
                    }
                }
                merged = [mergedRow as T];
            }
        }

        return {
            results: merged,
            successfulShards,
            failedShards,
            partial: failedShards.length > 0,
        };
    }

    /**
     * Execute a query on all shards and merge results using the specified
     * strategy. Convenience wrapper around `fanOut`.
     */
    async function queryAllShardsApi<T extends QueryResultRow = QueryResultRow>({
        query,
        values,
        concurrency,
        merge = "application",
    }: {
        query: string;
        values?: unknown[];
        concurrency?: number;
        merge?: "count" | "sum" | "min" | "max" | "application";
    }): Promise<{
        results: T[];
        successfulShards: string[];
        failedShards: Array<{ shardId: string; error: string }>;
        partial: boolean;
    }> {
        const mgr = manager;
        return fanOutApi<T>({
            query,
            values,
            shardIds: mgr?.shardIds,
            concurrency,
            merge: merge as "none" | "count" | "sum" | "min" | "max" | "application",
            onShardError: "continue",
        });
    }

    // Lazily initialize the coordinator on first distributedTransaction call.
    let distributedTxInitialized = false;
    const ensureDistributedTx = async (): Promise<DistributedTransactionCoordinator> => {
        if (!distributedTxCoordinator) {
            throw new MaxPreparedTransactionsError();
        }
        if (!distributedTxInitialized) {
            await distributedTxCoordinator.initialize();
            distributedTxInitialized = true;
        }
        return distributedTxCoordinator;
    };

    async function distributedTransactionFn<T>(
        callback: (tx: DistributedTransactionContext) => Promise<T> | T,
        options?: { isolation?: string; timeout?: number; retry?: unknown },
    ): Promise<{
        transactionId: string;
        status: "committed" | "aborted";
        participants: Record<string, "prepared" | "committed" | "aborted">;
    }> {
        if (!manager || !router) {
            throw new ShardingDisabledError();
        }

        const coordinator = await ensureDistributedTx();

        const shardPools = new Map<string, Pool>();
        for (const shardId of manager.shardIds) {
            const shard = manager.getShard(shardId);
            if (shard?.pool) {
                shardPools.set(shardId, shard.pool);
                coordinator.registerParticipantPool(shardId, shard.pool);
            }
        }

        const transactionId = await coordinator.begin(options as any);
        const ctx = new DistributedTransactionContext(
            transactionId,
            coordinator,
            shardPools,
            (shardKey: string) => {
                const { id } = router.resolveShard(shardKey);
                return id;
            },
        );

        try {
            const result = await callback(ctx);

            // Phase 1: Prepare all participants
            await coordinator.prepare(transactionId);

            // Phase 2: Commit all participants
            await coordinator.commit(transactionId);

            // Build participant status report
            const participants: Record<string, "prepared" | "committed" | "aborted"> = {};
            const durable = await coordinator.getStatus(transactionId);
            if (durable) {
                for (const shardId of durable.participants) {
                    participants[shardId] = "committed";
                }
            }

            return {
                transactionId,
                status: "committed",
                participants,
            };
        } catch (error) {
            // Abort on any failure
            try {
                await coordinator.abort(transactionId);
            } catch (abortError) {
                logger?.error?.(
                    { transactionId, err: abortError },
                    "Distributed transaction abort during error handling failed",
                );
            }

            const participants: Record<string, "prepared" | "committed" | "aborted"> = {};
            const durable = await coordinator.getStatus(transactionId);
            if (durable) {
                for (const shardId of durable.participants) {
                    participants[shardId] = "aborted";
                }
            }

            throw error;
        }
    }

    // Topology management layer — read-only inspection + opt-in rebalancing
    // proposal. Does not move data or change routing on its own.
    const topologyApi = {
        snapshot(): {
            generation: number;
            shards: Array<{
                shardId: string;
                state: "ACTIVE" | "DRAINING" | "DRAINED" | "REMOVED";
                weight: number;
                activeOps: number;
                connected: boolean;
                lastCheckedAt?: string;
                consecutiveFailures: number;
            }>;
        } {
            if (!manager) {
                return { generation: 0, shards: [] };
            }
            const shards = manager.shardIds.map((id) => {
                const shard = manager!.getShard(id);
                const cfg = manager!.getShardConfig(id) as { weight?: number } | undefined;
                const weight =
                    typeof cfg?.weight === "number" && Number.isFinite(cfg.weight) && cfg.weight > 0
                        ? cfg.weight
                        : 1;
                return {
                    shardId: id,
                    state: manager!.getLifecycleState(id),
                    weight,
                    activeOps: manager!.getActiveOps(id),
                    connected: shard?.connected ?? false,
                    lastCheckedAt: shard?.lastCheckedAt,
                    consecutiveFailures: shard?.consecutiveFailures ?? 0,
                };
            });
            return { generation: manager.routingVersion, shards };
        },
        validate(): { valid: boolean; issues: string[] } {
            if (!manager || !router) {
                return { valid: false, issues: ["sharding is disabled"] };
            }
            const issues: string[] = [];
            const shards = topologyApi.snapshot().shards;
            const ids = new Set<string>();
            for (const shard of shards) {
                if (ids.has(shard.shardId)) {
                    issues.push(`duplicate shard id: ${shard.shardId}`);
                }
                ids.add(shard.shardId);
                if (shard.state === "REMOVED") {
                    issues.push(`shard ${shard.shardId} is in REMOVED state`);
                }
                if (!shard.connected && shard.consecutiveFailures > 0) {
                    issues.push(
                        `shard ${shard.shardId} is unhealthy (${shard.consecutiveFailures} consecutive failures)`,
                    );
                }
            }
            if (manager.routingVersion <= 0 && shards.length > 0) {
                issues.push("routing version is not initialized");
            }
            return { valid: issues.length === 0, issues };
        },
        planRebalance(options?: {
            strategy?: "least-loaded" | "even";
            maxConcurrentMoves?: number;
        }): {
            generation: number;
            proposal: Array<{
                fromShardId: string;
                toShardId: string;
                reason: string;
                estimatedRows?: number;
            }>;
        } {
            if (!manager) {
                return { generation: 0, proposal: [] };
            }
            const snap = topologyApi.snapshot();
            const proposal: Array<{
                fromShardId: string;
                toShardId: string;
                reason: string;
                estimatedRows?: number;
            }> = [];
            if (snap.shards.length < 2) {
                return { generation: snap.generation, proposal };
            }
            const maxMoves = Math.max(1, options?.maxConcurrentMoves ?? 1);
            // A proposal only — no data is moved. Compare in-flight pressure
            // (activeOps) as a lightweight load signal; row-count data is not
            // available without a per-shard probe and is intentionally left
            // undefined rather than guessed.
            const ordered = [...snap.shards].sort((a, b) => b.activeOps - a.activeOps);
            let moves = 0;
            for (let i = 0; i < ordered.length && moves < maxMoves; i++) {
                const from = ordered[i];
                const to = ordered[ordered.length - 1 - i];
                if (from.shardId === to.shardId) {
                    continue;
                }
                if (from.activeOps <= to.activeOps) {
                    continue;
                }
                proposal.push({
                    fromShardId: from.shardId,
                    toShardId: to.shardId,
                    reason:
                        options?.strategy === "least-loaded"
                            ? "higher active operation count than target shard"
                            : "rebalance toward even load distribution",
                });
                moves += 1;
            }
            return { generation: snap.generation, proposal };
        },
    };

    return {
        get enabled(): boolean {
            return enabled;
        },
        get strategy(): ShardStrategyName | undefined {
            return router?.strategyName;
        },
        get routingVersion(): ShardMapVersion {
            return manager?.routingVersion ?? 0;
        },
        get shards(): ReadonlyArray<{ id: string; connected: boolean; pool?: Pool; consecutiveFailures: number }> {
            if (!manager) {
                return [];
            }
            return manager.shardIds.map((id) => {
                const shard = manager.getShard(id);
                return {
                    id,
                    connected: shard?.connected ?? false,
                    pool: shard?.pool,
                    consecutiveFailures: shard?.consecutiveFailures ?? 0,
                };
            });
        },
        getShard(shardId: string) {
            if (!manager) {
                return undefined;
            }
            return manager.getShard(shardId);
        },
        getShardForKey(shardKey: string) {
            if (!enabled || !router) {
                throw new ShardingDisabledError();
            }
            const { id, pool } = router.resolveShard(shardKey);
            const shard = manager?.getShard(id);
            return {
                id,
                connected: true,
                pool,
                consecutiveFailures: shard?.consecutiveFailures ?? 0,
            };
        },
        getRoutingDiagnostics() {
            if (!router) {
                return {
                    recent: [],
                    totalRoutes: 0,
                    byStrategy: {},
                    byShard: {},
                    clear: () => {},
                } as unknown as ShardRoutingDiagnostics;
            }
            return router.getDiagnostics();
        },
        query,
        transaction,
        distributedTransaction: distributedTransactionFn,
        shutdown,
        health: healthApi,
        shardHealth: shardHealthApi,
        addShard: addShardApi,
        removeShard: removeShardApi,
        batchInsertMany: batchInsertManyApi,
        fanOut: fanOutApi,
        queryAllShards: queryAllShardsApi,
        topology: topologyApi,
        metrics: shardMetrics,
        rebalanceTracker: rebalanceTracker!,
        executeOnShard: executeOnShardApi,
        migrate: migrate ?? undefined,
        schemaSync: schemaSyncService,
        get distributedTx() {
            return distributedTxCoordinator;
        },
        get isClosed(): boolean {
            return manager?.isClosed ?? false;
        },
    };
}

export function createDatabase({
    logger,
    instrumentation,
    hooks,
    retry,
    queryTimeout,
    timeout,
    connectTimeout,
    sharding,
    ...config
}: CreateDatabaseOptions = {}): Database {
    const hookSystem = hooks ?? createHooks({ logger, ...(instrumentation ?? {}) });
    const retryDefaults = typeof retry === "boolean" ? {} : (typeof retry === "number" ? { retries: retry } : retry);

    // Per-shard metrics collector — tracks query counts, errors, latency, and
    // routing decisions per shard. Bounded-cardinality labels (shardId only).
    const shardMetrics = createShardMetrics();

    // Rebalance progress tracking primitives — tracks in-flight shard
    // rebalance operations (copying, validating, cutting over).
    const rebalanceTracker = new ShardRebalanceTracker();

    // Validate sharding configuration early, before any pools are created.
    if (sharding) {
        validateShardingConfig(sharding as LegacyShardingConfig | ShardingConfig, logger);
    }

    let manager: ShardManager | undefined;
    let router: ShardRouter | undefined;
    let health: ShardHealth | undefined;
    let pool: Pool | undefined;
    let topLevelReplicationManager: ReplicationManager | undefined;
    const shardReplicationManagers = new Map<string, ReplicationManager>();

    const normalizedSharding = normalizeShardingConfig(sharding as LegacyShardingConfig | undefined);

    if (normalizedSharding?.enabled) {
        const shardingConfig = normalizedSharding as ShardingConfig;

        if (!shardingConfig.shards || shardingConfig.shards.length === 0) {
            throw new Error(
                "Sharding is enabled but no shards are configured",
            );
        }

        // Validate per-shard replication configs before creating any pools so
        // a configuration error never leaves partially-created pools behind.
        for (const shard of shardingConfig.shards) {
            if (shard.replication) {
                validateReplicationConfig({
                    enabled: true,
                    mode: shard.replication.mode,
                    primary: {
                        id: "primary",
                        connection: shard.connection,
                    },
                    replicas: shard.replication.replicas,
                    lagThresholdMs: shard.replication.lagThresholdMs,
                    healthCheck: shard.replication.healthCheck,
                });
            }
        }

        manager = new ShardManager(shardingConfig.shards, logger, undefined, shardingConfig.hooks, {
            coordination: shardingConfig.coordination,
            coordinationMode: shardingConfig.coordinationMode,
            abandonedOperationTimeoutMs: shardingConfig.abandonedOperationTimeoutMs,
        });

        const routerOptions: {
            manager: ShardManager;
            enabled: boolean;
            strategy?: ShardStrategyName;
            strategyInstance?: ShardStrategy;
            defaultShardId?: string;
            directory?: Record<string, string>;
            virtualNodes?: number;
            hashSeed?: string;
            ranges?: ShardRange[];
            tenants?: Record<string, string>;
            weights?: Record<string, number>;
            regions?: Record<string, string[]>;
            region?: string;
            defaultRegion?: string;
            compositeKey?: Record<string, string | number | boolean | null>;
            unknownKeyPolicy?: "throw" | "default";
            unknownRegionPolicy?: "throw" | "default";
        } = {
            manager,
            enabled: true,
            strategy: shardingConfig.strategy,
            strategyInstance: shardingConfig.strategyInstance,
            defaultShardId: shardingConfig.defaultShardId,
        };

        // The shard's primary pool (owned by ShardManager) is reused; only
        // replica pools are created and owned here.
        for (const shard of shardingConfig.shards) {
            if (
                shard.replication &&
                shard.replication.enabled &&
                shard.replication.replicas.length > 0
            ) {
                const shardPool = manager.getShard(shard.id)?.pool;
                if (!shardPool) {
                    manager.shutdown();
                    throw new ReplicationConfigError(
                        `Primary pool for shard "${shard.id}" was not created`,
                    );
                }

                const replicationManager = new ReplicationManager({
                    primary: {
                        endpointId: "primary",
                        pool: shardPool,
                        connection: shard.connection,
                        owned: false,
                    },
                    replicas: shard.replication.replicas.map((r) => ({
                        id: r.id,
                        connection: r.connection,
                    })),
                    mode: shard.replication.mode,
                    lagThresholdMs: shard.replication.lagThresholdMs,
                    healthCheck: shard.replication.healthCheck,
                    logger,
                });

                try {
                    replicationManager.initialize();
                } catch (error) {
                    manager.shutdown();
                    throw error;
                }
                shardReplicationManagers.set(shard.id, replicationManager);
            }
        }

        switch (shardingConfig.strategy) {
            case "consistent-hash": {
                if (shardingConfig.strategyInstance) break;
                const cc = shardingConfig as Extract<ShardingConfig, { strategy: "consistent-hash" }>;
                routerOptions.virtualNodes = cc.virtualNodes;
                break;
            }
            case "rendezvous": {
                if (shardingConfig.strategyInstance) break;
                const rc = shardingConfig as Extract<ShardingConfig, { strategy: "rendezvous" }>;
                routerOptions.hashSeed = rc.hashSeed;
                break;
            }
            case "range": {
                if (shardingConfig.strategyInstance) break;
                const rc = shardingConfig as Extract<ShardingConfig, { strategy: "range" }>;
                routerOptions.ranges = rc.ranges;
                break;
            }
            case "directory": {
                if (shardingConfig.strategyInstance) break;
                const dc = shardingConfig as Extract<ShardingConfig, { strategy: "directory" }>;
                routerOptions.directory = dc.directory;
                routerOptions.unknownKeyPolicy = dc.unknownKeyPolicy;
                break;
            }
            case "tenant": {
                if (shardingConfig.strategyInstance) break;
                const tc = shardingConfig as Extract<ShardingConfig, { strategy: "tenant" }>;
                routerOptions.tenants = tc.tenants;
                routerOptions.defaultShardId = tc.defaultTenantShardId ?? shardingConfig.defaultShardId;
                break;
            }
            case "composite":
                break;
            case "geographic": {
                if (shardingConfig.strategyInstance) break;
                const gc = shardingConfig as Extract<ShardingConfig, { strategy: "geographic" }>;
                routerOptions.regions = gc.regions;
                routerOptions.defaultRegion = gc.defaultRegion;
                routerOptions.unknownRegionPolicy = gc.unknownRegionPolicy;
                break;
            }
            case "weighted": {
                if (shardingConfig.strategyInstance) break;
                const wc = shardingConfig as Extract<ShardingConfig, { strategy: "weighted" }>;
                routerOptions.weights = Object.fromEntries(
                    wc.shards.map((s) => [s.id, s.weight]),
                );
                break;
            }
        }

        router = new ShardRouter(routerOptions);
        (router as { _opts?: typeof routerOptions })._opts = routerOptions;
        router.validateStrategyConfig(shardingConfig);

        health = new ShardHealth(manager, logger);
    } else {
        pool = createPool({
            logger,
            config,
            onError: hookSystem.config?.onError
                ? (error: unknown) => hookSystem.config?.onError?.({ error, sql: undefined, durationMs: 0 })
                : undefined,
        });

        if (config.replication?.enabled) {
            const replicationConfig = config.replication;
            const primaryEndpointId = replicationConfig.primary?.id ?? "primary";
            const primaryConnection =
                replicationConfig.primary?.connection ?? (config as PoolConfig);

            validateReplicationConfig({
                enabled: true,
                mode: replicationConfig.mode,
                primary: {
                    id: primaryEndpointId,
                    connection: primaryConnection,
                },
                replicas: replicationConfig.replicas,
                lagThresholdMs: replicationConfig.lagThresholdMs,
                healthCheck: replicationConfig.healthCheck,
            });

            // When the replication primary differs from the top-level config,
            // recreate the primary pool from the replication primary connection
            // so `db.pool` always refers to the replication primary.
            if (replicationConfig.primary) {
                pool.end();
                pool = createPool({
                    logger,
                    config: replicationConfig.primary.connection as Partial<DatabaseConfig>,
                    onError: hookSystem.config?.onError
                        ? (error: unknown) => hookSystem.config?.onError?.({ error, sql: undefined, durationMs: 0 })
                        : undefined,
                });
            }

            topLevelReplicationManager = new ReplicationManager({
                primary: {
                    endpointId: primaryEndpointId,
                    pool,
                    connection: primaryConnection,
                    owned: true,
                },
                replicas: replicationConfig.replicas.map((r) => ({
                    id: r.id,
                    connection: r.connection,
                })),
                mode: replicationConfig.mode,
                lagThresholdMs: replicationConfig.lagThresholdMs,
                healthCheck: replicationConfig.healthCheck,
                logger,
            });

            try {
                topLevelReplicationManager.initialize();
            } catch (error) {
                pool.end();
                throw error;
            }
        }
    }

    let primaryPool: Pool | undefined = pool;

    if (!primaryPool && router) {
        const defaultShard = router.resolveDefaultShard();
        primaryPool = defaultShard?.pool;
    }
    if (!primaryPool && manager) {
        const firstShardId = manager.shardIds[0];
        if (firstShardId) {
            primaryPool = manager.getShard(firstShardId)?.pool;
        }
    }

    if (!primaryPool) {
        throw new Error("Unable to create database: no pool available");
    }

    // primaryPool is guaranteed non-null here; capture as const for closures
    const primary = primaryPool;

    // Unified shard executor: single source of truth for shard routing
    // resolution and per-pool client caching. All query paths delegate to it.
    const shardExecutor = new ShardExecutor({
        primaryPool: primary,
        manager,
        router,
        logger,
        hooks: hookSystem,
        retry,
        instrumentation: instrumentation as Record<string, unknown> | undefined,
        queryTimeout,
        timeout,
        connectTimeout,
    });

    // Online resharding manager (declared early so the write path can
    // consult it for fencing enforcement; initialized below).
    let reshardingApi: ReshardingApi | undefined;
    let schemaSyncService: import("./sharding/schema-sync-service.js").SchemaSyncService | undefined;
    let schemaSyncWorker: import("./sharding/schema-sync-worker.js").SchemaSyncWorker | undefined;
    let schemaSyncOwnedCoordinationPool: Pool | undefined;

    const client = createClient(primary, logger, {
        hooks: hookSystem,
        retry,
        instrumentation: instrumentation as Record<string, unknown> | undefined,
        queryTimeout,
        timeout,
        connectTimeout,
    });

    // Cache of clients per shard pool, to avoid creating a new client
    // (with its hooks, retry, and instrumentation setup) on every query.
    const shardClients = new Map<string, ReturnType<typeof createClient>>();
    shardClients.set("primary", client);

    function getClientForPool(pool: Pool): ReturnType<typeof createClient> {
        return shardExecutor.getClientForPool(pool);
    }

    const transactions = createTransaction({
        pool: primary,
        logger,
        hooks: hookSystem,
        retry,
    });

    // Cache of transactions per shard pool
    const shardTransactions = new Map<string, ReturnType<typeof createTransaction>>();
    shardTransactions.set("primary", transactions);

    function getTransactionForPool(pool: Pool): ReturnType<typeof createTransaction> {
        for (const [shardId, shardInfo] of manager ? Array.from(manager.shardIds.map((id) => [id, manager.getShard(id)]) as [string, { pool?: Pool } | undefined][]) : []) {
            if (shardInfo?.pool === pool) {
                let cached = shardTransactions.get(shardId);
                if (!cached) {
                    cached = createTransaction({
                        pool,
                        logger,
                        hooks: hookSystem,
                        retry,
                    });
                    shardTransactions.set(shardId, cached);
                }
                return cached;
            }
        }
        return transactions;
    }

    /**
     * Resolve a read endpoint for the given shard key and read preference.
     * Returns null when no replication topology applies (the caller falls back
     * to the primary pool). Throws ReplicaUnavailableError for
     * `replica-only` when no eligible replica exists.
     */
    function resolveReadEndpoint(
        shardKey: string | undefined,
        readPreference: ReadPreference,
    ): { endpointId: string; role: "primary" | "replica"; pool: Pool } | null {
        // Sharded: route within the shard's replication topology.
        if (shardKey && router && router.isEnabled) {
            try {
                const { id: shardId } = router.resolveShard(shardKey);
                const mgr = shardReplicationManagers.get(shardId);
                if (mgr) {
                    return mgr.readEndpoint(readPreference);
                }
            } catch {
                // Shard key could not be resolved; fall back to primary.
                // resolvePoolForShardKey already raises for unknown keys.
            }
            return null;
        }

        // Non-sharded: route within the top-level replication topology.
        if (topLevelReplicationManager) {
            return topLevelReplicationManager.readEndpoint(readPreference);
        }

        return null;
    }

    const shutdownSystem = createShutdown(primary, { logger });

    const from = createQueryBuilder({ query: unifiedQuery, logger });

// Unified query: routes to shard when shardKey is in options, otherwise uses primary pool
    async function unifiedQuery<T extends QueryResultRow = QueryResultRow>(
        text: string | object,
        values?: unknown[] | QueryOptions,
        options?: QueryOptions,
    ): Promise<import("pg").QueryResult<T>> {
        const queryOptions = { ...(options ?? {}) };
        const shardKey = queryOptions.shardKey as string | undefined;
        const readPreference = queryOptions.readPreference as ReadPreference | undefined;

        // Resolve the target pool and shard through the unified executor.
        const resolution = shardExecutor.resolve({
            shardKey,
            readPreference,
            operationId: queryOptions.operationId as string | undefined,
            operationType: "query",
        });
        const { pool: primaryPool, context } = resolution;
        const resolvedShardId = context.shardId !== "primary" ? context.shardId : null;

        // Tag the normalized options so the client layer can attach shard ID
        // and routing strategy to hook/metrics contexts.
        queryOptions.__shardId = resolvedShardId;
        queryOptions.__routingStrategy = router?.strategyName;

        // Record routing decision for observability.
        const usedDefault = !shardKey && !!resolvedShardId;
        shardMetrics.recordRoutingDecision({
            shardKey,
            resolvedShardId,
            strategy: router?.strategyName,
            usedDefault,
            missingKey: !shardKey,
        });

        const startedAt = Number(process.hrtime.bigint() / 1_000_000n);

        try {
            // Acquire shard for distributed drain tracking through the executor.
            await shardExecutor.acquireShard(resolvedShardId);

            // Read preference routing: reads may target a replica; writes always
            // target the primary. Intent is never inferred by parsing SQL text.
            let targetPool = primaryPool;

            // Atomic write fencing: for sharded writes to the primary pool, wrap
            // the statement in a transaction with a shared advisory lock acquired
            // before the gate check. This eliminates the TOCTOU race where a
            // writer could observe fenced=FALSE, then commit after cutover sets
            // fenced=TRUE.
            //
            // Read-only queries routed to replicas (readPreference !== "primary")
            // are not fenced — they cannot mutate the source shard.
            const isShardedWrite = reshardingApi && resolvedShardId && !readPreference;

            if (isShardedWrite) {
                // Use a dedicated connection for the entire fenced transaction.
                // getClientForPool() acquires a new connection per query(), which
                // would break the BEGIN/LOCK/query/COMMIT sequence across
                // different connections. pool.connect() gives us one connection
                // held for the full transaction.
                const client = await targetPool.connect();
                try {
                    await client.query("BEGIN");

                    // Acquire SHARED transaction-level advisory lock.
                    // Multiple writers can hold shared locks simultaneously on
                    // the same shard. Cutover holds an EXCLUSIVE lock, which
                    // blocks until all shared-lock holders finish.
                    const [lockKey1, lockKey2] = fencingLockKey(resolvedShardId);
                    await client.query(`SELECT pg_advisory_xact_lock_shared($1, $2)`, [lockKey1, lockKey2]);

                    // Pre-check gate table existence before starting the fenced
                    // transaction. A failed query in PostgreSQL aborts the entire
                    // transaction, so we must not query the gate table inside the
                    // transaction unless we know it exists.
                    const gateExistsResult = await targetPool.query(
                        `SELECT to_regclass('resharding_write_gate') IS NOT NULL AS exists`
                    );
                    const gateExists = gateExistsResult.rows[0]?.exists ?? false;

                    if (!gateExists) {
                        // Gate table doesn't exist yet. If resharding is configured
                        // AND there is an active migration, the gate table should
                        // exist — its absence means fencing cannot be guaranteed.
                        // Fail closed rather than silently executing without fencing.
                        if (reshardingApi && reshardingApi.migrationId) {
                            await client.query("ROLLBACK");
                            throw new ReshardingError(
                                `Write fencing gate table "resharding_write_gate" does not exist on shard "${resolvedShardId}". ` +
                                `Ensure the resharding schema has been initialized before enabling writes during an active migration.`,
                            );
                        }
                        // No active resharding migration — gate table is not needed.
                        // Execute without fencing (no cutover is in progress).
                        await client.query("ROLLBACK");
                        const targetClient = getClientForPool(targetPool);
                        let normalizedValues: unknown[] | undefined;
                        if (Array.isArray(values)) {
                            normalizedValues = values;
                        } else if (values !== null && typeof values === "object" && !Buffer.isBuffer(values)) {
                            const valuesObj = values as Record<string, unknown>;
                            if (Array.isArray(valuesObj.values)) {
                                normalizedValues = valuesObj.values;
                            }
                        }
                        const result = await targetClient.query(
                            text as string | import("pg").QueryConfig,
                            normalizedValues ?? [],
                            queryOptions,
                        );
                        return result as unknown as import("pg").QueryResult<T>;
                    }

                    // Check the durable write gate within the same locked
                    // transaction. Querying the gate on the same connection
                    // ensures we observe a consistent state with the lock held.
                    const gateResult = await client.query(
                        `SELECT fenced FROM resharding_write_gate WHERE id = 1`
                    );
                    const fenced = Number(gateResult.rowCount ?? 0) > 0 && Boolean(gateResult.rows[0]?.fenced);
                    if (fenced) {
                        await client.query("ROLLBACK");
                        throw new WriteFencedError(shardKey ?? resolvedShardId, resolvedShardId);
                    }

                    // Normalize values and execute within the fenced transaction.
                    let normalizedValues: unknown[] | undefined;
                    const normalizedOptions = queryOptions;
                    if (Array.isArray(values)) {
                        normalizedValues = values;
                    } else if (values !== null && typeof values === "object" && !Buffer.isBuffer(values)) {
                        const valuesObj = values as Record<string, unknown>;
                        if (Array.isArray(valuesObj.values)) {
                            normalizedValues = valuesObj.values;
                        }
                        for (const [key, val] of Object.entries(valuesObj)) {
                            if (key !== "values") {
                                normalizedOptions[key] = val;
                            }
                        }
                    }

                    const result = await client.query(
                        text as string,
                        normalizedValues ?? [],
                    );
                    await client.query("COMMIT");
                    return result as unknown as import("pg").QueryResult<T>;
                } catch (error) {
                    try {
                        await client.query("ROLLBACK");
                    } catch {
                        // Ignore rollback errors — the original error takes precedence.
                    }
                    throw error;
                } finally {
                    client.release();
                    await shardExecutor.releaseShard(resolvedShardId);
                }
            } else {
                // Gate table doesn't exist yet.
                if (reshardingApi?.migrationId) {
                    // A migration is active but we cannot verify the fence state
                    // — fail closed to prevent unsafe writes.
                    if (resolvedShardId) {
                        await shardExecutor.releaseShard(resolvedShardId);
                    }
                    throw new ReshardingError(
                        `Unable to verify write fence state for shard "${resolvedShardId}"`,
                        { code: "FENCE_VERIFICATION_FAILED", transient: true },
                    );
                }
                // No migration active — gate table not yet initialized.
                // Allow writes during bootstrap.
                const targetClient = getClientForPool(targetPool);
                let normalizedValues: unknown[] | undefined;
                if (Array.isArray(values)) {
                    normalizedValues = values;
                } else if (values !== null && typeof values === "object" && !Buffer.isBuffer(values)) {
                    const valuesObj = values as Record<string, unknown>;
                    if (Array.isArray(valuesObj.values)) {
                        normalizedValues = valuesObj.values;
                    }
                }
                const result = await targetClient.query(
                    text as string | import("pg").QueryConfig,
                    normalizedValues ?? [],
                    queryOptions,
                );
                if (resolvedShardId) {
                    await shardExecutor.releaseShard(resolvedShardId);
                }
                return result as unknown as import("pg").QueryResult<T>;
            }

            // Non-sharded or replica read path: no fencing needed.
            const targetClient = getClientForPool(targetPool);

            // Normalize values: if a plain object is passed as values, treat it as options
            let normalizedValues: unknown[] | undefined;
            const normalizedOptions = queryOptions;
            if (Array.isArray(values)) {
                normalizedValues = values as unknown[];
            } else if (values !== null && typeof values === "object" && !Buffer.isBuffer(values)) {
                const valuesObj = values as Record<string, unknown>;
                if (Array.isArray(valuesObj.values)) {
                    normalizedValues = valuesObj.values as unknown[];
                }
                // Merge any non-values keys into options
                for (const [key, val] of Object.entries(valuesObj)) {
                    if (key !== "values") {
                        normalizedOptions[key] = val;
                    }
                }
            }

            try {
                const result = await targetClient.query(
                    text as string | import("pg").QueryConfig,
                    normalizedValues,
                    normalizedOptions,
                );
                return result as unknown as import("pg").QueryResult<T>;
            } finally {
                // No connection to release here — targetClient.query() acquires
                // and releases its own connection from the target pool.
                await shardExecutor.releaseShard(resolvedShardId ?? undefined);
            }
        } catch (error) {
            shardMetrics.recordError({ shardId: resolvedShardId } as QueryContext);
            throw error;
        } finally {
            const durationMs = Number(process.hrtime.bigint() / 1_000_000n) - startedAt;
            shardMetrics.recordQuery(
                { shardId: resolvedShardId, routingStrategy: router?.strategyName } as QueryContext,
                durationMs,
            );
        }
    }

    // Unified transaction: routes to shard when shardKey is in txOptions
    // Transactions are pinned to exactly one shard. Cross-shard transactions
    // are not supported (no two-phase commit, no distributed transaction
    // coordinator). If a transaction is detected that would span shards, it
    // is rejected with CrossShardTransactionError.
    async function unifiedTransaction<T>(
        callback: (client: PoolClient) => Promise<T> | T,
        txOptions: TransactionOptions = {},
    ): Promise<T> {
        const shardKey = txOptions.shardKey;
        // P2-1: sharded transactions require an explicit shardKey so un-keyed
        // writes cannot silently land on the default shard.
        if (manager && !shardKey) {
            throw new CrossShardTransactionError({
                requestedShard: "none",
                actualShard: "default",
                operation: "transaction",
            });
        }
        const resolution = shardExecutor.resolve({
            shardKey,
            readPreference: "primary",
            operationType: "transaction",
        });
        const { pool: targetPool, context } = resolution;
        const resolvedShardId = context.shardId !== "primary" ? context.shardId : undefined;

        // Acquire shard for distributed drain tracking through the executor.
        await shardExecutor.acquireShard(resolvedShardId);

        try {
            // Atomic write fencing: for sharded transactions, manage the
            // connection directly so we can acquire a shared advisory lock
            // immediately after BEGIN and before the gate check. This ensures
            // the lock is held for the entire transaction, preventing TOCTOU
            // races with cutover.
            if (reshardingApi && resolvedShardId) {
                const client = await targetPool.connect();
                try {
                    await client.query("BEGIN");

                    // Acquire SHARED transaction-level advisory lock.
                    const [lockKey1, lockKey2] = fencingLockKey(resolvedShardId);
                    await client.query(`SELECT pg_advisory_xact_lock_shared($1, $2)`, [lockKey1, lockKey2]);

                    // Check the durable write gate within the locked transaction.
                    const fenced = await reshardingApi.isShardWriteFenced(resolvedShardId);
                    if (fenced) {
                        await client.query("ROLLBACK");
                        throw new WriteFencedError(shardKey ?? resolvedShardId, resolvedShardId);
                    }

                    try {
                        const result = await callback(client);
                        await client.query("COMMIT");
                        return result as T;
                    } catch (error) {
                        await client.query("ROLLBACK").catch(() => {});
                        throw error;
                    }
                } finally {
                    client.release();
                }
            }

            // Non-sharded path: use the cached transaction manager.
            const tx = getTransactionForPool(targetPool);
            return tx.transaction(callback, txOptions) as Promise<T>;
        } finally {
            await shardExecutor.releaseShard(resolvedShardId);
        }
    }

    // Shard-aware read helpers built on the unified executor so that
    // queryOne and prepared statements honor shardKey instead of always
    // executing against the primary pool.
    const shardAwareQueryOne = async <T extends QueryResultRow = QueryResultRow>(
        text: string | object,
        values?: unknown[] | QueryOptions,
        options?: QueryOptions,
    ): Promise<T | null> => {
        const result = await unifiedQuery<T>(text, values, options);
        return result?.rows?.[0] ?? null;
    };

    const shardAwarePrepared = (
        name: string,
        text: string,
        values: unknown[] = [],
        options: QueryOptions = {},
    ): Promise<import("pg").QueryResult> => {
        return unifiedQuery({ name, text, values }, values, options);
    };

    const shardAwarePrepare = (name: string, text: string): PreparedStatement => {
        return {
            name,
            text,
            execute: (
                values: unknown[] = [],
                options: QueryOptions = {},
            ): Promise<import("pg").QueryResult> => {
                return unifiedQuery({ name, text, values }, values, options);
            },
        };
    };

    const shardingHooks = normalizedSharding?.hooks;

    // Online resharding manager (coordination lives on the primary pool;
    // per-shard change logs and checkpoints live on each shard's pool).
    // Must be initialized before buildShardingApi() so the sharding
    // query/transaction closures capture the fencing-aware resharding API.
    if (manager) {
        const reshardingManager = createReshardingManager(primary, { logger }, manager);
        // Schema initialization is idempotent; kick it off without blocking
        // database creation. Operations lazily ensure tables exist on first use.
        reshardingManager.init().catch((error) => {
            logger?.error?.({ err: error }, "Failed to initialize resharding schema");
        });
        reshardingApi = reshardingManager;
        for (const shardId of manager.shardIds) {
            const shard = manager.getShard(shardId);
            if (shard?.pool) {
                reshardingManager.initShard(shardId, shard.pool).catch(() => {
                    // Per-shard table init is best-effort at construction time;
                    // it is retried lazily on first use.
                });
            }
        }

        // Schema auto-sync (optional). When sharding is enabled and no explicit
        // schemaSync config is provided, auto-enable it using the primary pool
        // as the coordination store so multi-shard DDL drift can be repaired
        // (P2-3).
        let schemaSyncCfg = (config as any)?.schemaSync;
        if (normalizedSharding?.enabled && !schemaSyncCfg) {
            schemaSyncCfg = { enabled: true, coordinationPool: primary };
        }
        if (schemaSyncCfg?.enabled) {
            const provided = schemaSyncCfg.coordinationPool;
            let coordinationPool = primary;
            if (provided) {
                if (typeof provided === "string") {
                    coordinationPool = createPool({ logger, config: { connectionString: provided } });
                    schemaSyncOwnedCoordinationPool = coordinationPool;
                } else if (typeof provided === "object" && typeof (provided as any).query === "function") {
                    coordinationPool = provided as Pool;
                } else {
                    coordinationPool = createPool({ logger, config: provided as any });
                    schemaSyncOwnedCoordinationPool = coordinationPool;
                }
            }
            schemaSyncService = new SchemaSyncService({
                coordinationPool,
                shardManager: manager,
                logger,
                hooks: schemaSyncCfg.hooks,
                options: schemaSyncCfg.syncOptions ?? {},
                syncOptions: schemaSyncCfg.syncOptions ?? {},
            });
            if (process.env.NODE_ENV !== "test" && schemaSyncCfg.workerOptions?.enabled !== false) {
                schemaSyncWorker = new SchemaSyncWorker(
                    schemaSyncService,
                    manager,
                    schemaSyncCfg.workerOptions ?? {},
                );
                try {
                    schemaSyncWorker.start();
                } catch {
                    // best effort
                }
            }
        }
    }

    // Distributed transaction coordinator — uses the primary pool for
    // coordination state. Initialization is lazy (on first use of
    // distributedTransaction) to avoid side effects during construction.
    // If max_prepared_transactions is 0, distributedTransaction will throw
    // MaxPreparedTransactionsError rather than silently falling back.
    let distributedTxCoordinator: DistributedTransactionCoordinator | undefined;
    if (manager && primary) {
        distributedTxCoordinator = new DistributedTransactionCoordinator({
            coordinationPool: primary,
            coordinatorId: `db-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
            logger: {
                debug: logger?.debug?.bind(logger) ?? (() => {}),
                info: logger?.info?.bind(logger) ?? (() => {}),
                warn: logger?.warn?.bind(logger) ?? (() => {}),
                error: logger?.error?.bind(logger) ?? (() => {}),
            },
        });
    }

    // bulk operations — shard-aware via unified executor
    const batchApi = createBatch({
        query: unifiedQuery,
        transaction: transactions.transaction,
    });

    const shardingApi = buildShardingApi(
        manager,
        router,
        health,
        logger,
        shardingHooks,
        shardReplicationManagers,
        reshardingApi,
        schemaSyncService,
        distributedTxCoordinator,
        shardExecutor,
        shardMetrics,
        batchApi,
        rebalanceTracker,
    );

    function buildReplicationApi(): ReplicationApi {
        const mgr = topLevelReplicationManager;
        if (!mgr) {
            return {
                get enabled(): boolean {
                    return false;
                },
                get mode(): ReadPreference {
                    return "primary";
                },
                get primaryEndpointId(): string {
                    return "primary";
                },
                get replicaEndpointIds(): ReadonlyArray<string> {
                    return [];
                },
                async health(): Promise<ReplicationHealthResult> {
                    return {
                        healthy: true,
                        checkedAt: new Date().toISOString(),
                        primary: {
                            endpointId: "primary",
                            role: "primary",
                            status: "up",
                            latencyMs: 0,
                            lagMs: null,
                            lagUnknown: true,
                            lastCheckedAt: new Date().toISOString(),
                            consecutiveFailures: 0,
                        },
                        replicas: [],
                        downReplicas: [],
                        upReplicas: [],
                        laggingReplicas: [],
                    };
                },
                async endpointHealth(): Promise<ReplicationEndpointHealth | undefined> {
                    return undefined;
                },
                get primaryPool(): Pool {
                    return primary;
                },
                readEndpoint(): {
                    endpointId: string;
                    role: "primary" | "replica";
                    pool: Pool;
                } | null {
                    return { endpointId: "primary", role: "primary", pool: primary };
                },
                async shutdown(): Promise<void> {
                    return;
                },
                get isClosed(): boolean {
                    return false;
                },
            };
        }

        return {
            get enabled(): boolean {
                return mgr.enabled;
            },
            get mode(): ReadPreference {
                return mgr.mode;
            },
            get primaryEndpointId(): string {
                return mgr.primaryEndpointId;
            },
            get replicaEndpointIds(): ReadonlyArray<string> {
                return mgr.replicaEndpointIds;
            },
            async health(): Promise<ReplicationHealthResult> {
                return mgr.health();
            },
            async endpointHealth(endpointId: string): Promise<ReplicationEndpointHealth | undefined> {
                return mgr.endpointHealth(endpointId);
            },
            get primaryPool(): Pool {
                return mgr.primaryPool;
            },
            readEndpoint(readPreference?: ReadPreference) {
                return mgr.readEndpoint(readPreference);
            },
            async shutdown(): Promise<void> {
                return mgr.shutdown();
            },
            get isClosed(): boolean {
                return mgr.isClosed;
            },
        };
    }

    const combinedShutdown = async (): Promise<void> => {
        // Shutdown per-shard replication managers (replica pools) first.
        for (const mgr of shardReplicationManagers.values()) {
            try {
                await mgr.shutdown();
            } catch {
                // Continue shutting down remaining endpoints.
            }
        }
        // Clear module-level connection-tracking sets so leaked clients can be
        // garbage-collected across createDatabase() generations (P2-7).
        clearActiveConnections();
        clearManualClients();
        // Top-level replication manager owns the primary pool in non-sharded mode.
        if (topLevelReplicationManager) {
            await topLevelReplicationManager.shutdown();
        } else if (manager) {
            await manager.shutdown();
        } else {
            await shutdownSystem.shutdown();
        }
        try {
            schemaSyncWorker?.stop?.();
        } catch {
            // ignore
        }
        if (schemaSyncOwnedCoordinationPool) {
            try {
                await schemaSyncOwnedCoordinationPool.end();
            } catch {
                // ignore
            }
        }
    };

    const replication = buildReplicationApi();

    // Expose getClient/releaseClient on the query function so the Model layer
    // can auto-acquire a transaction client for atomic single operations
    // (matching the insertMany atomic path).
    unifiedQuery.getClient = client.getClient;
    unifiedQuery.releaseClient = client.releaseClient;

    return {
        pool: primary,

        // queries — transparent sharding via unified executor
        getClient: client.getClient,
        query: unifiedQuery,
        queryOne: shardAwareQueryOne,
        releaseClient: client.releaseClient,
        prepare: shardAwarePrepare,
        prepared: shardAwarePrepared,

        // transactions — transparent sharding via unified executor
        transaction: unifiedTransaction,
        savepoint: transactions.savepoint,

        // streaming — shard-aware via unified executor
        ...createStream(primary, logger, {}, (shardKey?: string, shardId?: string) => {
            if (!shardKey && !shardId) return primary;
            const { pool } = shardExecutor.resolve({ shardKey, shardId, operationType: "stream" });
            return pool;
        }),
        ...createCursor(primary, (shardKey?: string, shardId?: string) => {
            if (!shardKey && !shardId) return primary;
            const { pool } = shardExecutor.resolve({ shardKey, shardId, operationType: "cursor" });
            return pool;
        }),

        // health & lifecycle
        ...createCheck(primary),
        shutdown: combinedShutdown,
        get isClosed(): boolean {
            return manager?.isClosed ?? shutdownSystem.isClosed;
        },

        // bulk operations
        batch: batchApi,

        // query builder & models
        from,
        table: from,
        model: createModelFactory({
            query: unifiedQuery,
            logger,
        }),

        // schema & migrations
        schema: createSchemaManager(primary),
        migrate: createMigrator(primary, { logger }),

        // instrumentation
        hooks: hookSystem,
        metrics: hookSystem.metrics,
        retry: createRetry(retryDefaults ?? {}),

        // sharding — kept as advanced/deprecated API
        sharding: shardingApi,

        // online resharding / data migration
        resharding: reshardingApi,

        // schema auto-sync
        schemaSync: schemaSyncService,
        schemaSyncWorker: schemaSyncWorker,

        // replication — primary / read replica routing
        replication,
    };
}
