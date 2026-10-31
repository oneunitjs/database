// ---------------------------------------------------------------------------
// Shard Executor — unified shard routing and client-cache abstraction.
//
// Provides a single source of truth for:
//   1. Resolving a shard key / shard id to a pool (ShardRoutingOptions → pool)
//   2. Caching per-pool client wrappers (created once per pool, reused)
//   3. Acquiring/releasing shard leases for distributed drain tracking
//
// The actual query execution (hooks, metrics, write fencing) remains in
// `unifiedQuery()` in database.ts, which delegates routing resolution to this
// executor. Non-sharded databases resolve to the primary pool (shardId ===
// "primary").
// ---------------------------------------------------------------------------

import type { PoolClient } from "pg";
import type {
    Pool,
    QueryOptions,
    QueryResultRow,
    ReadPreference,
    RetryOptions,
} from "../types.js";
import { createClient } from "../client/client.js";
import type { ShardManager } from "./shard-manager.js";
import type { ShardRouter } from "./shard-router.js";
import type { Logger } from "../types.js";
import {
    ShardNotFoundError,
    ShardRoutingError,
} from "./errors.js";
import type {
    ShardExecutionContext,
    OperationType,
} from "./shard-execution-context.js";
import {
    createShardExecutionContext,
    createPrimaryExecutionContext,
} from "./shard-execution-context.js";

export interface ShardRoutingOptions {
    shardKey?: string;
    shardId?: string;
    readPreference?: ReadPreference;
    operationId?: string;
    operationType?: OperationType;
}

export interface ShardQueryOptions extends QueryOptions {
    shardKey?: string;
    shardId?: string;
    readPreference?: ReadPreference;
}

interface ShardResolution {
    context: ShardExecutionContext;
    pool: Pool;
    shardId: string | null;
}

/**
 * ShardExecutor — single entry point for all shard routing resolution and
 * per-pool client caching.
 */
export class ShardExecutor {
    private readonly primaryPool: Pool;
    private readonly manager: ShardManager | undefined;
    private readonly router: ShardRouter | undefined;
    private readonly logger?: Logger;
    private readonly shardClients = new Map<string, ReturnType<typeof createClient>>();

    constructor({
        primaryPool,
        manager,
        router,
        logger,
        hooks,
        retry,
        instrumentation,
        queryTimeout,
        timeout,
        connectTimeout,
    }: {
        primaryPool: Pool;
        manager?: ShardManager;
        router?: ShardRouter;
        logger?: Logger;
        hooks: ReturnType<typeof import("../client/hooks.js").createHooks>;
        retry?: RetryOptions | boolean | number;
        instrumentation?: Record<string, unknown>;
        queryTimeout?: number;
        timeout?: number;
        connectTimeout?: number;
    }) {
        this.primaryPool = primaryPool;
        this.manager = manager;
        this.router = router;
        this.logger = logger;
        const clientOpts = {
            hooks,
            retry,
            instrumentation: instrumentation as Record<string, unknown> | undefined,
            queryTimeout,
            timeout,
            connectTimeout,
        };
        this.shardClients.set("primary", createClient(primaryPool, logger, clientOpts));
        this.clientOpts = clientOpts;
    }

    private readonly clientOpts: {
        hooks: ReturnType<typeof import("../client/hooks.js").createHooks>;
        retry: RetryOptions | boolean | number | undefined;
        instrumentation: Record<string, unknown> | undefined;
        queryTimeout: number | undefined;
        timeout: number | undefined;
        connectTimeout: number | undefined;
    };

    /**
     * Resolve the target pool and shard for the given routing options.
     * Returns shardId === null when no sharding is configured (primary pool).
     */
    resolve(options: ShardRoutingOptions): ShardResolution {
        const {
            shardKey,
            shardId,
            readPreference = "primary",
            operationId = crypto.randomUUID(),
            operationType = "query",
        } = options;

        // Non-sharded mode: always use primary pool.
        if (!this.manager || !this.router || !this.router.isEnabled) {
            const context = createPrimaryExecutionContext({
                operationId,
                operationType,
                pool: this.primaryPool,
                readPreference,
            });
            return { context, pool: this.primaryPool, shardId: null };
        }

        // Sharded mode: resolve via explicit shard id, shard key, or default.
        // Fall back to the primary pool when no routing input is available.
        let resolvedShardId: string;
        let pool: Pool;

        if (shardId) {
            const shard = this.manager.getShard(shardId);
            if (!shard || !shard.pool) {
                throw new Error(`Shard "${shardId}" not found or pool unavailable`);
            }
            resolvedShardId = shardId;
            pool = shard.pool;
        } else if (shardKey) {
            try {
                const { id, pool: shardPool } = this.router.resolveShard(shardKey);
                resolvedShardId = id;
                pool = shardPool;
            } catch (error) {
                if (error instanceof ShardNotFoundError || error instanceof ShardRoutingError) {
                    throw new ShardRoutingError(
                        `Unable to route to a shard for key "${shardKey}": ${error.message}`,
                    );
                }
                throw error;
            }
        } else {
            // No routing input: fall back to the primary pool (backward compatible).
            const context = createPrimaryExecutionContext({
                operationId,
                operationType,
                pool: this.primaryPool,
                readPreference,
            });
            return { context, pool: this.primaryPool, shardId: null };
        }

        const context = createShardExecutionContext({
            shardId: resolvedShardId,
            shardKey,
            operationId,
            operationType,
            pool,
            readPreference,
        });
        return { context, pool, shardId: resolvedShardId };
    }

    /**
     * Get or create a client wrapper for the given pool. One client per pool
     * is cached and reused; pools are never duplicated.
     */
    getClientForPool(pool: Pool): ReturnType<typeof createClient> {
        const key = this.poolKey(pool);
        let client = this.shardClients.get(key);
        if (!client) {
            client = createClient(pool, this.logger, this.clientOpts);
            this.shardClients.set(key, client);
        }
        return client;
    }

    private poolKey(pool: Pool): string {
        return `pool:${String(pool)}`;
    }

    /**
     * Get a client wrapper for a specific shard id.
     */
    getClientForShard(shardId: string): ReturnType<typeof createClient> | undefined {
        if (shardId === "primary") {
            return this.shardClients.get("primary");
        }
        if (!this.manager) return undefined;
        const shard = this.manager.getShard(shardId);
        if (!shard || !shard.pool) return undefined;
        const key = `shard:${shardId}`;
        let client = this.shardClients.get(key);
        if (!client) {
            client = createClient(shard.pool, this.logger, this.clientOpts);
            this.shardClients.set(key, client);
        }
        return client;
    }

    /**
     * Acquire a shard lease for distributed drain tracking. No-op for primary.
     */
    async acquireShard(shardId: string | null | undefined): Promise<void> {
        if (!shardId || shardId === "primary" || !this.manager) return;
        await this.manager.acquireShard(shardId);
    }

    /**
     * Release a shard lease. No-op for primary. Safe to call multiple times.
     */
    async releaseShard(shardId: string | null | undefined): Promise<void> {
        if (!shardId || shardId === "primary" || !this.manager) return;
        await this.manager.releaseShard(shardId).catch(() => {});
    }

    /**
     * Execute a callback with a dedicated connection from the resolved shard pool.
     * The connection is held for the entire callback duration and released after.
     */
    async withClient<T>(
        context: ShardExecutionContext,
        callback: (client: PoolClient) => Promise<T>,
    ): Promise<T> {
        await this.acquireShard(context.shardId !== "primary" ? context.shardId : null);
        const client = await context.pool.connect();
        try {
            return await callback(client);
        } finally {
            client.release();
            await this.releaseShard(context.shardId !== "primary" ? context.shardId : null);
        }
    }

    shutdown(): void {
        this.shardClients.clear();
    }
}
