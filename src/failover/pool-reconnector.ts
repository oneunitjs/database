import { Pool, type PoolConfig } from "pg";

/**
 * Result of a pool reconnection attempt.
 */
export interface ReconnectionResult {
    nodeId: string;
    success: boolean;
    oldPool?: Pool;
    newPool?: Pool;
    durationMs: number;
    retries: number;
    error?: string;
}

/**
 * Options for the PoolReconnector.
 */
export interface ReconnectionOptions {
    /** Maximum number of reconnection attempts. Default: 5. */
    maxRetries: number;
    /** Base delay in ms between retries (exponential backoff). Default: 1_000. */
    retryDelayMs: number;
    /** Maximum delay in ms between retries. Default: 30_000. */
    maxRetryDelayMs: number;
    /** Whether to validate the new pool before swapping. Default: true. */
    validatePool: boolean;
}

/**
 * Default reconnection options.
 */
export const DEFAULT_RECONNECTION_OPTIONS: Required<ReconnectionOptions> = {
    maxRetries: 5,
    retryDelayMs: 1_000,
    maxRetryDelayMs: 30_000,
    validatePool: true,
};

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
 * Provider function that supplies a PoolConfig for a given node ID.
 */
export type PoolConfigProvider = (nodeId: string) => Promise<PoolConfig>;

/**
 * Reconnections database connection pools after a failover, with exponential
 * backoff retry logic. Pools are swapped atomically (new pool validated before
 * old pool is closed) to avoid leaving a node without a working pool.
 */
export class PoolReconnector {
    private readonly pendingReconnections = new Map<string, Promise<ReconnectionResult>>();

    constructor(
        private readonly poolMap: Map<string, Pool>,
        private readonly configProvider: PoolConfigProvider,
        private readonly logger: RequiredFailoverLogger,
        private readonly options: Required<ReconnectionOptions> = DEFAULT_RECONNECTION_OPTIONS,
        private readonly poolFactory: (config: PoolConfig) => Pool = (config) => new Pool(config),
    ) {}

    /**
     * Reconnect the pool for a given node. If a reconnection is already in
     * progress for the same node, returns the existing promise.
     */
    async reconnectPool(nodeId: string, newConfig?: PoolConfig): Promise<ReconnectionResult> {
        const existing = this.pendingReconnections.get(nodeId);
        if (existing) return existing;

        const promise = this.doReconnectPool(nodeId, newConfig);
        this.pendingReconnections.set(nodeId, promise);
        try {
            return await promise;
        } finally {
            this.pendingReconnections.delete(nodeId);
        }
    }

    /**
     * Reconnect the primary pool.
     */
    async reconnectPrimaryPool(nodeId: string): Promise<ReconnectionResult> {
        return this.reconnectPool(nodeId);
    }

    /**
     * Reconnect pools for all given replica nodes concurrently.
     */
    async reconnectReplicaPools(replicaNodeIds: string[]): Promise<ReconnectionResult[]> {
        return Promise.all(replicaNodeIds.map((nodeId) => this.reconnectPool(nodeId)));
    }

    private async doReconnectPool(
        nodeId: string,
        newConfig?: PoolConfig,
        retries: number = 0,
    ): Promise<ReconnectionResult> {
        const startTime = Date.now();

        try {
            const oldPool = this.poolMap.get(nodeId);
            if (!oldPool) {
                throw new Error(`No pool found for node ${nodeId}`);
            }

            const config = newConfig ?? (await this.configProvider(nodeId));
            const newPool = this.poolFactory(config);

            if (this.options.validatePool) {
                await this.testPool(newPool);
            }

            // Atomic swap: new pool is ready before old pool is closed.
            this.poolMap.set(nodeId, newPool);
            await oldPool.end();

            this.logger.info(
                { nodeId, retries, durationMs: Date.now() - startTime },
                "Pool reconnected successfully",
            );

            return {
                nodeId,
                success: true,
                oldPool,
                newPool,
                durationMs: Date.now() - startTime,
                retries,
            };
        } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));

            if (retries >= this.options.maxRetries) {
                this.logger.error(
                    { nodeId, retries: this.options.maxRetries, err },
                    "Pool reconnection failed after max retries",
                );
                return {
                    nodeId,
                    success: false,
                    durationMs: Date.now() - startTime,
                    retries: this.options.maxRetries,
                    error: err.message,
                };
            }

            const delayMs = Math.min(
                this.options.retryDelayMs * Math.pow(2, retries),
                this.options.maxRetryDelayMs,
            );

            this.logger.warn(
                { nodeId, retry: retries + 1, delayMs, err: err.message },
                "Pool reconnection attempt failed, retrying",
            );

            await new Promise((resolve) => setTimeout(resolve, delayMs));
            return this.doReconnectPool(nodeId, newConfig, retries + 1);
        }
    }

    private async testPool(pool: Pool): Promise<void> {
        const client = await pool.connect();
        try {
            await client.query("SELECT 1");
        } finally {
            client.release();
        }
    }

    /**
     * Get the number of in-flight reconnections (for testing/introspection).
     */
    getPendingCount(): number {
        return this.pendingReconnections.size;
    }
}
