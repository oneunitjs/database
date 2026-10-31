import type { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";
import type { QueryOptions } from "../types.js";
import type { DistributedTxOptions } from "../failover/failover-types.js";
import { DistributedTransactionCoordinator } from "./distributed-transaction-coordinator.js";
import { DistributedTransactionError } from "./distributed-transaction-coordinator.js";

export interface DistributedTransactionExecuteOptions extends QueryOptions {}

export class DistributedTransactionContext {
    readonly transactionId: string;
    private readonly coordinator: DistributedTransactionCoordinator;
    private readonly shardPools: Map<string, Pool>;
    private readonly shardIdByKey: Map<string, string>;
    private readonly resolveShardKey: (shardKey: string) => string;
    private completed = false;
    private aborted = false;

    constructor(
        transactionId: string,
        coordinator: DistributedTransactionCoordinator,
        shardPools: Map<string, Pool>,
        resolveShardKey: (shardKey: string) => string,
    ) {
        this.transactionId = transactionId;
        this.coordinator = coordinator;
        this.shardPools = shardPools;
        this.resolveShardKey = resolveShardKey;
        this.shardIdByKey = new Map();
    }

    /**
     * Execute a SQL statement on a specific shard within the distributed
     * transaction. The first call for a given shard begins a local
     * transaction and registers the participant.
     */
    async execute<T extends QueryResultRow = QueryResultRow>(
        shardKey: string,
        text: string | object,
        values?: unknown[] | QueryOptions,
        options?: QueryOptions,
    ): Promise<QueryResult<T>> {
        if (this.completed) {
            throw new DistributedTransactionError(
                `Distributed transaction ${this.transactionId} has already been committed or aborted`,
                this.transactionId,
            );
        }
        if (this.aborted) {
            throw new DistributedTransactionError(
                `Distributed transaction ${this.transactionId} has been aborted`,
                this.transactionId,
            );
        }

        const shardId = this.resolveShardKey(shardKey);
        const pool = this.shardPools.get(shardId);
        if (!pool) {
            throw new DistributedTransactionError(
                `No pool configured for shard "${shardId}" (key: ${shardKey})`,
                this.transactionId,
            );
        }

        // Normalize arguments: (text, values?, options?)
        let resolvedValues: unknown[] | undefined;
        let resolvedOptions: QueryOptions | undefined;

        if (values !== undefined) {
            if (Array.isArray(values)) {
                resolvedValues = values;
                resolvedOptions = options;
            } else if (typeof values === "object" && values !== null && !Array.isArray(values) && !Buffer.isBuffer(values)) {
                // values is actually an options object
                resolvedOptions = values as QueryOptions;
            } else {
                resolvedValues = values as unknown[];
                resolvedOptions = options;
            }
        }

        // Register participant and begin local transaction if not already done
        const client = await this.coordinator.addParticipant(
            this.transactionId,
            shardId,
            pool,
            { isolation: resolvedOptions?.isolation as any, timeout: resolvedOptions?.timeout },
        );

        try {
            return (await client.query(text as string, resolvedValues ?? [])) as unknown as QueryResult<T>;
        } catch (error) {
            // Mark transaction as aborted — the query failed
            this.aborted = true;
            throw error;
        }
    }

    /**
     * Get the raw client for a shard. Use with caution — operations executed
     * directly on the client are part of the distributed transaction.
     */
    async getClient(shardKey: string): Promise<PoolClient> {
        const shardId = this.resolveShardKey(shardKey);
        const pool = this.shardPools.get(shardId);
        if (!pool) {
            throw new DistributedTransactionError(
                `No pool configured for shard "${shardId}" (key: ${shardKey})`,
                this.transactionId,
            );
        }
        return this.coordinator.addParticipant(
            this.transactionId,
            shardId,
            pool,
            {},
        );
    }

    /**
     * Get all registered participant shard IDs.
     */
    getParticipantShards(): string[] {
        return Array.from(this.shardIdByKey.values());
    }
}
