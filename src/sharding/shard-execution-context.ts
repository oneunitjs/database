// ---------------------------------------------------------------------------
// Shard Execution Context — immutable routing context for a single operation.
// ---------------------------------------------------------------------------

import type { Pool, PoolClient } from "pg";
import type { ReadPreference, QueryOptions } from "../types.js";

export type OperationType =
    | "query"
    | "transaction"
    | "batch"
    | "stream"
    | "cursor"
    | "schema"
    | "migration";

export interface ShardExecutionContext {
    readonly shardId: string;
    readonly shardKey?: string;
    readonly operationId: string;
    readonly operationType: OperationType;
    readonly pool: Pool;
    readonly readPreference?: ReadPreference;
    readonly fenced: boolean;
}

/**
 * Create an immutable shard execution context.
 */
export function createShardExecutionContext({
    shardId,
    shardKey,
    operationId,
    operationType,
    pool,
    readPreference,
    fenced = false,
}: {
    shardId: string;
    shardKey?: string;
    operationId: string;
    operationType: OperationType;
    pool: Pool;
    readPreference?: ReadPreference;
    fenced?: boolean;
}): ShardExecutionContext {
    return Object.freeze({
        shardId,
        shardKey,
        operationId,
        operationType,
        pool,
        readPreference,
        fenced,
    });
}

/**
 * Create a context for non-sharded (primary pool) execution.
 */
export function createPrimaryExecutionContext({
    operationId,
    operationType,
    pool,
    readPreference = "primary",
}: {
    operationId: string;
    operationType: OperationType;
    pool: Pool;
    readPreference?: ReadPreference;
}): ShardExecutionContext {
    return Object.freeze({
        shardId: "primary",
        operationId,
        operationType,
        pool,
        readPreference,
        fenced: false,
    });
}
