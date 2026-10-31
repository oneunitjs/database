import { DatabaseError, TimeoutError } from "../client/errors.js";

const SHARD_NOT_FOUND = "SHARD_NOT_FOUND";
const SHARD_UNAVAILABLE = "SHARD_UNAVAILABLE";
const SHARDING_DISABLED = "SHARDING_DISABLED";
const SHARD_CONFIG_ERROR = "SHARD_CONFIG_ERROR";
const CROSS_SHARD_TRANSACTION = "CROSS_SHARD_TRANSACTION";
const SHARD_NOT_READY = "SHARD_NOT_READY";
const SHARD_COORDINATION = "SHARD_COORDINATION";
const SHARD_TOPOLOGY_STALE = "SHARD_TOPOLOGY_STALE";
const SHARD_OPERATION_TIMEOUT = "SHARD_OPERATION_TIMEOUT";
const SHARD_DRAIN_TIMEOUT = "SHARD_DRAIN_TIMEOUT";
const SHARD_MIGRATION_PARTIAL_FAILURE = "SHARD_MIGRATION_PARTIAL_FAILURE";

export class ShardNotFoundError extends DatabaseError {
    constructor(shardId: string) {
        super(`Shard not found: ${shardId}`, {
            code: SHARD_NOT_FOUND,
            transient: false,
        });
        this.name = "ShardNotFoundError";
    }
}

export class ShardUnavailableError extends DatabaseError {
    constructor(shardId: string, cause?: Error) {
        super(`Shard unavailable: ${shardId}`, {
            code: SHARD_UNAVAILABLE,
            transient: true,
            cause,
        });
        this.name = "ShardUnavailableError";
    }
}

export class ShardingDisabledError extends DatabaseError {
    constructor() {
        super("Sharding is disabled", {
            code: SHARDING_DISABLED,
            transient: false,
        });
        this.name = "ShardingDisabledError";
    }
}

export class ShardConfigError extends DatabaseError {
    constructor(message: string, options?: { cause?: Error }) {
        super(message, {
            code: SHARD_CONFIG_ERROR,
            transient: false,
            ...options,
        });
        this.name = "ShardConfigError";
    }
}

export class CrossShardTransactionError extends DatabaseError {
    constructor(options?: {
        requestedShard?: string;
        actualShard?: string;
        operation?: string;
    }) {
        const requested = options?.requestedShard ?? "unknown";
        const actual = options?.actualShard ?? "unknown";
        const operation = options?.operation ?? "transaction";
        super(
            `Cross-shard transactions are not supported: ${operation} requested shard "${requested}" but would span multiple shards. ` +
            `Transactions are pinned to a single shard for their entire lifetime. ` +
            `Actual transaction shard: ${actual}. ` +
            `Use application-level workflows (saga pattern, outbox pattern) for operations that span multiple shards.`,
            {
                code: CROSS_SHARD_TRANSACTION,
                transient: false,
            },
        );
        this.name = "CrossShardTransactionError";
        this.requestedShard = options?.requestedShard;
        this.actualShard = options?.actualShard;
        this.operation = options?.operation;
    }

    readonly requestedShard?: string;
    readonly actualShard?: string;
    readonly operation?: string;
}

export function isShardNotFoundError(error: unknown): boolean {
    return (
        error instanceof ShardNotFoundError ||
        ((error as Record<string, unknown>)?.code === SHARD_NOT_FOUND)
    );
}

export function isShardUnavailableError(error: unknown): boolean {
    return (
        error instanceof ShardUnavailableError ||
        ((error as Record<string, unknown>)?.code === SHARD_UNAVAILABLE)
    );
}

export function isShardingDisabledError(error: unknown): boolean {
    return (
        error instanceof ShardingDisabledError ||
        ((error as Record<string, unknown>)?.code === SHARDING_DISABLED)
    );
}

export function isCrossShardTransactionError(error: unknown): boolean {
    return (
        error instanceof CrossShardTransactionError ||
        ((error as Record<string, unknown>)?.code === CROSS_SHARD_TRANSACTION)
    );
}

/**
 * Error thrown when a query or transaction cannot be routed to a shard.
 */
export class ShardRoutingError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ShardRoutingError";
    }
}

export function isShardRoutingError(error: unknown): boolean {
    return (
        error instanceof ShardRoutingError ||
        ((error as Record<string, unknown>)?.name === "ShardRoutingError")
    );
}

/**
 * Thrown when a shard exists but is not ready to serve operations (e.g.,
 * still being provisioned or recovering from failover).
 */
export class ShardNotReadyError extends DatabaseError {
    constructor(shardId: string, reason?: string) {
        super(
            `Shard "${shardId}" is not ready${reason ? `: ${reason}` : ""}`,
            {
                code: SHARD_NOT_READY,
                transient: true,
            },
        );
        this.name = "ShardNotReadyError";
        this.shardId = shardId;
        this.reason = reason;
    }

    readonly shardId: string;
    readonly reason?: string;
}

/**
 * Thrown when the coordination database (used for distributed drain state)
 * is unavailable and the operation cannot proceed safely.
 */
export class ShardCoordinationError extends DatabaseError {
    constructor(message: string, options?: { cause?: Error; transient?: boolean }) {
        super(message, {
            code: SHARD_COORDINATION,
            transient: options?.transient ?? true,
            ...options,
        });
        this.name = "ShardCoordinationError";
    }
}

/**
 * Thrown when cached routing/topology information is stale relative to the
 * durable topology generation.
 */
export class ShardTopologyStaleError extends DatabaseError {
    constructor(options?: { routingVersion?: number; expectedVersion?: number }) {
        super(
            `Shard topology is stale (routing version ${options?.routingVersion ?? "unknown"}, expected ${options?.expectedVersion ?? "unknown"})`,
            {
                code: SHARD_TOPOLOGY_STALE,
                transient: true,
            },
        );
        this.name = "ShardTopologyStaleError";
        this.routingVersion = options?.routingVersion;
        this.expectedVersion = options?.expectedVersion;
    }

    readonly routingVersion?: number;
    readonly expectedVersion?: number;
}

/**
 * Thrown when an operation exceeds its shard-scoped timeout.
 */
export class ShardOperationTimeoutError extends TimeoutError {
    constructor(shardId: string, operation: string, timeoutMs: number) {
        super(
            `Shard operation "${operation}" on shard "${shardId}" timed out after ${timeoutMs}ms`,
            {
                code: SHARD_OPERATION_TIMEOUT,
                timeout: timeoutMs,
            },
        );
        this.name = "ShardOperationTimeoutError";
        this.shardId = shardId;
        this.operation = operation;
    }

    readonly shardId: string;
    readonly operation: string;
}

/**
 * Thrown when shard drain timeout is reached before all operations complete.
 */
export class ShardDrainTimeoutError extends DatabaseError {
    constructor(shardId: string, activeOps: number, timeoutMs: number) {
        super(
            `Shard "${shardId}" drain timeout: ${activeOps} operation(s) still in flight after ${timeoutMs}ms`,
            {
                code: SHARD_DRAIN_TIMEOUT,
                transient: true,
            },
        );
        this.name = "ShardDrainTimeoutError";
        this.shardId = shardId;
        this.activeOps = activeOps;
        this.timeoutMs = timeoutMs;
    }

    readonly shardId: string;
    readonly activeOps: number;
    readonly timeoutMs: number;
}

/**
 * Thrown when a per-shard migration succeeds on some shards but fails on
 * others, resulting in partial application.
 */
export class ShardMigrationPartialFailureError extends DatabaseError {
    constructor(options: {
        successfulShards: string[];
        failedShards: Array<{ shardId: string; error: string }>;
    }) {
        super(
            `Migration partially applied: ${options.successfulShards.length} succeeded, ${options.failedShards.length} failed`,
            {
                code: SHARD_MIGRATION_PARTIAL_FAILURE,
                transient: false,
            },
        );
        this.name = "ShardMigrationPartialFailureError";
        this.successfulShards = options.successfulShards;
        this.failedShards = options.failedShards;
    }

    readonly successfulShards: string[];
    readonly failedShards: Array<{ shardId: string; error: string }>;
}

export function isShardNotReadyError(error: unknown): boolean {
    return (
        error instanceof ShardNotReadyError ||
        ((error as Record<string, unknown>)?.code === SHARD_NOT_READY)
    );
}

export function isShardCoordinationError(error: unknown): boolean {
    return (
        error instanceof ShardCoordinationError ||
        ((error as Record<string, unknown>)?.code === SHARD_COORDINATION)
    );
}

export function isShardTopologyStaleError(error: unknown): boolean {
    return (
        error instanceof ShardTopologyStaleError ||
        ((error as Record<string, unknown>)?.code === SHARD_TOPOLOGY_STALE)
    );
}

export function isShardOperationTimeoutError(error: unknown): boolean {
    return (
        error instanceof ShardOperationTimeoutError ||
        ((error as Record<string, unknown>)?.code === SHARD_OPERATION_TIMEOUT)
    );
}

export function isShardDrainTimeoutError(error: unknown): boolean {
    return (
        error instanceof ShardDrainTimeoutError ||
        ((error as Record<string, unknown>)?.code === SHARD_DRAIN_TIMEOUT)
    );
}

export function isShardMigrationPartialFailureError(error: unknown): boolean {
    return (
        error instanceof ShardMigrationPartialFailureError ||
        ((error as Record<string, unknown>)?.code === SHARD_MIGRATION_PARTIAL_FAILURE)
    );
}
