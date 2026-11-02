import type { Pool, PoolClient, QueryResult, QueryResultRow, PoolConfig, QueryConfig } from "pg";
import type { Readable } from "node:stream";
import type { DatabaseError, TimeoutError } from "./client/errors.js";

export type { DatabaseError, TimeoutError };

export type { Pool, PoolClient, QueryResult, QueryResultRow };
export type Logger = {
    debug?(payload: unknown, message?: string): void;
    info?(payload: unknown, message?: string): void;
    warn?(payload: unknown, message?: string): void;
    error?(payload: unknown, message?: string): void;
};

export type SslConfig =
    | false
    | {
          rejectUnauthorized?: boolean;
          ca?: string;
      };

export interface IdleTransactionTimeout {
    enabled: boolean;
    timeout?: number;
}

export type ShardStrategyName =
    | "hash"
    | "consistent-hash"
    | "rendezvous"
    | "range"
    | "directory"
    | "tenant"
    | "composite"
    | "geographic"
    | "weighted"
    | (string & {});

export interface ShardConfig {
    id: string;
    connection: PoolConfig;
    /**
     * Version when this shard was added or last modified.
     * Used for tracking shard map changes during resharding.
     */
    version?: number;
    /**
     * Optional per-shard replication configuration. When present, the shard's
     * `connection` is the primary endpoint and `replication.replicas` are the
     * read replicas. Absent (or `enabled: false`) preserves single-pool behavior.
     */
    replication?: ShardReplicationConfig;
}

export interface WeightedShardConfig extends ShardConfig {
    weight: number;
}

export interface ShardRange {
    shardId: string;
    start: string | number;
    end: string | number;
    startInclusive?: boolean;
    endInclusive?: boolean;
}

// ---------------------------------------------------------------------------
// Resharding types
// ---------------------------------------------------------------------------

export type ShardMapVersion = number;

export interface ShardMapEntry {
    id: string;
    connection: PoolConfig;
    version: ShardMapVersion;
    addedAt: string;
}

export interface ReshardingHooks {
    /**
     * Called before shard membership changes.
     * Receives the old and new version, and the shards being added/removed.
     * If this throws, the resharding is aborted.
     */
    onBeforeReshard?: (
        oldVersion: ShardMapVersion,
        newVersion: ShardMapVersion,
        added: ShardConfig[],
        removed: string[],
    ) => Promise<void> | void;

    /**
     * Called after shard membership changes have been applied.
     * Receives the old and new version, and the shards that were added/removed.
     */
    onAfterReshard?: (
        oldVersion: ShardMapVersion,
        newVersion: ShardMapVersion,
        added: ShardConfig[],
        removed: string[],
    ) => Promise<void> | void;
}

// ---------------------------------------------------------------------------
// Discriminated sharding configuration
// ---------------------------------------------------------------------------

export interface BaseShardingConfig {
    enabled: boolean;
    defaultShardId?: string;
    /**
     * Custom strategy instance for pluggable shard selection. When set,
     * takes precedence over `strategy` name lookup. The strategy's `name`
     * is used for routing diagnostics and validation.
     */
    strategyInstance?: ShardStrategy;
    /**
     * Current routing version. Increments on every shard map change.
     * Used by strategies and clients to detect when routing tables are stale.
     */
    routingVersion?: ShardMapVersion;
    /**
     * Optional hooks called before/after shard membership changes (resharding).
     */
    hooks?: ReshardingHooks;
    /**
     * Dedicated coordination database used for distributed drain state
     * (`shard_drain_state`, `shard_active_ops`). Independently addressable
     * from shard databases. When omitted, drain state is stored on the
     * first available shard pool.
     */
    coordination?: PoolConfig;
    /**
     * Coordination mode for distributed drain. "strict" uses the coordination
     * database, "local-only" uses only local state, "fail-closed" rejects
     * operations when coordination is unavailable.
     */
    coordinationMode?: "strict" | "local-only" | "fail-closed";
    /**
     * Operations without a heartbeat after this interval are treated as abandoned.
     * Default: 300000ms (5 minutes).
     */
    abandonedOperationTimeoutMs?: number;
}

export interface HashShardingConfig extends BaseShardingConfig {
    strategy: "hash";
    shards: ShardConfig[];
}

export interface ConsistentHashShardingConfig extends BaseShardingConfig {
    strategy: "consistent-hash";
    shards: ShardConfig[];
    virtualNodes?: number;
}

export interface RendezvousShardingConfig extends BaseShardingConfig {
    strategy: "rendezvous";
    shards: ShardConfig[];
    hashSeed?: string;
}

export interface RangeShardingConfig extends BaseShardingConfig {
    strategy: "range";
    shards: ShardConfig[];
    ranges: ShardRange[];
}

export interface DirectoryShardingConfig extends BaseShardingConfig {
    strategy: "directory";
    shards: ShardConfig[];
    directory: Record<string, string>;
    unknownKeyPolicy?: "throw" | "default";
}

export interface TenantShardingConfig extends BaseShardingConfig {
    strategy: "tenant";
    shards: ShardConfig[];
    tenants: Record<string, string>;
    defaultTenantShardId?: string;
}

export interface CompositeShardingConfig extends BaseShardingConfig {
    strategy: "composite";
    shards: ShardConfig[];
    hashStrategy?: "hash" | "consistent-hash" | "rendezvous" | "weighted";
}

export interface GeographicShardingConfig extends BaseShardingConfig {
    strategy: "geographic";
    shards: ShardConfig[];
    regions: Record<string, string[]>;
    defaultRegion?: string;
    unknownRegionPolicy?: "throw" | "default";
}

export interface WeightedShardingConfig extends BaseShardingConfig {
    strategy: "weighted";
    shards: WeightedShardConfig[];
}

// Backward-compatible legacy flat config (pre-union). Accepted by createDatabase
// alongside the discriminated union. Normalized internally before routing.
export interface LegacyShardingConfig {
    enabled: boolean;
    strategy?: ShardStrategyName;
    shards?: ShardConfig[];
    defaultShardId?: string;
    routing?: {
        shardKey?: string;
        directory?: Record<string, string>;
    };
    [key: string]: unknown;
}

export type ShardingConfig =
    | HashShardingConfig
    | ConsistentHashShardingConfig
    | RendezvousShardingConfig
    | RangeShardingConfig
    | DirectoryShardingConfig
    | TenantShardingConfig
    | CompositeShardingConfig
    | GeographicShardingConfig
    | WeightedShardingConfig;

// ---------------------------------------------------------------------------
// Replication (primary / read replica) types
// ---------------------------------------------------------------------------

/**
 * Read preference / routing mode for replication.
 * - `"primary"`: all reads go to the primary.
 * - `"replica-preferred"`: reads go to an eligible replica, falling back to primary.
 * - `"replica-only"`: reads go only to an eligible replica; error if none available.
 */
export type ReplicationMode = "primary" | "replica-preferred" | "replica-only";

export type ReadPreference = ReplicationMode;

export interface ReplicationEndpointConfig {
    /**
     * Unique, stable identifier for the endpoint within its shard/database.
     * Must be unique among the primary and all replicas.
     */
    id: string;
    /**
     * PostgreSQL connection configuration for this endpoint.
     * Also serves as the per-endpoint pool settings (`max`, `min`,
     * `idleTimeoutMillis`, `connectionTimeoutMillis`, …).
     */
    connection: PoolConfig;
}

export interface ReplicationHealthCheckConfig {
    enabled?: boolean;
    intervalMs?: number;
    timeoutMs?: number;
    /**
     * SQL executed on a replica to measure replication lag in milliseconds.
     * Defaults to `EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000`.
     * Returns NULL when the server is not in recovery (not a replica) or when
     * the value is not yet available, which is reported as unknown lag.
     */
    lagQuery?: string;
}

export interface ShardReplicationConfig {
    enabled: boolean;
    mode: ReplicationMode;
    replicas: ReplicationEndpointConfig[];
    /**
     * Exclude a replica from read routing when its known lag (ms) exceeds this
     * threshold. Unknown lag is not treated as exceeding the threshold.
     */
    lagThresholdMs?: number;
    healthCheck?: ReplicationHealthCheckConfig;
}

export interface ReplicationConfig {
    enabled: boolean;
    mode: ReplicationMode;
    /**
     * Primary endpoint. When omitted (e.g. per-shard replication), the primary
     * is the shard's/main connection with id `"primary"`.
     */
    primary?: ReplicationEndpointConfig;
    replicas: ReplicationEndpointConfig[];
    lagThresholdMs?: number;
    healthCheck?: ReplicationHealthCheckConfig;
}

export interface ReplicationEndpointHealth {
    endpointId: string;
    role: "primary" | "replica";
    status: "up" | "down";
    latencyMs: number;
    /**
     * Known replication lag in milliseconds, or null when unknown.
     * Null is distinct from zero lag — it means the lag has not been measured.
     */
    lagMs: number | null;
    lagUnknown: boolean;
    lastCheckedAt: string;
    consecutiveFailures: number;
    /**
     * Sanitized error message from the last failed check, if any.
     */
    error?: string;
    errorCode?: string;
}

export interface ReplicationHealthResult {
    healthy: boolean;
    checkedAt: string;
    primary: ReplicationEndpointHealth | undefined;
    replicas: ReplicationEndpointHealth[];
    downReplicas: string[];
    upReplicas: string[];
    laggingReplicas: string[];
}

/**
 * Public replication API exposed on `Database`.
 */
export interface ReplicationApi {
    readonly enabled: boolean;
    readonly mode: ReplicationMode;
    readonly primaryEndpointId: string;
    readonly replicaEndpointIds: ReadonlyArray<string>;
    /**
     * Health of every endpoint (primary + replicas).
     */
    health(): Promise<ReplicationHealthResult>;
    /**
     * Health of a single endpoint.
     */
    endpointHealth(endpointId: string): Promise<ReplicationEndpointHealth | undefined>;
    /**
     * The primary pool. Bypasses the replication router — use only for writes
     * or administrative operations that must hit the primary.
     */
    primaryPool: import("pg").Pool;
    /**
     * Read endpoint for a given read preference, or null if none is eligible.
     */
    readEndpoint(readPreference?: ReadPreference): {
        endpointId: string;
        role: "primary" | "replica";
        pool: import("pg").Pool;
    } | null;
    shutdown(): Promise<void>;
    readonly isClosed: boolean;
}

// ---------------------------------------------------------------------------
// Strategy context and interface
// ---------------------------------------------------------------------------

export interface ShardRoutingContext {
    shardKey: string;
    strategy: ShardStrategyName;
    shardIds: string[];
    directory?: Record<string, string>;
    // Strategy-specific options carried through context
    virtualNodes?: number;
    hashSeed?: string;
    ranges?: ShardRange[];
    tenants?: Record<string, string>;
    weights?: Record<string, number>;
    regions?: Record<string, string[]>;
    region?: string;
    compositeKey?: Record<string, string | number | boolean | null>;
    unknownKeyPolicy?: "throw" | "default";
    defaultShardId?: string;
    defaultTenantShardId?: string;
    unknownRegionPolicy?: "throw" | "default";
    defaultRegion?: string;
}

export interface ShardStrategy {
    readonly name: ShardStrategyName;
    readonly version: number;
    validate(config: ShardingConfig, shardIds: string[]): void;
    resolveShard(shardKey: string, context: ShardRoutingContext): string;
}

// Strategy registry entry
export interface ShardStrategyRegistryEntry {
    name: ShardStrategyName;
    strategy: ShardStrategy;
    configShape: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Custom strategy registration
// ---------------------------------------------------------------------------

export interface CustomShardStrategyRegistration {
    name: string;
    strategy: ShardStrategy;
    configShape?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Shard routing diagnostics
// ---------------------------------------------------------------------------

export interface ShardRoutingDiagnostic {
    shardKey: string;
    selectedShardId: string;
    strategy: ShardStrategyName;
    strategyVersion: number;
    routingPath: string[];
    metadata: Record<string, unknown>;
    timestamp: string;
}

export interface ShardRoutingDiagnostics {
    recent: ShardRoutingDiagnostic[];
    totalRoutes: number;
    byStrategy: Record<ShardStrategyName, number>;
    byShard: Record<string, number>;
    clear(): void;
}

// ---------------------------------------------------------------------------
// Rebalance progress tracking
// ---------------------------------------------------------------------------

export interface ShardRebalanceProgress {
    rebalanceId: string;
    sourceShardId: string;
    destinationShardId: string;
    totalKeys: number;
    movedKeys: number;
    failedKeys: number;
    skippedKeys: number;
    percentage: number;
    status: "planning" | "copying" | "validating" | "cutting-over" | "completed" | "failed" | "cancelled";
    startedAt: string;
    updatedAt: string;
    estimatedCompletionAt?: string;
    errors: string[];
}

export interface ShardRebalanceTracker {
    createRebalance(sourceShardId: string, destinationShardId: string, totalKeys: number): ShardRebalanceProgress;
    updateProgress(rebalanceId: string, update: Partial<ShardRebalanceProgress>): ShardRebalanceProgress | undefined;
    getRebalance(rebalanceId: string): ShardRebalanceProgress | undefined;
    listRebalances(): ShardRebalanceProgress[];
    completeRebalance(rebalanceId: string): ShardRebalanceProgress | undefined;
    failRebalance(rebalanceId: string, error: string): ShardRebalanceProgress | undefined;
    cancelRebalance(rebalanceId: string): ShardRebalanceProgress | undefined;
    clearCompleted(): number;
}

export interface ShardInfo {
    id: string;
    connected: boolean;
    pool?: Pool;
    lastCheckedAt?: string;
    lastError?: string;
    consecutiveFailures: number;
}

export interface ShardHealthCheckResult {
    shardId: string;
    connectivity: "healthy" | "unhealthy" | "unknown";
    readiness: "ready" | "not-ready" | "unknown";
    /** Backward-compatible alias: "up" when healthy + ready, otherwise "down". */
    status?: "up" | "down";
    latency: { value: number; unit: "ms" };
    pool?: PoolStats;
    replication?: {
        lagBytes?: number;
        lagSeconds?: number;
        state: string;
    };
    error?: string;
    code?: string;
    checkedAt: string;
    consecutiveFailures: number;
}

export interface ShardHealthResult {
    healthy: boolean;
    checkedAt: string;
    shards: ShardHealthCheckResult[];
    downShards: string[];
    upShards: string[];
}

// ---------------------------------------------------------------------------
// Migration coordination types
// ---------------------------------------------------------------------------

export type MigrationStatusName = "pending" | "applied" | "failed";

export interface MigrationRecord {
    migrationId: string;
    shardId: string;
    status: MigrationStatusName;
    appliedAt?: string;
    error?: string;
}

export interface MigrationStatus {
    migrationId: string;
    shards: Record<string, MigrationStatusName>;
    allApplied: boolean;
    anyFailed: boolean;
}

export interface ShardMigrationTracker {
    /**
     * Initialize durable migration tracking on all known shards.
     * Ensures the `shard_migrations` table exists on each shard and loads
     * persisted migration state into the in-memory cache. Idempotent and
     * safe for concurrent calls. Throws if any shard's tracking table
     * cannot be created or read.
     */
    initialize(): Promise<void>;

    /**
     * Ensure the migration tracking table exists on a single shard.
     * Idempotent. Throws if the shard pool is unavailable.
     */
    initShard(shardId: string): Promise<void>;

    /**
     * Record the result of a migration on a specific shard.
     * Called internally by runMigrationOnAllShards, but can also be called
     * externally to record manually applied migrations.
     */
    recordMigration(shardId: string, migrationId: string, status: MigrationStatusName, error?: string): void;

    /**
     * Get the status of a specific migration across all shards.
     */
    getMigrationStatus(migrationId: string): MigrationStatus | undefined;

    /**
     * Get all migration records for a specific shard.
     */
    getShardMigrationStatus(shardId: string): MigrationRecord[];

    /**
     * Run a migration (arbitrary SQL) against all shards.
     * Returns per-shard results. Continues on individual shard failures.
     */
    runMigrationOnAllShards(migrationId: string, sql: string): Promise<MigrationStatus>;

    /**
     * Register a new shard for migration tracking.
     */
    registerShard(shardId: string): void;

    /**
     * Unregister a shard from migration tracking.
     */
    unregisterShard(shardId: string): void;
}

export interface ShardingApi {
    readonly enabled: boolean;
    readonly strategy: ShardStrategyName | undefined;
    readonly routingVersion: ShardMapVersion;
    readonly shards: ReadonlyArray<ShardInfo>;
    getShard(shardId: string): ShardInfo | undefined;
    getShardForKey(shardKey: string): ShardInfo;
    query<T extends QueryResultRow = QueryResultRow>(
        shardKey: string,
        text: string | object,
        values?: unknown[] | QueryOptions,
        options?: QueryOptions,
    ): Promise<QueryResult<T>>;
    transaction<T>(
        shardKey: string,
        callback: (client: PoolClient) => Promise<T> | T,
        txOptions?: TransactionOptions,
    ): Promise<T>;
    /**
     * Execute a distributed (cross-shard) transaction using the two-phase
     * commit protocol. The callback receives a `DistributedTransactionContext`
     * that allows executing operations on multiple shards. All operations
     * are prepared atomically and committed or aborted together.
     *
     * Requires `max_prepared_transactions > 0` on all participating
     * PostgreSQL servers. Throws `MaxPreparedTransactionsError` if not
     * configured.
     *
     * @param callback - Receives a `DistributedTransactionContext` with an
     *   `execute(shardKey, text, values, options)` method for running SQL on
     *   specific shards within the transaction.
     * @param options - Distributed transaction options.
     * @returns Result with transaction ID, status, and participant statuses.
     * @throws MaxPreparedTransactionsError if prepared transactions are not
     *   enabled on the coordination database.
     * @throws DistributedTransactionError if any participant fails to prepare
     *   or commit.
     */
    distributedTransaction?: <T>(
        callback: (tx: import("./distributed-tx/distributed-transaction-context.js").DistributedTransactionContext) => Promise<T> | T,
        options?: import("./distributed-tx/distributed-transaction-context.js").DistributedTransactionExecuteOptions,
    ) => Promise<{
        transactionId: string;
        status: "committed" | "aborted";
        participants: Record<string, "prepared" | "committed" | "aborted">;
    }>;
    shutdown(options?: { timeout?: number }): Promise<void>;
    readonly isClosed: boolean;
    health(options?: { timeout?: number }): Promise<ShardHealthResult>;
    shardHealth(shardId: string): Promise<ShardHealthCheckResult | undefined>;
    /**
     * Per-shard metrics and routing observability.
     */
    readonly metrics?: {
        snapshot(): {
            byShard: Record<string, {
                queries: number;
                errors: number;
                slowQueries: number;
                retries: number;
                transactions: number;
                rolledBackTransactions: number;
                totalDurationMs: number;
                maxDurationMs: number;
                lastDurationMs: number;
                avgDurationMs: number;
            }>;
            routing: {
                totalRoutes: number;
                defaultRouteUsage: number;
                missingShardKey: number;
                unknownShardKey: number;
                crossShardViolations: number;
                strategyUsage: Record<string, number>;
            };
        };
        forShard(shardId: string): {
            snapshot(): Record<string, number>;
            reset(): void;
        } | undefined;
        reset(): void;
    };
    addShard(config: ShardConfig, hooks?: ReshardingHooks): Promise<void>;
    removeShard(shardId: string, hooks?: ReshardingHooks): Promise<void>;
    readonly migrate?: ShardMigrationTracker;
    /**
     * Execute an arbitrary callback with a dedicated connection on a specific
     * shard. Intended for DDL and administrative operations that must target
     * a particular shard. The callback receives a `PoolClient` bound to that
     * shard's pool.
     */
    executeOnShard(
        shardId: string,
        callback: (client: PoolClient) => Promise<void> | void,
    ): Promise<void>;
    /**
     * Insert rows into multiple shards independently. Each group is routed to
     * its own shard and executed separately. No cross-shard atomicity is
     * claimed; results and failures are reported per shard.
     */
    batchInsertMany(groups: Array<{
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
    }>;
    /**
     * Execute a query on multiple shards independently and merge results.
     * Does not claim global snapshot consistency across independent shard
     * transactions. Partial results are returned when explicitly requested.
     */
    fanOut<T extends QueryResultRow = QueryResultRow>({
        query,
        values,
        shardIds,
        concurrency,
        merge,
        onShardError,
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
    }>;
    /**
     * Execute a query on all shards and merge results using the specified
     * strategy. Convenience wrapper around `fanOut`.
     */
    queryAllShards<T extends QueryResultRow = QueryResultRow>({
        query,
        values,
        concurrency,
        merge,
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
    }>;
    /**
     * Topology management layer. Provides read-only inspection and opt-in
     * rebalancing planning. Does not automatically move data or change
     * routing.
     */
    readonly topology: {
        /** Return a snapshot of the current shard topology. */
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
        };
        /** Validate topology consistency. Returns any issues found. */
        validate(): { valid: boolean; issues: string[] };
        /** Produce a rebalancing proposal (does not execute). */
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
        };
    };
    /**
     * Distributed transaction coordinator. Available only when
     * `distributedTransaction` is configured and initialized.
     */
    readonly distributedTx?: import("./distributed-tx/distributed-transaction-coordinator.js").DistributedTransactionCoordinator;

    /**
     * Routing diagnostics — recent shard routing decisions, totals, and
     * per-strategy / per-shard counts. Useful for debugging routing
     * distribution and detecting hot shards.
     */
    getRoutingDiagnostics(): ShardRoutingDiagnostics;

    /**
     * Rebalance progress tracking primitives — create, update, and query
     * in-flight shard rebalance operations.
     */
    readonly rebalanceTracker: ShardRebalanceTracker;
}

export interface DatabaseConfig extends PoolConfig {
    connectionString?: string;
    host?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;
    max?: number;
    min?: number;
    idleTimeoutMillis?: number;
    connectionTimeoutMillis?: number;
    statement_timeout?: number;
    query_timeout?: number;
    keepAlive?: boolean;
    application_name?: string;
    ssl?: SslConfig;
    idleTransactionTimeout?: IdleTransactionTimeout;
}

export interface RetryOptions {
    retries?: number;
    maxRetries?: number;
    baseDelay?: number;
    maxDelay?: number;
    factor?: number;
    jitter?: number;
    retryOnTimeout?: boolean;
    signal?: AbortSignal;
    shouldRetry?(error: unknown): boolean;
    onRetry?(info: RetryInfo): void;
    sleep?(ms: number): Promise<void>;
}

export interface RetryInfo {
    error: NormalizedError;
    attempt: number;
    delay: number;
    retries: number;
}

export interface NormalizedError {
    name: string;
    message: string;
    code?: string;
    constraint?: string;
    table?: string;
    column?: string;
    detail?: string;
    hint?: string;
    transient?: boolean;
    cause?: unknown;
}

export interface QueryOptions {
    timeout?: number;
    retry?: boolean | number | RetryOptions;
    destroyOnError?: boolean;
    cancel?: boolean;
    signal?: AbortSignal;
    values?: unknown[];
    /**
     * Read preference for replication routing.
     * - `"primary"` (default): always read from the primary endpoint.
     * - `"replica-preferred"`: read from an eligible replica, fall back to primary.
     * - `"replica-only"`: read only from an eligible replica; throw if none is available.
     * Writes are always routed to the primary regardless of this setting.
     */
    readPreference?: ReadPreference;
    [key: string]: unknown;
}

export interface TransactionOptions {
    isolation?: "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE" | string;
    readOnly?: boolean;
    deferrable?: boolean;
    timeout?: number;
    retry?: boolean | number | RetryOptions;
    /**
     * Shard key used to route the transaction to a single shard.
     * Transactions are pinned to exactly one shard for their entire lifetime.
     * Cross-shard transactions are not supported: PostgreSQL transactions
     * cannot atomically commit across independent database servers, and this
     * package does not implement two-phase commit or a distributed transaction
     * coordinator. Provide exactly one shardKey; there is no way to span a
     * transaction across multiple shards through this API.
     */
    shardKey?: string;
}

export interface InstrumentationOptions {
    logQueries?: boolean;
    logParameters?: boolean;
    slowQueryMs?: number;
    onQuery?(event: {
        sql?: string;
        parameters?: unknown;
        durationMs: number;
        rowCount?: number;
        success?: boolean;
    }): void;
    onError?(event: { error: unknown; sql?: string; durationMs: number }): void;
    onRetry?(info: RetryInfo): void;
}

export type QueryRunner = (text: string | QueryConfig, values?: unknown[], options?: QueryOptions) => Promise<QueryResult>;

export interface SqlFragment {
    readonly text: string;
    readonly values: unknown[];
}

export type QueryFragment = SqlFragment;

export type QueryLike = string | SqlFragment | SqlFragment[] | Record<string, unknown> | unknown[];

export type SqlFn = ((strings: TemplateStringsArray, ...values: QueryLike[]) => SqlFragment) & {
    raw(text: string): SqlFragment;
    identifier(name: string): SqlFragment;
    quoteIdent(name: string): string;
    quoteIdentList(names: string | string[]): string;
    isFragment(value: unknown): value is SqlFragment;
    join: JoinFn;
    rowNumber(partitionBy?: string[], orderBy?: string[]): SqlFragment;
    rank(partitionBy?: string[], orderBy?: string[]): SqlFragment;
    denseRank(partitionBy?: string[], orderBy?: string[]): SqlFragment;
    window(name: string, options?: { partitionBy?: string[]; orderBy?: string[] }): SqlFragment;
    jsonGetText(column: string, path: string): SqlFragment;
    jsonGet(column: string, path: string): SqlFragment;
    jsonContains(column: string, value: unknown): SqlFragment;
    jsonContained(column: string, value: unknown): SqlFragment;
    jsonArrayLength(column: string): SqlFragment;
    jsonObjectKeys(column: string): SqlFragment;
    jsonPath(column: string, path: string): SqlFragment;
    quoteString(value: string): string;
    caseWhen(conditions: Array<{ when: string | SqlFragment; then: unknown }>, elseValue?: unknown): SqlFragment;
    countDistinct(column: string): SqlFragment;
    stringAgg(column: string, delimiter: string): SqlFragment;
    arrayAgg(column: string): SqlFragment;
    jsonAgg(column: string): SqlFragment;
    jsonObjectAgg(key: string, value: string): SqlFragment;
};

export type JoinFn = (values: QueryLike | QueryLike[], separator?: string) => SqlFragment;

export interface TransactionHooks {
    metrics?: {
        recordTransaction?: (rolledBack: boolean) => void;
    };
    recordRetry?: (info: RetryInfo) => void;
    onError?: (event: { error: unknown; sql?: string; durationMs: number }) => void;
    config?: InstrumentationOptions;
}

export interface CreateDatabaseOptions extends DatabaseConfig {
    logger?: Logger;
    instrumentation?: InstrumentationOptions;
    hooks?: Hooks;
    retry?: boolean | number | RetryOptions;
    queryTimeout?: number;
    timeout?: number;
    connectTimeout?: number;
    sharding?: ShardingConfig;
    /**
     * Top-level replication configuration for non-sharded databases.
     * When sharding is enabled, use `ShardConfig.replication` instead.
     */
    replication?: ReplicationConfig;
}

export interface PoolStats {
    total: number;
    idle: number;
    waiting: number;
}

export interface HealthCheckResult {
    status: "up" | "down";
    latency: { value: number; unit: "ms" };
    pool?: PoolStats;
    error?: string;
    code?: string;
}

export interface HealthResult {
    healthy: boolean;
    checkedAt: string;
    status: "up" | "down";
    latency: { value: number; unit: "ms" };
    pool?: PoolStats;
    error: string | null;
    errorCode: string | null;
}

export interface PreparedStatement {
    name: string;
    text: string;
    execute(values?: unknown[], options?: QueryOptions): Promise<QueryResult>;
}

export interface CursorHandle {
    read(count?: number): Promise<QueryResultRow[]>;
    close(error?: unknown): Promise<void>;
    /** Cancel and close the cursor, releasing the connection. */
    cancel(): Promise<void>;
    [Symbol.asyncIterator](): AsyncIterator<QueryResultRow>;
}

export interface InsertManyOptions {
    columns?: string[];
    returning?: string | string[];
    chunkSize?: number;
    shardKey?: string;
    shardId?: string;
    onConflict?:
        | true
        | "nothing"
        | {
              columns?: string[];
              constraint?: string;
              update?: string[];
              do?: "nothing" | string;
              action?: "nothing" | string;
              id?: string;
          };
    transaction?: boolean;
    client?: Pick<PoolClient, "query">;
    queryOptions?: QueryOptions;
    retry?: boolean | number | RetryOptions;
    signal?: AbortSignal;
    timeout?: number;
    onRowError?: (row: Record<string, unknown>, error: unknown, rowIndex: number) => void;
    savepoint?: boolean;
    onProgress?: (progress: BatchProgressEvent) => void;
    adaptive?: boolean;
}

export interface BatchProgressEvent {
    operation: "insert" | "update" | "delete" | "select" | "executeBatch";
    chunkIndex: number;
    totalChunks: number;
    rowsProcessed: number;
    rowsSucceeded: number;
    rowsFailed: number;
    durationMs: number;
}

export interface BatchRowError {
    rowIndex: number;
    row: Record<string, unknown>;
    error: unknown;
}

export interface BatchStats {
    chunks: number;
    rowsProcessed: number;
    rowsSucceeded: number;
    rowsFailed: number;
    durationMs: number;
    errors: BatchRowError[];
}

export interface BatchInsertResult {
    rows: unknown[];
    rowCount: number;
    stats: BatchStats;
}

export interface BatchUpdateDeleteResult {
    rows: unknown[];
    rowCount: number;
    stats: BatchStats;
}

export interface BatchSelectResult {
    rows: unknown[];
    rowCount: number;
    stats: BatchStats;
}

export interface BatchApi {
    insertMany(table: string, rows: Record<string, unknown>[], options?: InsertManyOptions): Promise<BatchInsertResult>;
    selectMany(
        builderOrSql: any,
        options?: {
            chunkSize?: number;
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            client?: any;
            onChunk?: (rows: unknown[]) => Promise<void> | void;
            keysetColumn?: string;
            lastKeysetValue?: unknown;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        }
    ): Promise<BatchSelectResult>;
    updateMany(
        table: string,
        values: Record<string, unknown>,
        whereConditions: { column: string; operator: string; values: unknown[] }[],
        options?: {
            chunkSize?: number;
            returning?: string[];
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            client?: any;
            transaction?: boolean;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onRowError?: (row: Record<string, unknown>, error: unknown, rowIndex: number) => void;
            savepoint?: boolean;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        }
    ): Promise<BatchUpdateDeleteResult>;
    deleteMany(
        table: string,
        whereConditions: { column: string; operator: string; values: unknown[] }[],
        options?: {
            chunkSize?: number;
            returning?: string[];
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            client?: any;
            transaction?: boolean;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onRowError?: (row: Record<string, unknown>, error: unknown, rowIndex: number) => void;
            savepoint?: boolean;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        }
    ): Promise<BatchUpdateDeleteResult>;
    executeBatch(
        builders: any[],
        options?: {
            chunkSize?: number;
            transaction?: boolean;
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        }
    ): Promise<QueryResult[]>;
}

export interface QueryBuilder {
    // Core CRUD
    select(...columns: Array<string | string[] | SqlFragment>): QueryBuilder;
    distinct(value?: boolean): QueryBuilder;
    insert(values: Record<string, unknown> | Record<string, unknown>[]): QueryBuilder;
    values(values: Record<string, unknown> | Record<string, unknown>[]): QueryBuilder;
    update(values: Record<string, unknown>): QueryBuilder;
    delete(): QueryBuilder;
    returning(...columns: Array<string | string[]>): QueryBuilder;
    
    // WHERE conditions
    where(column: string | Record<string, unknown> | SqlFragment, operator?: unknown, value?: unknown): QueryBuilder;
    orWhere(column: string, operator?: unknown, value?: unknown): QueryBuilder;
    whereNull(column: string): QueryBuilder;
    whereNotNull(column: string): QueryBuilder;
    whereIn(column: string, values: unknown[]): QueryBuilder;
    whereNotIn(column: string, values: unknown[]): QueryBuilder;
    whereRaw(fragment: SqlFragment): QueryBuilder;
    
    // JSON operators
    whereJson(column: string, operator: string, value: unknown): QueryBuilder;
    whereJsonContains(column: string, value: unknown): QueryBuilder;
    whereJsonContained(column: string, value: unknown): QueryBuilder;
    
    // Subqueries
    from(subquery: QueryBuilder | QueryFragment | string, alias?: string): QueryBuilder;
    selectSubquery(subquery: QueryBuilder | QueryFragment, alias: string): QueryBuilder;
    whereExists(subquery: QueryBuilder | QueryFragment): QueryBuilder;
    whereNotExists(subquery: QueryBuilder | QueryFragment): QueryBuilder;
    whereInSubquery(column: string, subquery: QueryBuilder | QueryFragment): QueryBuilder;
    whereNotInSubquery(column: string, subquery: QueryBuilder | QueryFragment): QueryBuilder;
    
    // CTEs (WITH clauses)
    with(name: string, query: QueryBuilder | QueryFragment | string, columns?: string[], materialized?: boolean): QueryBuilder;
    withRecursive(name: string, query: QueryBuilder | QueryFragment | string, columns?: string[]): QueryBuilder;
    
    // Window functions
    window(name: string, definition: { partitionBy?: string[]; orderBy?: Array<{ column: string; direction?: string }>; frame?: { type?: "ROWS" | "RANGE"; start?: string | number; end?: string | number } }): QueryBuilder;
    
    // Complex joins
    join(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    innerJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    leftJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    rightJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    fullJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    fullOuterJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    leftOuterJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    rightOuterJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;
    crossJoin(table: string | QueryBuilder, alias?: string): QueryBuilder;
    
    // Nested WHERE groups
    whereGroup(callback: (builder: QueryBuilder) => QueryBuilder): QueryBuilder;
    orWhereGroup(callback: (builder: QueryBuilder) => QueryBuilder): QueryBuilder;
    
    // Query combination
    union(query: QueryBuilder): QueryBuilder;
    unionAll(query: QueryBuilder): QueryBuilder;
    intersect(query: QueryBuilder): QueryBuilder;
    intersectAll(query: QueryBuilder): QueryBuilder;
    except(query: QueryBuilder): QueryBuilder;
    exceptAll(query: QueryBuilder): QueryBuilder;
    
    // CASE expressions
    selectCase(alias: string, callback: (builder: import("./query/builder.js").CaseBuilder) => import("./query/builder.js").CaseBuilder): QueryBuilder;
    
    // DISTINCT ON
    distinctOn(columns: string[]): QueryBuilder;
    
    // Row locking
    forUpdate(): QueryBuilder;
    forNoKeyUpdate(): QueryBuilder;
    skipLocked(): QueryBuilder;
    
    // Grouping
    groupBy(...columns: Array<string | string[]>): QueryBuilder;
    having(column: string, operator: string, value?: unknown): QueryBuilder;
    
    // Ordering
    orderBy(column: string | SqlFragment, direction?: string): QueryBuilder;
    
    // Limiting
    limit(value: number): QueryBuilder;
    offset(value: number): QueryBuilder;
    onConflict(options: InsertManyOptions["onConflict"]): QueryBuilder;
    
    // Query logging and timeout
    withQueryContext(context: Record<string, unknown>): QueryBuilder;
    getQueryContext(): Record<string, unknown>;
    timeout(ms: number): QueryBuilder;

    // Execution plan inspection
    explain(options?: {
        analyze?: boolean;
        verbose?: boolean;
        buffers?: boolean;
        costs?: boolean;
        timing?: boolean;
        summary?: boolean;
        settings?: boolean;
        format?: "text" | "json" | "xml" | "yaml";
    }): QueryBuilder;
    
    // Sharding
    withShard(shardKey: string): QueryBuilder;
    withShardId(shardId: string): QueryBuilder;

    // Keyset pagination
    keysetPagination(lastValues: Record<string, unknown>): QueryBuilder;

    // Execution
    toSQL(): { text: string; values: unknown[] };
    execute(options?: QueryOptions): Promise<QueryResult>;
    first(options?: QueryOptions): Promise<QueryResultRow | null>;
    then<TResult1 = QueryResult, TResult2 = never>(
        onfulfilled?: ((value: QueryResult) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ): Promise<TResult1 | TResult2>;
    catch<TResult>(onRejected?: (reason: unknown) => TResult | PromiseLike<TResult>): Promise<TResult | QueryResult>;
}

export interface ModelFindOptions extends QueryOptions {
    columns?: string[];
    orderBy?: string | Array<[string, string?]>;
    direction?: string;
    limit?: number;
    offset?: number;
    client?: Pick<PoolClient, "query">;
    returning?: string[];
    /**
     * When true and no `client` is provided, the operation is wrapped in an
     * automatic transaction (BEGIN/COMMIT/ROLLBACK) using a client acquired
     * from the query runner. When false (default), the statement runs as a
     * single auto-committed query.
     */
    atomic?: boolean;
    /**
     * Override the model-level default scope for this call.
     * Pass `false` to disable the default scope entirely.
     * Pass a WHERE object to merge with / override the model default scope.
     */
    defaultScope?: Record<string, unknown> | boolean;
    /**
     * Optional cache hooks for query result caching.
     * `get(key)` returns cached rows or undefined/None to miss.
     * `set(key, rows)` stores rows for subsequent calls.
     * Works with find, findOne, findById, count, exists.
     */
    cache?: {
        get(key: string): unknown[] | null | undefined | Promise<unknown[] | null | undefined>;
        set(key: string, rows: unknown[]): void | Promise<void>;
    };
}

/**
 * Extended find options with support for complex WHERE conditions
 */
export interface ExtendedModelFindOptions extends ModelFindOptions {
    /**
     * Complex WHERE conditions. Can be:
     * - Simple object: { status: 'active' }
     * - Array of conditions: [{ status: 'active' }, { OR: [{ role: 'admin' }] }]
     * - QueryBuilder function: (q) => q.where(...)
     */
    where?: Record<string, unknown> | Array<Record<string, unknown>> | ((q: QueryBuilder) => QueryBuilder);
    /**
     * Pre-built QueryBuilder to use instead of creating a new one.
     * Useful for queries requiring JOINs, subqueries, CTEs, etc.
     */
    queryBuilder?: QueryBuilder;
    /**
     * Include soft-deleted records (when softDelete is enabled)
     */
    withDeleted?: boolean;
    /**
     * Force permanent deletion (bypass soft delete when enabled)
     */
    forceDelete?: boolean;
}

/**
 * Hook types for Model lifecycle events
 */
export interface ModelHooks<T extends QueryResultRow = QueryResultRow> {
    beforeCreate?: (data: Partial<T>) => Partial<T> | Promise<Partial<T>>;
    afterCreate?: (result: T | null) => T | Promise<T>;
    beforeUpdate?: (data: Partial<T>, where: Record<string, unknown>) => Partial<T> | Promise<Partial<T>>;
    afterUpdate?: (result: T | null, where: Record<string, unknown>) => T | Promise<T>;
    beforeDelete?: (where: Record<string, unknown>) => Record<string, unknown> | Promise<Record<string, unknown>>;
    afterDelete?: (result: T | null, where: Record<string, unknown>) => T | Promise<T>;
}

/**
 * Options for creating a Model
 */
export interface ModelOptions<T extends QueryResultRow = QueryResultRow> {
    /**
     * Query function for database operations
     */
    query?: QueryRunner;
    /**
     * Table name
     */
    table: string;
    /**
     * Custom ID column name. Default: 'id'
     */
    idColumn?: string;
    /**
     * Logger
     */
    logger?: Logger;
    /**
     * Shard key for routing
     */
    shardKey?: string;
    /**
     * Shard ID for routing
     */
    shardId?: string;
    /**
     * Enable soft deletes. When enabled, delete operations set deleted_at timestamp
     * instead of actually deleting. Use forceDelete to permanently delete.
     */
    softDelete?: boolean;
    /**
     * Custom field name for soft delete timestamp. Default: 'deleted_at'
     */
    softDeleteField?: string;
    /**
     * Enable automatic timestamps. When enabled, created_at and updated_at
     * are automatically set on insert and update.
     */
    timestamps?: boolean;
    /**
     * Custom field name for created_at. Default: 'created_at'
     */
    createdAtField?: string;
    /**
     * Custom field name for updated_at. Default: 'updated_at'
     */
    updatedAtField?: string;
    /**
     * Validation function to run before create/update. Throw an error to reject.
     */
    validate?: (data: Record<string, unknown>, operation: 'create' | 'update') => Record<string, unknown> | Promise<Record<string, unknown>>;
    /**
     * Lifecycle hooks
     */
    hooks?: ModelHooks<T>;
    /**
     * Relationship definitions for eager loading
     */
    relationships?: {
        [key: string]: {
            type: 'hasMany' | 'belongsTo' | 'hasOne' | 'manyToMany';
            model: string;
            foreignKey?: string;
            through?: string;
            throughForeignKey?: string;
            throughOtherKey?: string;
        };
    };
    /**
     * Enable automatic type coercion for insert/update data.
     * When true, values are coerced based on coerceTypes or heuristic detection.
     * Default: false (no coercion).
     */
    coerce?: boolean;
    /**
     * Explicit column-to-type mapping for type coercion.
     * Supported types: 'string', 'number', 'boolean', 'date'.
     * Only applies when coerce is true.
     */
    coerceTypes?: Record<string, 'string' | 'number' | 'boolean' | 'date'>;
    /**
     * Default scope applied to all queries (find, findOne, findById, count, exists).
     * Can be a static WHERE object or a function returning one. Useful for
     * multi-tenancy (e.g. { tenant_id: currentTenant }) or soft-delete-like filters.
     * Can be overridden per-call via options.defaultScope: false.
     */
    defaultScope?: Record<string, unknown> | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
    /**
     * Optimistic locking using a version column.
     * When configured, update/updateById:
     *  - Include the version column in the WHERE clause matching the current value
     *  - Increment the version column on each update
     *  - Throw if no rows matched (version conflict)
     */
    optimisticLocking?: { column: string };
    /**
     * Enable automatic schema introspection from information_schema.
     * When true, the model can introspect column names, types, nullability,
     * and defaults from the database. When an object, `schema` specifies the
     * PostgreSQL schema to introspect (default: 'public') and
     * `autoCoerceTypes` automatically populates coerceTypes from discovered
     * column types.
     */
    introspectSchema?: boolean | { schema?: string; autoCoerceTypes?: boolean };
}

export interface ModelSchemaColumn {
    name: string;
    dataType: string;
    isNullable: "YES" | "NO";
    columnDefault: string | null;
    characterMaximumLength: number | null;
    numericPrecision: number | null;
    numericScale: number | null;
    ordinalPosition: number;
}

export interface ModelSchema {
    table: string;
    schema: string;
    columns: ModelSchemaColumn[];
    columnMap: Record<string, ModelSchemaColumn>;
}

export interface Model {
    table: string;
    idColumn: string;
    query(): QueryBuilder;
    builder(): QueryBuilder;
    select(): QueryBuilder;
    
    /**
     * Find records with optional complex WHERE conditions
     * @param where - Simple object, array of conditions, or QueryBuilder function
     * @param options - Find options including withDeleted and forceDelete
     */
    find(where?: Record<string, unknown> | Array<Record<string, unknown>>, options?: ExtendedModelFindOptions): Promise<QueryResultRow[]>;
    
    /**
     * Find one record with optional complex WHERE conditions
     */
    findOne(where?: Record<string, unknown> | Array<Record<string, unknown>>, options?: ExtendedModelFindOptions): Promise<QueryResultRow | null>;
    
    /**
     * Find by primary key
     */
    findById(id: unknown, options?: ExtendedModelFindOptions): Promise<QueryResultRow | null>;
    
    /**
     * Insert a record with automatic timestamps, validation, and hooks
     */
    insert(data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    
    /**
     * Insert multiple records with optional atomic transaction
     * @param rows - Array of records to insert
     * @param options - Options including atomic (default: true) and client for transaction
     */
    insertMany(rows: Record<string, unknown>[], options?: ModelFindOptions & { atomic?: boolean }): Promise<QueryResultRow[]>;
    
    /**
     * Bulk upsert: insert rows, and on conflict (matching conflictColumns),
     * update the specified columns using PostgreSQL ON CONFLICT DO UPDATE.
     * @param rows - Array of records to upsert
     * @param options.conflictColumns - Unique/constraint columns that trigger the conflict
     * @param options.updateColumns - Columns to update on conflict (defaults to all non-conflict columns)
     * @param options.returning - Columns to return (defaults to ["*"])
     * @param options.atomic - Wrap in a transaction (default: true)
     * @param options.client - Existing transaction client to join
     */
    bulkUpsert(rows: Record<string, unknown>[], options?: ModelFindOptions & { conflictColumns?: string[]; updateColumns?: string[]; atomic?: boolean }): Promise<QueryResultRow[]>;
    
    /**
     * Update records with automatic timestamps, validation, and hooks
     */
    update(where: Record<string, unknown>, data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    
    /**
     * Update by primary key
     */
    updateById(id: unknown, data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    
    /**
     * Delete records (soft delete if enabled)
     */
    delete(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    
    /**
     * Delete by primary key (soft delete if enabled)
     */
    deleteById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    
    /**
     * Alias for delete
     */
    remove(where: Record<string, unknown>, options?: ModelFindOptions & { forceDelete?: boolean }): Promise<QueryResultRow[]>;
    
    /**
     * Alias for deleteById
     */
    removeById(id: unknown, options?: ModelFindOptions & { forceDelete?: boolean }): Promise<QueryResultRow | null>;
    
    /**
     * Count records with optional complex WHERE conditions
     */
    count(where?: Record<string, unknown> | Array<Record<string, unknown>>, options?: ExtendedModelFindOptions): Promise<number>;
    
    /**
     * Check if records exist with optional complex WHERE conditions
     */
    exists(where?: Record<string, unknown> | Array<Record<string, unknown>>, options?: ExtendedModelFindOptions): Promise<boolean>;
    
    // ========== NEW METHODS ==========
    
    /**
     * Find records including soft-deleted ones (when softDelete is enabled)
     */
    findWithDeleted(where?: Record<string, unknown> | Array<Record<string, unknown>>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    
    /**
     * Default scope configuration
     */
    defaultScope?: Record<string, unknown> | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
    /**
     * Optimistic locking configuration
     */
    optimisticLocking?: { column: string };
    
    /**
     * Serialize a model row to a plain object (e.g. convert Dates to ISO strings
     * for JSON serialization).
     */
    serialize(row: QueryResultRow): Record<string, unknown>;
    
    /**
     * Hydrate a plain object into a model-compatible format (e.g. convert ISO
     * strings back to Date objects).
     */
    hydrate(data: Record<string, unknown>): Record<string, unknown>;
    
    /**
     * Permanently delete records (bypasses soft delete)
     */
    forceDelete(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    
    /**
     * Permanently delete by primary key (bypasses soft delete)
     */
    forceDeleteById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    
    /**
     * Restore soft-deleted records
     */
    restore(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    
    /**
     * Restore soft-deleted record by primary key
     */
    restoreById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    
    // ========== CONFIGURATION GETTERS ==========
    
    /**
     * Check if soft delete is enabled
     */
    readonly softDeleteEnabled: boolean;
    
    /**
     * Check if timestamps are enabled
     */
    readonly timestampsEnabled: boolean;
    
    /**
     * Get the hooks configuration
     */
    readonly hooks?: ModelHooks<QueryResultRow>;
    
    /**
     * Get the validation function
     */
    readonly validate?: (data: Record<string, unknown>, operation: 'create' | 'update') => Record<string, unknown> | Promise<Record<string, unknown>>;

    // ========== SCHEMA INTROSPECTION ==========

    /**
     * Introspect the database schema for this model's table from
     * information_schema.columns. Discovers column names, types, nullability,
     * defaults, and character maximum length.
     */
    introspect(options?: { schema?: string; force?: boolean }): Promise<ModelSchema>;

    /**
     * Get the last introspected schema, or null if not yet introspected.
     */
    getSchema(): ModelSchema | null;
    
    // ========== RELATIONSHIP METHODS ==========
    
    /**
     * Find records and include related data (eager loading)
     * @param where - WHERE conditions
     * @param options - Find options
     * @param include - Relationships to include
     */
    findWithIncludes(
        where?: Record<string, unknown> | Array<Record<string, unknown>>,
        options?: ExtendedModelFindOptions,
        include?: string[]
    ): Promise<QueryResultRow[]>;
    
    /**
     * Find one record and include related data (eager loading)
     * @param where - WHERE conditions
     * @param options - Find options
     * @param include - Relationships to include
     */
    findOneWithIncludes(
        where?: Record<string, unknown> | Array<Record<string, unknown>>,
        options?: ExtendedModelFindOptions,
        include?: string[]
    ): Promise<QueryResultRow | null>;
    
    /**
     * Find by primary key and include related data (eager loading)
     * @param id - Primary key value
     * @param options - Find options
     * @param include - Relationships to include
     */
    findByIdWithIncludes(
        id: unknown,
        options?: ExtendedModelFindOptions,
        include?: string[]
    ): Promise<QueryResultRow | null>;
    
    /**
     * Delete records with cascade to related tables
     * @param where - WHERE conditions
     * @param options - Options including cascade relationships
     */
    deleteWithCascade(
        where: Record<string, unknown>,
        options?: ModelFindOptions & { cascade?: string[] }
    ): Promise<QueryResultRow[]>;
    
    /**
     * Delete by primary key with cascade to related tables
     * @param id - Primary key value
     * @param options - Options including cascade relationships
     */
    deleteByIdWithCascade(
        id: unknown,
        options?: ModelFindOptions & { cascade?: string[] }
    ): Promise<QueryResultRow | null>;
    
    /**
     * Load related data for records (lazy relationship loading).
     * Attaches related records to each parent record under the relationship name key.
     * @param records - Array of parent records to load relationships for
     * @param relationshipName - Name of the relationship as defined in model options
     * @param getModel - Optional function to resolve a model name to a model instance.
     *                   If omitted, the model's query function must have a .model() method.
     */
    loadRelated(
        records: QueryResultRow[],
        relationshipName: string,
        getModel?: (modelName: string) => unknown
    ): Promise<QueryResultRow[]>;
    
    /**
     * Get the relationships configuration
     */
    readonly relationships?: {
        [key: string]: {
            type: 'hasMany' | 'belongsTo' | 'hasOne' | 'manyToMany';
            model: string;
            foreignKey?: string;
            through?: string;
            throughForeignKey?: string;
            throughOtherKey?: string;
        };
    };

    /**
     * Check if type coercion is enabled
     */
    readonly coerceEnabled: boolean;

    // ========== JOIN METHODS ==========

    /**
     * Join another table (delegates to QueryBuilder.join).
     * Returns a QueryBuilder that can be passed via { queryBuilder } to find/findOne/count/exists.
     */
    join(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;

    /**
     * INNER JOIN another table.
     */
    innerJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;

    /**
     * LEFT JOIN another table.
     */
    leftJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;

    /**
     * RIGHT JOIN another table.
     */
    rightJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;

    /**
     * FULL JOIN another table.
     */
    fullJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): QueryBuilder;

    /**
     * CROSS JOIN another table.
     */
    crossJoin(table: string | QueryBuilder, alias?: string): QueryBuilder;
}

export type ModelFactory = (table: string, options?: ModelOptions) => Model;

export interface MigrationDefinition {
    id?: string;
    name?: string;
    up: string | ((context: { client: PoolClient; query: MigrationQueryFn; logger?: Logger }) => Promise<unknown>);
    down?: string | ((context: { client: PoolClient; query: MigrationQueryFn; logger?: Logger }) => Promise<unknown>);
    dependsOn?: string[];
    mode?: MigrationMode;
    retry?: MigrationRetryOptions;
    sql?: string;
    source?: string;
    requiresApproval?: boolean;
    backupBefore?: boolean;
}

export type MigrationRunStatus = "pending" | "applied" | "failed" | "skipped";

export type MigrationQueryFn = (text: string, values?: unknown[]) => Promise<unknown>;

export type MigrationMode = "transactional" | "non-transactional";

export type RollbackRisk = "safe" | "potentially-destructive" | "irreversible";
export interface ApprovalProvider {
    requiresApproval?(migration: MigrationDefinition, context?: { runnerId?: string; host?: string; targetDatabase?: string }): boolean;
    approve?(migration: MigrationDefinition, context?: { runnerId?: string; host?: string; targetDatabase?: string }): Promise<{ approved: boolean; approvedBy?: string; approvedAt?: unknown; reason?: string }>;
}

export interface BackupProvider {
    backup(targetDatabase: string, context: { migrationId?: string; reason: string; runnerId?: string }): Promise<{ backupId: string; backupLocation?: string; createdAt?: unknown }>;
    restore?(backupId: string, targetDatabase?: string): Promise<void>;
    exists?(backupId: string): Promise<boolean>;
}

export type ApprovalStatus = "approved" | "rejected" | "bypassed" | "not_required";

export interface MigrationApprovalResult {
    migrationId: string;
    approved: boolean;
    approvedBy?: string;
    approvedAt?: unknown;
    reason?: string;
    status: ApprovalStatus;
}

export interface BaselineRecord {
    id: string;
    name: string;
    appliedMigrationIds: string[];
    checksum: string;
    createdAt: unknown;
    createdBy?: string;
    verified: boolean;
    schemaVersion?: string;
}

export interface BaselineOptions {
    name: string;
    migrations: MigrationDefinition[];
    verify?: boolean;
    createdBy?: string;
    schemaVersion?: string;
}

export interface BaselineResult {
    baseline: BaselineRecord;
    appliedCount: number;
    skipped: string[];
}

export interface SquashOptions {
    name?: string;
    migrations?: MigrationDefinition[];
    keepHistorical?: boolean;
    dryRun?: boolean;
}

export interface SquashResult {
    baseline: BaselineRecord;
    squashed: string[];
    skipped: string[];
    dryRun: boolean;
}

export interface MigrationProgressOptions {
    source?: unknown;
    migrations?: MigrationDefinition[];
    directory?: string;
    onProgress?: (progress: MigrationProgressEvent) => void;
}

export interface MigrationProgressEvent {
    migrationId: string;
    status: "pending" | "running" | "completed" | "failed" | "skipped";
    startedAt?: unknown;
    completedAt?: unknown;
    durationMs?: number;
    error?: string;
}

export interface MigrationProgressResult {
    total: number;
    completed: number;
    failed: number;
    skipped: number;
    events: MigrationProgressEvent[];
    durationMs: number;
}

export interface DiscoverDependenciesOptions {
    source?: unknown;
    migrations?: MigrationDefinition[];
    directory?: string;
}

export interface DiscoverDependenciesResult {
    migrations: MigrationDefinition[];
    dependencyGraph: Record<string, string[]>;
    cycles: string[][];
    roots: string[];
    leaves: string[];
    ordered: string[];
}

export interface RepairMigrationOptions {
    migrationId: string;
    action: "mark-applied" | "mark-failed" | "mark-pending" | "retry";
    source?: unknown;
    migrations?: MigrationDefinition[];
    directory?: string;
    checksum?: string;
}

export interface RepairMigrationResult {
    migrationId: string;
    previousStatus: string;
    newStatus: string;
    action: string;
    checksum?: string;
}

export interface MigrationBackupRecord {
    id: string;
    migrationId: string;
    targetDatabase?: string;
    backupId: string;
    backupLocation?: string;
    createdAt: unknown;
    successful: boolean;
}



export interface RollbackAssessment {
    migrationId: string;
    risk: RollbackRisk;
    reason: string;
    hasDown: boolean;
    autoGenerated?: boolean;
    requiresConfirmation: boolean;
}

export interface RollbackValidationResult {
    id: string;
    name: string;
    valid: boolean;
    error?: string;
    risk: RollbackRisk;
}

export interface RollbackDependencyCheck {
    safe: boolean;
    blocking: Array<{ migrationId: string; dependentOn: string[] }>;
}

export interface MigrationAuditEvent {
    id: string;
    migrationId: string;
    target: string;
    checksum: string;
    startedAt: unknown;
    completedAt: unknown;
    durationMs: number;
    status: "applied" | "failed" | "rolled_back" | "skipped";
    attempt: number;
    mode: MigrationMode;
    error?: string;
    rollbackRisk?: RollbackRisk;
    applicationVersion?: string;
    runnerId?: string;
    host?: string;
}

export interface MigrationTarget {
    id: string;
    pool: Pool;
    database?: string;
    host?: string;
}

export interface TargetDrift {
    targetId: string;
    targetDatabase?: string;
    driftType: "missing_migration" | "extra_migration" | "checksum_mismatch" | "status_mismatch";
    migrationId: string;
    expected?: MigrationLogRecord;
    actual?: MigrationLogRecord;
}

export interface InterruptionRecord {
    id: string;
    migrationId: string;
    targetDatabase?: string;
    checksum: string;
    startedAt: unknown;
    attempt: number;
    mode: MigrationMode;
    error?: string;
    runnerId?: string;
    host?: string;
}

export interface RecoveryResult {
    recovered: string[];      // migrations confirmed applied, history marked complete
    failed: string[];         // migrations not applied, marked failed
    skipped: string[];        // migrations already complete, no action needed
}

export interface MigrationRecoveryOptions {
    allTargets?: boolean;
    targetDatabase?: string;
    migrations?: MigrationDefinition[];
    directory?: string;
    source?: unknown;
}

export interface ReconcileResult {
    reconciled: string[];
    conflicts: string[];
    interruptions: InterruptionRecord[];
}

export interface MigrationRetryOptions {
    maxAttempts?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    retryableErrors?: string[];
}

export interface IndexInfo {
    name: string;
    table: string;
    columns: string[];
    unique: boolean;
    schema: string;
}

export interface ConstraintInfo {
    name: string;
    table: string;
    type: "PRIMARY KEY" | "UNIQUE" | "FOREIGN KEY" | "CHECK";
    columns: string[];
    schema: string;
    definition: string;
}

export interface ForeignKeyInfo {
    name: string;
    table: string;
    columns: string[];
    foreignTable: string;
    foreignColumns: string[];
    onDelete: string;
    onUpdate: string;
    schema: string;
}

export interface SequenceInfo {
    name: string;
    schema: string;
    dataType: string;
    startValue: number;
    incrementBy: number;
}

export interface EnumInfo {
    name: string;
    schema: string;
    values: string[];
}

export interface ExtendedColumnInfo {
    name: string;
    type: string;
    nullable: boolean;
    default: string | null;
    isIdentity: boolean;
    isGenerated: boolean;
    charMaxLength: number | null;
    numericPrecision: number | null;
    numericScale: number | null;
}

export interface ExtendedTableSchema {
    name: string;
    schema: string;
    columns: Record<string, ExtendedColumnInfo>;
    indexes: IndexInfo[];
    constraints: ConstraintInfo[];
    foreignKeys: ForeignKeyInfo[];
    sequences: SequenceInfo[];
    enums: EnumInfo[];
}

export interface SchemaChange {
    object: string;
    objectType: "table" | "column" | "index" | "constraint" | "foreign_key" | "sequence" | "enum";
    changeType: "create" | "drop" | "alter";
    risk: RollbackRisk;
    details: Record<string, unknown>;
    sql?: string;
    rollbackSql?: string;
    requiresManualIntervention: boolean;
}

export interface SchemaDiffResult {
    createTable: Array<{ name: string; schema: string; columns: Record<string, string> }>;
    dropTable: Array<{ name: string; schema: string }>;
    alterTable: Array<{
        name: string;
        schema: string;
        addColumns: Record<string, string>;
        dropColumns: string[];
    }>;
    changes: SchemaChange[];
    hasDestructiveChanges: boolean;
    hasSafeChanges: boolean;
}

export interface MigrationLogRecord {
    id: string;
    name: string;
    status: MigrationRunStatus;
    applied: boolean;
    checksum?: string;
    execution_ms?: number;
    applied_at?: unknown;
    failed_at?: unknown;
    error_message?: string;
    attempts?: number;
}

export interface Migrator {
    table: string;
    pools: Pool[];
    run(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; targetFailurePolicy?: "stop" | "continue" }): Promise<string[]>;
    status(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<MigrationLogRecord[]>;
    validate(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<{
        valid: boolean;
        results: Array<{ id: string; name: string; status: MigrationRunStatus; error?: string }>;
    }>;
    dryRun(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<{
        wouldApply: string[];
        checksums: Record<string, string>;
    }>;
    rollback(options?: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        steps?: number;
        dryRun?: boolean;
        allowDestructive?: boolean;
    }): Promise<string[]>;
    failed(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<MigrationLogRecord[]>;
    assessRollback(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; steps?: number }): Promise<RollbackAssessment[]>;
    history(options?: { migrationId?: string; limit?: number }): Promise<MigrationAuditEvent[]>;
    statusAllTargets(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<Map<string, MigrationLogRecord[]>>;
    retryFailed(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<{ retried: number; succeeded: number; failed: number; skipped: number }>;
    reconcile(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; allTargets?: boolean }): Promise<ReconcileResult>;
    detectDrift(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<{ drift: TargetDrift[] }>;
    detectInterruptions(options?: MigrationRecoveryOptions): Promise<{ interruptions: InterruptionRecord[] }>;
    recoverInterruptions(options?: MigrationRecoveryOptions): Promise<RecoveryResult>;
    detectLongRunningTransactions(options?: { minDurationMs?: number }): Promise<{ transactions: LongRunningTransaction[] }>;
    detectLockContention(options?: { minWaitMs?: number }): Promise<{ contention: LockContention[] }>;
    getLockDiagnostics(): Promise<LockDiagnostics>;
    expandContract(options: ExpandContractOptions): Promise<ExpandContractResult>;
    backfill(options: BackfillOptions): Promise<BackfillResult>;
    getBackfillStatus(options: BackfillStatusOptions): Promise<BackfillJob | null>;
    pauseBackfill(options: PauseBackfillOptions): Promise<BackfillJob>;
    resumeBackfill(options: ResumeBackfillOptions): Promise<BackfillJob>;
    retryBackfill(options: RetryBackfillOptions): Promise<BackfillResult>;
    cancelBackfill(options: CancelBackfillOptions): Promise<BackfillJob>;
    verifyBackup(options: { migrationId?: string; target?: string; restoreTarget?: string }): Promise<{
        backupId: string;
        targetDatabase: string;
        restored: boolean;
        verified: boolean;
        checks: Array<{ name: string; passed: boolean; detail?: string }>;
    }>;
    baseline(options?: BaselineOptions): Promise<BaselineResult>;
    listBaselines(): Promise<BaselineRecord[]>;
    squash(options?: SquashOptions): Promise<SquashResult>;
    progress(options?: MigrationProgressOptions): Promise<MigrationProgressResult>;
    discoverDependencies(options?: DiscoverDependenciesOptions): Promise<DiscoverDependenciesResult>;
    repairMigration(options: RepairMigrationOptions): Promise<RepairMigrationResult>;
    approveMigration(migrationId: string, context?: { runnerId?: string; host?: string; targetDatabase?: string }): Promise<MigrationApprovalResult>;
    checkApproval(migrationId: string, context?: { runnerId?: string; host?: string; targetDatabase?: string }): Promise<MigrationApprovalResult>;
    backups(options?: { migrationId?: string }): Promise<MigrationBackupRecord[]>;
    ensureTables?(): Promise<void>;
    validateRollback(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; steps?: number }): Promise<{
        valid: boolean;
        results: RollbackValidationResult[];
    }>;
    checkRollbackDependencies(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; steps?: number }): Promise<RollbackDependencyCheck>;
}

export interface LockWaitMetric {
    lockKey: number;
    waitMs: number;
    acquiredAt: unknown;
    runnerId?: string;
}

export interface LockContention {
    lockKey: number;
    waitMs: number;
    pid: number;
    query: string;
    state: string;
    applicationName?: string;
    targetDatabase?: string;
}

export interface LongRunningTransaction {
    pid: number;
    xactStart: unknown;
    durationMs: number;
    state: string;
    query: string | null;
    applicationName?: string;
    usename: string;
    clientAddr?: string;
    targetDatabase?: string;
}

export interface LockDiagnostics {
    advisoryLocks: LockWaitMetric[];
    lockContention: LockContention[];
    longRunningTransactions: LongRunningTransaction[];
    advisoryLockStatus: "free" | "held" | "contended";
}

export type ExpandContractPhase = "expand" | "backfill" | "verify" | "switch" | "contract";

export type ExpandContractPhaseStatus =
    | "pending"
    | "running"
    | "completed"
    | "failed"
    | "blocked"
    | "needs_approval"
    | "needs_reconciliation";

export interface ExpandContractPhaseState {
    phase: ExpandContractPhase;
    status: ExpandContractPhaseStatus;
    startedAt?: unknown;
    completedAt?: unknown;
    error?: string;
}

export interface ExpandContractStep {
    phase: "expand" | "backfill" | "verify" | "switch" | "contract";
    name: string;
    sql: string;
    downSql?: string;
    batchSize?: number;
    risk: RollbackRisk;
    requiresManualIntervention: boolean;
    requiresApproval?: boolean;
    nonTransactional?: boolean;
    compatibility?: { confirmed?: boolean };
}

export interface BackfillProgress {
    total: number;
    processed: number;
    percentage: number;
    startedAt: unknown;
    lastUpdatedAt: unknown;
    errors: number;
}

export interface ExpandContractOptions {
    steps: ExpandContractStep[];
    batchSize?: number;
    maxRetries?: number;
    onProgress?: (progress: BackfillProgress) => void;
    allowDestructive?: boolean;
    enforcePhases?: boolean;       // enforce expand→backfill→verify→switch→contract prerequisites
    compatibility?: { confirmed?: boolean };  // default compatibility acknowledgement for all steps
    migrationId?: string;          // stable id for phase tracking and approval scoping
}

export interface ExpandContractResult {
    completedSteps: string[];
    skippedSteps: string[];
    progress: BackfillProgress[];
    phaseStates: ExpandContractPhaseState[];
    successful: boolean;
    errors: string[];
}

export type BackfillCursorType = "serial" | "composite";

export interface BackfillCursor {
    type: BackfillCursorType;
    columns: string[];          // ordering columns, stable and unique
    direction?: "asc" | "desc";
}

export type BackfillJobStatus = "pending" | "running" | "paused" | "failed" | "completed" | "needs_reconciliation";

export interface BackfillJob {
    id: string;
    migrationId: string;
    targetDatabase?: string;
    jobName: string;
    status: BackfillJobStatus;
    sourceTable: string;
    cursorType: BackfillCursorType;
    cursorColumns: string[];
    cursorValue: unknown[] | null;
    batchSize: number;
    rowsProcessed: number;
    rowsRemaining: number | null;
    progressPercentage: number | null;
    startedAt: unknown;
    updatedAt: unknown;
    completedAt: unknown;
    lastError: string | null;
    retryCount: number;
    runnerId?: string;
    host?: string;
}

export interface BackfillBatchContext {
    client: PoolClient;
    query: MigrationQueryFn;
    rows: Array<Record<string, unknown>>;
    batchNumber: number;
}

export interface BackfillProcessBatchResult {
    processed: number;          // number of rows successfully transformed in this batch
    skipped?: number;           // rows that needed no change
    rowsRemaining?: number;     // estimate of remaining rows, if measurable
}

export interface BackfillOptions {
    id: string;                 // job name (unique per migration + target)
    migrationId: string;
    target?: string;            // target database name; defaults to pools[0] database
    table: string;
    cursor: BackfillCursor;
    batchSize?: number;
    processBatch: (ctx: BackfillBatchContext) => Promise<BackfillProcessBatchResult>;
    where?: string;             // optional trusted SQL filter appended to the batch SELECT
    maxRetries?: number;
    delayMs?: number;           // delay between batches
    statementTimeout?: number;
    onProgress?: (progress: BackfillProgress) => void;
}

export interface BackfillResult {
    jobId: string;
    status: BackfillJobStatus;
    rowsProcessed: number;
    batches: number;
    progress: BackfillProgress;
    errors: string[];
    completed: boolean;
}

export interface BackfillStatusOptions {
    id: string;
    target?: string;
}

export interface PauseBackfillOptions extends BackfillStatusOptions {}
export interface ResumeBackfillOptions extends BackfillStatusOptions {}
export interface RetryBackfillOptions extends BackfillStatusOptions {
    processBatch: (ctx: BackfillBatchContext) => Promise<BackfillProcessBatchResult>;
    maxRetries?: number;
    delayMs?: number;
    statementTimeout?: number;
    onProgress?: (progress: BackfillProgress) => void;
}
export interface CancelBackfillOptions extends BackfillStatusOptions {}

export interface SchemaCreate {
    table(name: string, columns: Record<string, string>): Promise<void>;
    index(name: string, table: string, columns: string | string[]): Promise<void>;
}

export interface SchemaAlter {
    addColumns(table: string, columns: Record<string, string>): Promise<void>;
    dropColumns(table: string, columns: string | string[]): Promise<void>;
    renameColumn(table: string, oldName: string, newName: string): Promise<void>;
    alterColumn(table: string, column: string, type: string): Promise<void>;
    renameTable(oldName: string, newName: string): Promise<void>;
    unique(table: string, constraint: string, columns: string | string[]): Promise<void>;
}

export interface SchemaDrop {
    table(name: string, options?: { cascade?: boolean }): Promise<void>;
    index(name: string, options?: { cascade?: boolean }): Promise<void>;
}

export interface SchemaConstrain {
    unique(tableName: string, constraint: string, columns: string | string[]): Promise<void>;
    foreignKey(
        tableName: string,
        constraint: string,
        columns: string | string[],
        referenceTable: string,
        referenceColumns: string | string[],
        options?: { onDelete?: string; onUpdate?: string },
    ): Promise<void>;
    check(tableName: string, constraint: string, expression: string): Promise<void>;
    dropConstraint(tableName: string, constraint: string): Promise<void>;
}

export interface SchemaManager {
    create: SchemaCreate;
    alter: SchemaAlter;
    drop: SchemaDrop;
    constrain: SchemaConstrain;
    inspect: {
        schema(options?: { schemas?: string[] }): Promise<ExtendedTableSchema[]>;
    };
    diff: {
        schema(
            current: ExtendedTableSchema[],
            desired: Record<string, Record<string, string>>,
        ): SchemaDiffResult;
    };
}

export interface Metrics {
    recordQuery(durationMs?: number): void;
    recordError(): void;
    recordSlowQuery(): void;
    recordRetry(): void;
    recordTransaction(rolledBack?: boolean): void;
    snapshot(): Record<string, number>;
    reset(): void;
}

export interface CheckApi {
    check(options?: { timeout?: number }): Promise<HealthCheckResult>;
    health(options?: { timeout?: number }): Promise<HealthResult>;
}

export interface ShutdownApi {
    shutdown(options?: { timeout?: number }): Promise<void>;
    readonly isClosed: boolean;
}

export interface StreamOptions {
    timeout?: number;
    shardKey?: string;
    shardId?: string;
    /** Abort signal to cancel the stream mid-execution. */
    signal?: AbortSignal;
    /** Number of retry attempts on stream/query failure (default: 0). */
    retries?: number;
    /** Progress callback invoked with rows streamed and elapsed time. */
    onProgress?: (info: { rows: number; elapsedMs: number }) => void;
}

export interface StreamApi {
    stream(text: string, values?: unknown[], options?: StreamOptions): Promise<NodeJS.ReadableStream>;
}

export interface CursorApi {
    cursor(text: string, values?: unknown[], options?: object): Promise<CursorHandle>;
}

export interface ClientApi {
    getClient(options?: { timeout?: number }): Promise<PoolClient>;
    query: QueryRunner;
    queryOne<T extends QueryResultRow = QueryResultRow>(text: string | object, values?: unknown[] | QueryOptions, options?: QueryOptions): Promise<T | null>;
    releaseClient(client?: PoolClient | null, error?: unknown): void;
    cancel(client: PoolClient): void;
    prepare(name: string, text: string): PreparedStatement;
    prepared(name: string, text: string, values?: unknown[], options?: QueryOptions): Promise<QueryResult>;
    hooks: QueryHooks;
    getActiveManualClientCount(): number;
}

export interface QueryContext {
    sql?: string;
    parameters?: unknown[];
    startedAt?: number;
    rowCount?: number;
    /**
     * The shard the query was routed to (undefined when not sharded / no shard key).
     */
    shardId?: string;
    /**
     * The routing strategy used to resolve the shard (undefined when not sharded).
     */
    routingStrategy?: string;
}

export interface MetricsCollector {
    recordQuery(durationMs?: number): void;
    recordError(): void;
    recordSlowQuery(): void;
    recordRetry(): void;
    recordTransaction(rolledBack?: boolean): void;
    snapshot(): Record<string, number>;
    reset(): void;
}

export interface QueryHooks {
    metrics: MetricsCollector;
    beforeQuery(context: QueryContext): void;
    afterQuery(context: QueryContext): void;
    onQueryError(context: QueryContext, error: DatabaseError): void;
    recordRetry(info: RetryInfo): void;
    config: InstrumentationOptions;
    logger?: unknown;
}

export interface HooksConfig {
    logQueries?: boolean;
    logParameters?: boolean;
    slowQueryMs?: number;
    onQuery?(event: { sql?: string; parameters?: unknown; durationMs: number; rowCount?: number; success?: boolean }): void;
    onError?(event: { error: unknown; sql?: string; durationMs: number }): void;
    onRetry?(info: RetryInfo): void;
}

export type Hooks = QueryHooks;

export interface RetryApi {
    run<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
    retry<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
    withRetry<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
    isTransient(error: unknown): boolean;
}

export interface Database {
    pool: Pool;
    getClient(options?: { timeout?: number }): Promise<PoolClient>;
    query<T extends QueryResultRow = QueryResultRow>(text: string | object, values?: unknown[] | QueryOptions, options?: QueryOptions): Promise<QueryResult<T>>;
    queryOne<T extends QueryResultRow = QueryResultRow>(text: string | object, values?: unknown[] | QueryOptions, options?: QueryOptions): Promise<T | null>;
    releaseClient(client?: PoolClient | null, error?: unknown): void;
    prepare(name: string, text: string): PreparedStatement;
    prepared(name: string, text: string, values?: unknown[], options?: QueryOptions): Promise<QueryResult>;
    transaction<T>(callback: (client: PoolClient) => Promise<T> | T, options?: TransactionOptions): Promise<T>;
    savepoint<T>(client: PoolClient, name: string, callback: (client: PoolClient) => Promise<T> | T): Promise<T>;
    stream(text: string, values?: unknown[], options?: object): Promise<NodeJS.ReadableStream>;
    cursor(text: string, values?: unknown[], options?: object): Promise<CursorHandle>;
    check(options?: { timeout?: number }): Promise<HealthCheckResult>;
    health(options?: { timeout?: number }): Promise<HealthResult>;
    shutdown(options?: { timeout?: number }): Promise<void>;
    readonly isClosed: boolean;
    batch: BatchApi;
    from(table: string): QueryBuilder;
    table(table: string): QueryBuilder;
    model(table: string, options?: { idColumn?: string }): Model;
    schema: SchemaManager;
    migrate: Migrator;
    hooks: Hooks;
    metrics: Metrics;
    retry: RetryApi;
    sharding: ShardingApi;
    /**
     * Online resharding / data migration API. Only present when sharding is
     * enabled and a resharding manager has been constructed. Operations are
     * explicit (plan/start/validate/cutover/finalize) and never start as a
     * side effect of addShard/removeShard.
     */
    resharding?: import("./resharding/resharding-types.js").ReshardingApi;
    /**
     * Replication API. Only present when replication is enabled (non-sharded
     * via `CreateDatabaseOptions.replication`, or per-shard via
     * `ShardConfig.replication`). When disabled, `enabled` is false and all
     * operations target the primary.
     */
    replication: ReplicationApi;
}