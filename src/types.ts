import type { Pool, PoolClient, QueryResult, QueryResultRow, PoolConfig, QueryConfig } from "pg";
import type { Readable } from "node:stream";
import { FRAGMENT } from "./query/sql.js";

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
    | "weighted";

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
    [FRAGMENT]?: true;
    text: string;
    values: unknown[];
}

export type QueryFragment = SqlFragment;

export type QueryLike = string | SqlFragment | SqlFragment[] | Record<string, unknown> | unknown[];

export type SqlFn = (strings: TemplateStringsArray, ...values: QueryLike[]) => SqlFragment;

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
}

export interface BatchApi {
    insertMany(table: string, rows: Record<string, unknown>[], options?: InsertManyOptions): Promise<unknown>;
}

export interface QueryBuilder {
    select(...columns: Array<string | string[] | SqlFragment>): QueryBuilder;
    distinct(value?: boolean): QueryBuilder;
    insert(values: Record<string, unknown> | Record<string, unknown>[]): QueryBuilder;
    values(values: Record<string, unknown> | Record<string, unknown>[]): QueryBuilder;
    update(values: Record<string, unknown>): QueryBuilder;
    delete(): QueryBuilder;
    returning(...columns: Array<string | string[]>): QueryBuilder;
    where(column: string | Record<string, unknown> | SqlFragment, operator?: unknown, value?: unknown): QueryBuilder;
    orWhere(column: string, operator?: unknown, value?: unknown): QueryBuilder;
    whereNull(column: string): QueryBuilder;
    whereNotNull(column: string): QueryBuilder;
    whereIn(column: string, values: unknown[]): QueryBuilder;
    whereRaw(fragment: SqlFragment): QueryBuilder;
    join(table: string, on: string | SqlFragment): QueryBuilder;
    leftJoin(table: string, on: string | SqlFragment): QueryBuilder;
    groupBy(...columns: Array<string | string[]>): QueryBuilder;
    having(column: string, operator: string, value?: unknown): QueryBuilder;
    orderBy(column: string, direction?: string): QueryBuilder;
    limit(value: number): QueryBuilder;
    offset(value: number): QueryBuilder;
    onConflict(options: InsertManyOptions["onConflict"]): QueryBuilder;
    toSQL(): { text: string; values: unknown[] };
    execute(options?: QueryOptions): Promise<QueryResult>;
    first(options?: QueryOptions): Promise<QueryResultRow | null>;
    /**
     * Pin this builder to a specific shard key. Replaces any previously set
     * shard key or shard id. The shard key is merged into execution options at
     * `.execute()` time and routed through the unified executor.
     */
    withShard(shardKey: string): QueryBuilder;
    /**
     * Pin this builder to a specific shard id. Replaces any previously set
     * shard key or shard id.
     */
    withShardId(shardId: string): QueryBuilder;
    then<TResult1 = QueryResult, TResult2 = never>(
        onfulfilled?: ((value: QueryResult) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ): Promise<TResult1 | TResult2>;
}

export interface ModelFindOptions extends QueryOptions {
    columns?: string[];
    orderBy?: string | Array<[string, string?]>;
    direction?: string;
    limit?: number;
    offset?: number;
    client?: Pick<PoolClient, "query">;
    returning?: string[];
}

export interface Model {
    table: string;
    idColumn: string;
    query(): QueryBuilder;
    builder(): QueryBuilder;
    select(): QueryBuilder;
    find(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    findOne(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    findById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    insert(data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    insertMany(rows: Record<string, unknown>[], options?: ModelFindOptions): Promise<QueryResultRow[]>;
    update(where: Record<string, unknown>, data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    updateById(id: unknown, data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    delete(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    deleteById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    remove(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    removeById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    count(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<number>;
    exists(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<boolean>;
}

export type ModelFactory = (table: string, options?: { idColumn?: string }) => Model;

export interface MigrationDefinition {
    id?: string;
    name?: string;
    up: string | ((context: { client: PoolClient; query: PoolClient["query"]; logger?: Logger }) => Promise<unknown>);
    down?: string | ((context: { client: PoolClient; query: PoolClient["query"]; logger?: Logger }) => Promise<unknown>);
    sql?: string;
    source?: string;
}

export interface Migrator {
    table: string;
    run(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<string[]>;
    status(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<Array<{
        id: string;
        name: string;
        applied: boolean;
        appliedAt: unknown;
    }>>;
    rollback(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; steps?: number }): Promise<string[]>;
}

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
}

export interface StreamApi {
    stream(text: string, values?: unknown[], options?: StreamOptions): Promise<NodeJS.ReadableStream>;
}

export interface CursorApi {
    cursor(text: string, values?: unknown[], options?: object): Promise<CursorHandle>;
}

export interface BatchApi {
    insertMany(table: string, rows: Record<string, unknown>[], options?: InsertManyOptions): Promise<unknown>;
}

export interface ClientApi {
    getClient(options?: { timeout?: number }): Promise<PoolClient>;
    query: QueryRunner;
    queryOne<T extends QueryResultRow = QueryResultRow>(text: string | object, values?: unknown[] | QueryOptions, options?: QueryOptions): Promise<T | null>;
    releaseClient(client?: PoolClient | null, error?: unknown): void;
    prepare(name: string, text: string): PreparedStatement;
    prepared(name: string, text: string, values?: unknown[], options?: QueryOptions): Promise<QueryResult>;
    hooks: QueryHooks;
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

export class DatabaseError extends Error {
    name = "DatabaseError";
    code?: string;
    detail?: string;
    hint?: string;
    constraint?: string;
    table?: string;
    column?: string;
    schema?: string;
    dataType?: string;
    severity?: string;
    position?: string;
    where?: string;
    routine?: string;
    sqlState?: string;
    transient: boolean;
    cause?: unknown;

    constructor(message: string, options?: { cause?: unknown }) {
        super(message, options);
        this.transient = false;
    }

    toJSON(): Record<string, unknown> {
        return {
            name: this.name,
            message: this.message,
            code: this.code,
            detail: this.detail,
            hint: this.hint,
            constraint: this.constraint,
            table: this.table,
            column: this.column,
            schema: this.schema,
            dataType: this.dataType,
            severity: this.severity,
            position: this.position,
            where: this.where,
            routine: this.routine,
            sqlState: this.sqlState,
            transient: this.transient,
            cause: this.cause,
        };
    }
}

export class TimeoutError extends DatabaseError {
    name = "TimeoutError";
    timeout?: number;

    constructor(message: string, options?: { cause?: unknown; timeout?: number }) {
        super(message, options);
        this.timeout = options?.timeout;
    }
}

export declare const dbConfig: DatabaseConfig;

export declare function createDatabase(options?: CreateDatabaseOptions): Database;
export declare function createPool(options?: { logger?: Logger; config?: DatabaseConfig; onError?(error: unknown): void }): Pool;
export declare function createClient(pool: Pool, logger?: Logger, options?: CreateDatabaseOptions): Pick<Database, "getClient" | "query" | "queryOne" | "releaseClient" | "prepare" | "prepared" | "hooks">;
export declare function createTransaction(options: { pool: Pool; logger?: Logger; hooks?: Hooks; retry?: boolean | number | RetryOptions }): Pick<Database, "transaction" | "savepoint">;
export declare function createStream(pool: Pool, logger?: Logger, defaults?: { timeout?: number }): Pick<Database, "stream">;
export declare function createCursor(pool: Pool): Pick<Database, "cursor">;
export declare function createCheck(pool: Pool): Pick<Database, "check" | "health">;
export declare function createShutdown(pool: Pool, options?: { logger?: Logger; timeout?: number }): { shutdown: Database["shutdown"]; readonly isClosed: boolean };
export declare function createBatch(options?: { query?: Database["query"]; transaction?: Database["transaction"] }): BatchApi;
export declare function createSchemaManager(pool: Pool): SchemaManager;
export declare function createMigrator(pool: Pool, options?: { logger?: Logger; table?: string; lockKey?: number; source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Migrator;
export declare function createModel(options: { query: Database["query"]; table: string; idColumn?: string; logger?: Logger; transaction?: Database["transaction"] }): Model;
export declare function createModelFactory(options?: { query?: Database["query"]; logger?: Logger; transaction?: Database["transaction"] }): Database["model"];
export declare function createQueryBuilder(options?: { query?: Database["query"]; logger?: Logger }): Database["from"];
export declare function createHooks(options?: { logger?: Logger; metrics?: Metrics; instrumentation?: InstrumentationOptions } & InstrumentationOptions): Hooks;
export declare function createMetrics(): Metrics;
export declare function createRetry(defaults?: RetryOptions): RetryApi;
export declare function loadDatabaseConfig(overrides?: DatabaseConfig, env?: NodeJS.ProcessEnv): DatabaseConfig;
export declare function parseSslConfig(env?: NodeJS.ProcessEnv): SslConfig;
export declare function withRetry<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
export declare function normalizeError(error: unknown): DatabaseError;
export declare function isDatabaseError(error: unknown): error is DatabaseError;
export declare function isTransientError(error: unknown): boolean;
export declare function isUniqueViolation(error: unknown): boolean;
export declare function isForeignKeyViolation(error: unknown): boolean;
export declare function isNotNullViolation(error: unknown): boolean;
export declare function isCheckViolation(error: unknown): boolean;
export declare function isExclusionViolation(error: unknown): boolean;
export declare function isSerializationFailure(error: unknown): boolean;
export declare function isDeadlock(error: unknown): boolean;
export declare function isConnectionError(error: unknown): boolean;
export declare function isConstraintViolation(error: unknown): boolean;

export declare function sql(strings: TemplateStringsArray, ...values: unknown[]): SqlFragment;
export declare namespace sql {
    function raw(text: string): SqlFragment;
    function identifier(name: string): SqlFragment;
    function quoteIdent(name: string): string;
    function quoteIdentList(names: string | string[]): string;
    function isFragment(value: unknown): boolean;
    function join(values: unknown[], separator?: string): SqlFragment;
}

export declare function raw(text: string): SqlFragment;
export declare function identifier(name: string): SqlFragment;
export declare function quoteIdent(name: string): string;
export declare function quoteIdentList(names: string | string[]): string;

export declare function id(): { id: string };
export declare function bigIntId(): { id: string };
export declare function uuid(columnName?: string): Record<string, string>;
export declare function timestamp(...columnNames: string[]): Record<string, string>;

export declare function encode(value: unknown): Buffer;
export declare function decode(buffer: Buffer | Uint8Array | string): unknown;
export declare function encodeToString(value: unknown): string;
export declare function decodeFromString(value: string): unknown;
export declare function serializeRow(row: unknown): Buffer;
export declare function deserializeRow(buffer: Buffer | Uint8Array | string): unknown;
export declare function serializeQueryResult(result: Pick<QueryResult, "rows" | "rowCount" | "fields">): Buffer;
export declare function deserializeQueryResult(buffer: Buffer | Uint8Array | string): unknown;