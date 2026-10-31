// ---------------------------------------------------------------------------
// Online resharding / data migration types
// ---------------------------------------------------------------------------

import type { PoolConfig } from "pg";
import type { QueryResultRow, ShardMapVersion } from "../types.js";

/**
 * Durable resharding lifecycle states.
 *
 * Legal transitions (enforced by the persistence layer):
 *
 *   planned -> copying -> catching_up -> validating -> ready_for_cutover
 *   planned -> paused
 *   copying -> paused | failed | cancelled
 *   catching_up -> paused | failed | cancelled | validating
 *   validating -> ready_for_cutover | failed | cancelled | rollback_required
 *   ready_for_cutover -> cutting_over | paused | cancelled | rollback_required
 *   cutting_over -> completed | failed | rollback_required
 *   failed -> cancelled | rollback_required | planned (operator retry)
 *   rollback_required -> cancelled | planned (operator retry)
 *   cancelled -> (terminal)
 *   completed -> finalized | (terminal)
 *   finalized -> (terminal)
 *   paused -> copying | catching_up | validating | cancelled | failed
 *
 * `paused`, `failed`, `cancelled`, `rollback_required` and `finalized` are
 * operator-visible states; none of them is ever interpreted as `completed`.
 */
export type ReshardingStatus =
    | "planned"
    | "copying"
    | "catching_up"
    | "validating"
    | "ready_for_cutover"
    | "cutting_over"
    | "completed"
    | "paused"
    | "failed"
    | "cancelled"
    | "rollback_required"
    | "finalized";

export interface ReshardingStatusInfo {
    migrationId: string;
    status: ReshardingStatus;
    version: number; // optimistic concurrency version
    sourceRoutingVersion: ShardMapVersion;
    targetRoutingVersion: ShardMapVersion;
    createdAt: string;
    updatedAt: string;
    startedAt?: string;
    completedAt?: string;
    finalizedAt?: string;
    pausedAt?: string;
    unitsTotal: number;
    unitsCompleted: number;
    unitsFailed: number;
    rowsCopied: number;
    rowsValidated: number;
    changeLogLag: number; // unapplied change events
    errorCode?: string;
    errorMessage?: string; // sanitized, never contains credentials or row contents
    cutoverIntent: boolean;
    cutoverCommitted: boolean;
}

export interface TopologySnapshot {
    shards: TopologyShard[];
    strategy: ShardStrategyName;
    strategyConfig: unknown;
    routingVersion: ShardMapVersion;
    capturedAt: string;
}

export interface TopologyShard {
    id: string;
    version: ShardMapVersion;
    connection: Pick<PoolConfig, "host" | "port" | "database" | "user">; // sanitized, no credentials
    addedAt?: string;
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

export interface MigrationUnit {
    unitId: string;
    unitType: "shard-key" | "range" | "bucket" | "tenant";
    sourceShardId: string;
    destinationShardId: string;
    shardKey?: string;        // logical shard key value (directory / tenant / range start)
    hashBucket?: number;      // hash / consistent-hash bucket
    rangeStart?: string | number;
    rangeEnd?: string | number;
    tableName?: string;       // table this unit belongs to (for multi-table migrations)
    estimatedRows: number | null;
    status: "planned" | "copying" | "caught_up" | "validated" | "cut_over" | "completed" | "failed";
    rowsCopied: number;
    lastCheckpoint?: CheckpointCursor;
    captureTxid?: number;        // transaction snapshot boundary (xmin) at capture time
    captureXmax?: number;        // xmax component of txid_current_snapshot() at capture time
    captureXip?: number[];      // in-progress XIDs at capture time (xmin..xmax)
    updatedAt: string;
}

export interface CheckpointCursor {
    // Opaque, adapter-defined cursor for resuming a keyset scan.
    // Never contains raw row contents or sensitive key values in logs.
    kind: "keyset";
    columns: string[];
    lastValues: unknown[];
    batchNo: number;
    updatedAt: string;
}

export interface ReshardingPlan {
    migrationId: string;
    createdAt: string;
    sourceRoutingVersion: ShardMapVersion;
    targetRoutingVersion: ShardMapVersion;
    sourceTopology: TopologySnapshot;
    targetTopology: TopologySnapshot;
    strategy: ShardStrategyName;
    strategyConfig: unknown;
    units: MigrationUnit[];
    estimatedTotalRows: number | null;
    affectedShardIds: string[];
    dryRun: boolean;
    version: number;
}

export interface PlanOptions {
    /** When true, the plan is computed but not persisted and no data changes are made. */
    dryRun?: boolean;
    /** Expected current routing version; plan is rejected if it differs (stale-plan detection). */
    expectedRoutingVersion?: ShardMapVersion;
    /** Maximum estimated rows to copy; planning fails hard when exceeded to avoid runaway migrations. */
    maxEstimatedRows?: number;
}

/**
 * Application-defined contract for migrating a table. The package cannot
 * safely infer schemas or shard-key semantics, so every participating table
 * must be described through this adapter.
 */
export interface MigrationTableAdapter {
    table: string;
    /**
     * Primary-key column(s), in order. Used for idempotent destination writes
     * (ON CONFLICT) and duplicate detection during validation.
     */
    primaryKeyColumns: string[];
    /**
     * Shard-key column(s) used to compute the row's destination shard.
     * The adapter is responsible for interpreting these consistently with the
     * routing strategy.
     */
    shardKeyColumns: string[];
    /**
     * Whether the adapter guarantees that reads ordered by primary key columns
     * produce a stable, repeatable scan (e.g. a monotonic PK or a unique index
     * used as the keyset cursor). When false, the copier adds a deterministic
     * tie-breaker and cannot assume monotonicity.
     */
    stableKeyOrder: boolean;

    /**
     * Read a batch of rows from the source shard for a migration unit.
     * `cursor` is the previously returned CheckpointCursor (or null for the
     * first batch). Must return rows in a stable, deterministic order.
     */
    readBatch: (
        params: {
            client: import("pg").PoolClient;
            sourceShardId: string;
            unit: MigrationUnit;
            batchSize: number;
            cursor: CheckpointCursor | null;
        },
    ) => Promise<{ rows: Record<string, unknown>[]; nextCursor: CheckpointCursor | null; done: boolean }>;

    /**
     * Compute the destination shard id for a row using the target routing
     * configuration. Used to validate the plan and to detect rows whose
     * calculated destination differs from the plan.
     */
    destinationShardForRow?: (row: Record<string, unknown>, targetTopology: TopologySnapshot) => string | Promise<string>;

    /**
     * Insert or update rows at the destination idempotently.
     * Implementations should use ON CONFLICT (primary key) upsert semantics.
     * Must not overwrite newer destination data with stale snapshot data:
     * apply changes strictly in change-log sequence order after the snapshot.
     *
     * `rows` provides bulk-copy rows (backward compatible).
     * `changes` provides change events with metadata (D3) including sourceTxId
     * for conflict resolution. The destination table must have a `source_tx_id`
     * column for conflict resolution to work:
     * ON CONFLICT (id) DO UPDATE SET ... WHERE excluded.source_tx_id >
     * destination.source_tx_id
     */
    upsertRows: (
        params: {
            client: import("pg").PoolClient;
            destinationShardId: string;
            unit: MigrationUnit;
            rows: Record<string, unknown>[];
            changes?: ChangeEvent[];
        },
    ) => Promise<{ inserted: number; updated: number }>;

    /**
     * Delete rows at the destination (tombstone propagation). Idempotent.
     */
    deleteRows?: (
        params: {
            client: import("pg").PoolClient;
            destinationShardId: string;
            unit: MigrationUnit;
            shardKeys: string[]; // shard-key values to delete
        },
    ) => Promise<{ deleted: number }>;

    /**
     * Optional validation: count rows for a unit on a given shard.
     */
    countRows?: (
        params: {
            client: import("pg").PoolClient;
            shardId: string;
            unit: MigrationUnit;
        },
    ) => Promise<number>;

    /**
     * Optional validation: compute a deterministic content hash for a unit.
     */
    computeChecksum?: (
        params: {
            client: import("pg").PoolClient;
            shardId: string;
            unit: MigrationUnit;
            cursor?: CheckpointCursor | null;
        },
    ) => Promise<{ hash: string; rowCount: number }>;

    /**
     * Optional list of dependent tables that must be migrated before/after
     * this table (foreign-key ordering constraints).
     */
    dependsOn?: string[];
    /** Human-readable description for audit logging. */
    description?: string;
}

export interface MigrationDescriptor {
    migrationId: string;
    tables: MigrationTableAdapter[];
    /**
     * Table migration order, respecting dependencies. When omitted, the
     * descriptor is validated for dependency cycles and tables are migrated
     * in descriptor order.
     */
    order?: string[];
    /**
     * Consistency boundary for validation/cutover: "snapshot" (validate
     * against a point-in-time snapshot) or "streamed" (validate after change
     * log is drained at cutover).
     */
    consistencyBoundary: "snapshot" | "streamed";
}

export interface ChangeEvent {
    id: string; // stable, monotonically ordered change-log sequence id
    sequence: number;
    migrationId: string;
    unitId: string | null;
    table: string;
    operation: "insert" | "update" | "delete";
    shardKey: string | null;
    oldRow: Record<string, unknown> | null;
    newRow: Record<string, unknown> | null;
    createdAt: string;
    appliedAt?: string;
    /** Transaction commit timestamp (approximate commit time). */
    commitTs?: Date;
    /** Source transaction ID (assigned at BEGIN, used for grouping). */
    sourceTxId: number;
    /** Commit order (assigned at COMMIT time via advisory lock, used for correct replay ordering). */
    commitOrder: number;
}

export interface ProgressEvent {
    type: "batch-copied" | "checkpoint-persisted" | "change-applied" | "phase-changed" | "validation-completed" | "cutover-committed" | "error";
    migrationId: string;
    unitId?: string;
    timestamp: string;
    rowsCopied?: number;
    rowsRemaining?: number;
    changeLogLag?: number;
    detail?: Record<string, unknown>; // sanitized: no credentials or row contents
}

export interface ValidationReport {
    table: string;
    unitId?: string;
    sourceCount: number;
    destinationCount: number;
    countMatch: boolean;
    duplicateKeys: string[];
    missingKeys: string[];
    extraKeys: string[];
    checksumMatch: boolean | null;
    sourceChecksum: string | null;
    destinationChecksum: string | null;
    unappliedChanges: number;
    mismatchedDestination: number; // rows whose calculated destination != plan
    passed: boolean;
    failures: ValidationFailure[];
}

export interface ValidationFailure {
    code: string;
    message: string; // sanitized
    table: string;
    unitId?: string;
    severity: "warning" | "error";
}

export interface BulkCopyOptions {
    batchSize: number;
    maxConcurrency: number;
    maxRetries: number;
    baseRetryDelayMs: number;
    maxRetryDelayMs: number;
    throttleMs?: number; // optional inter-batch throttling
    batchTimeoutMs: number;
    idleTimeoutMs: number;
}

export interface ValidationOptions {
    enabled: boolean;
    checkCounts: boolean;
    checkDuplicates: boolean;
    checkChecksums: boolean;
    checkDestinationPlacement: boolean;
}

export interface CutoverOptions {
    /** Maximum milliseconds to wait for the change log to drain before cutover. */
    drainTimeoutMs: number;
    /** Maximum milliseconds to wait for in-flight writes to the moved keys to complete. */
    writeFenceTimeoutMs: number;
    /** Require validation to pass before allowing cutover. */
    requireValidation: boolean;
}

export interface ReshardingHooks {
    /** Advisory notification only; failure does not affect the migration protocol. */
    onProgress?: (event: ProgressEvent) => void;
    /** Advisory notification. */
    onStatusChange?: (migrationId: string, from: ReshardingStatus, to: ReshardingStatus) => void;
    /**
     * Protocol hook: participate in cutover. If it throws, cutover is aborted
     * and the migration enters rollback_required. Runs inside the cutover
     * commit transaction boundary conceptually (coordination DB commit).
     */
    onBeforeCutover?: (migrationId: string, plan: ReshardingPlan) => Promise<void> | void;
    /** Protocol hook: run after cutover commits. Advisory; failure is logged but does not roll back the committed cutover. */
    onAfterCutover?: (migrationId: string) => Promise<void> | void;
}

export interface ReshardingOptions {
    /** Coordination authority: a dedicated pool (or pool config) where the migration control plane lives. */
    coordination?: PoolConfig | import("pg").Pool;
    /** Schema prefix for resharding tables on shard databases (change log, checkpoints). Default "resharding". */
    shardSchemaPrefix?: string;
    /** Coordination schema/table prefix. Default "resharding". */
    coordinationSchemaPrefix?: string;
    logger?: import("../types.js").Logger;
    hooks?: ReshardingHooks;
    bulkCopy?: Partial<BulkCopyOptions>;
    validation?: Partial<ValidationOptions>;
    cutover?: Partial<CutoverOptions>;
}

export interface ReshardingApi {
    readonly migrationId: string | null;
    /** Plan a migration. Persists the plan unless dryRun. */
    plan(descriptor: MigrationDescriptor, targetTopology: TopologySnapshot, options?: PlanOptions): Promise<ReshardingPlan>;
    /**
     * Reattach to an existing migration by id after a process restart.
     * Returns undefined when no migration with that id exists.
     */
    load(migrationId: string): Promise<ReshardingStatusInfo | undefined>;
    /** Start executing an existing plan. Claims the migration worker lock. */
    start(): Promise<ReshardingStatusInfo>;
    getStatus(): Promise<ReshardingStatusInfo | undefined>;
    pause(): Promise<ReshardingStatusInfo>;
    resume(): Promise<ReshardingStatusInfo>;
    cancel(): Promise<ReshardingStatusInfo>;
    /**
     * Validate before cutover. Accepts an optional migration descriptor;
     * required on restart recovery when the descriptor was not re-registered.
     */
    validate(descriptor?: MigrationDescriptor): Promise<{ report: ValidationReport[]; passed: boolean }>;
    /**
     * Coordinated cutover. Accepts an optional migration descriptor;
     * required on restart recovery when the descriptor was not re-registered.
     */
    cutover(descriptor?: MigrationDescriptor): Promise<ReshardingStatusInfo>;
    finalize(): Promise<ReshardingStatusInfo>;
    /** Refresh the active routing version from the coordination authority. */
    refreshRoutingVersion(): Promise<ShardMapVersion>;
    /** Check whether writes for a shard key are currently fenced by an in-progress cutover. */
    isWriteFenced(shardKey: string): Promise<boolean>;
    /**
     * Check whether a shard's write gate is fenced. Durable, cross-instance
     * enforcement: the gate is stored shard-locally in PostgreSQL and checked
     * by the write path before executing any write.
     */
    isShardWriteFenced(shardId: string): Promise<boolean>;
    /** Record a change event within a source transaction (transactional outbox). Call inside db.transaction/sharding.transaction. */
    recordChange(client: import("pg").PoolClient, event: Omit<ChangeEvent, "id" | "sequence" | "createdAt">): Promise<ChangeEvent>;
    /**
     * Execute application mutations and CDC change capture atomically in a
     * single transaction on the source shard. The callback receives the
     * transaction client and a `recordChange` function that writes change
     * events through the same client, so mutations and their CDC records
     * commit or roll back together.
     *
     * @param shardId - Source shard ID (resolve from shardKey via
     *   `db.sharding.getShardForKey(shardKey).id`).
     * @param callback - Receives `(client, recordChange)`. All mutations and
     *   CDC events recorded via `recordChange` share the same transaction.
     * @param txOptions - Standard transaction options.
     * @throws WriteFencedError if the shard's write gate is fenced.
     * @throws ReshardingError if the shard pool is unavailable.
     */
    instrumentedTransaction<T>(
        shardId: string,
        callback: (
            client: import("pg").PoolClient,
            recordChange: (
                event: Omit<ChangeEvent, "id" | "sequence" | "createdAt">,
            ) => Promise<ChangeEvent>,
        ) => Promise<T> | T,
        txOptions?: import("../types.js").TransactionOptions,
    ): Promise<T>;
    /**
     * Drain and apply pending change events for a unit up to a sequence boundary.
     * `apply` is called with a batch of change events in commit-approximate order
     * and should apply them idempotently at the destination.
     *
     * @param migrationId - Migration ID (used to look up the source shard if shardId not provided)
     * @param unitId - Unit ID
     * @param uptoSequence - Maximum sequence number to drain
     * @param apply - Callback to apply change events (at least once delivery)
     * @param afterTxid - Only drain changes with txid >= this value
     * @param shardId - Optional source shard ID. If provided, skips the coordination DB lookup for the unit's source shard.
     */
    drainChanges(migrationId: string, unitId: string, uptoSequence: number, apply?: (events: ChangeEvent[]) => Promise<void>, afterTxid?: number, shardId?: string): Promise<{ applied: number }>;
    shutdown(): Promise<void>;
    readonly isClosed: boolean;
}

declare const migrationIdSymbol: unique symbol;
export interface ReshardingContext {
    readonly [migrationIdSymbol]: string;
}
