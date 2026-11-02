export { ShardHealth } from "./shard-health.js";
export type { ShardHealthOptions } from "./shard-health.js";
export { ShardManager } from "./shard-manager.js";
export type { ShardManagerOptions } from "./shard-manager.js";
export { ShardRouter } from "./shard-router.js";
export type { ShardRouterOptions, ShardRange } from "./shard-router.js";
export { ShardMigrationTracker } from "./shard-migration.js";
export type { ShardMigrationTracker as ShardMigrationTrackerType } from "./shard-migration.js";
export { ShardRebalanceTracker } from "./shard-rebalance-tracker.js";
export type { ShardRebalanceTracker as ShardRebalanceTrackerType } from "./shard-rebalance-tracker.js";
export { ShardExecutor } from "./shard-executor.js";
export { createShardMetrics } from "./shard-metrics.js";
export type { ShardMetricsSnapshot } from "./shard-metrics.js";
export type {
    ShardExecutionContext,
    OperationType,
} from "./shard-execution-context.js";
export type {
    ShardRoutingOptions,
    ShardQueryOptions,
} from "./shard-executor.js";
export {
    createShardExecutionContext,
    createPrimaryExecutionContext,
} from "./shard-execution-context.js";
export {
    ShardNotFoundError,
    ShardUnavailableError,
    ShardingDisabledError,
    ShardConfigError,
    CrossShardTransactionError,
    ShardRoutingError,
    ShardNotReadyError,
    ShardCoordinationError,
    ShardTopologyStaleError,
    ShardOperationTimeoutError,
    ShardDrainTimeoutError,
    ShardMigrationPartialFailureError,
    isShardNotFoundError,
    isShardUnavailableError,
    isShardingDisabledError,
    isCrossShardTransactionError,
    isShardRoutingError,
    isShardNotReadyError,
    isShardCoordinationError,
    isShardTopologyStaleError,
    isShardOperationTimeoutError,
    isShardDrainTimeoutError,
    isShardMigrationPartialFailureError,
} from "./errors.js";
export {
    HashShardStrategy,
    DirectoryShardStrategy,
    ConsistentHashShardStrategy,
    RendezvousShardStrategy,
    RangeShardStrategy,
    TenantShardStrategy,
    CompositeShardStrategy,
    GeographicShardStrategy,
    WeightedShardStrategy,
    createShardStrategy,
    registerShardStrategy,
    getRegisteredStrategyNames,
} from "./strategies.js";

// Schema auto-sync
export { SchemaVersionTracker } from "./schema-version-tracker.js";
export { SchemaHashCalculator } from "./schema-hash-calculator.js";
export type {
    ColumnSchemaInfo as SchemaColumnSchemaInfo,
    IndexSchemaInfo as SchemaIndexSchemaInfo,
    ConstraintSchemaInfo as SchemaConstraintSchemaInfo,
    TableSchemaInfo as SchemaTableSchemaInfo,
    ShardSchemaInfo,
} from "./schema-hash-calculator.js";
export { SchemaValidator } from "./schema-validator.js";
export { MigrationApplier } from "./migration-applier.js";
export { SyncLockManager } from "./sync-lock-manager.js";
export { SyncStateStore } from "./sync-state-store.js";
export { SchemaSyncService } from "./schema-sync-service.js";
export { SchemaSyncCLI } from "./schema-sync-cli.js";
export { SchemaSyncWorker } from "./schema-sync-worker.js";
export { SchemaRollbackCoordinator } from "./schema-rollback-coordinator.js";
export { SchemaSyncReporter } from "./schema-sync-reporter.js";
export * from "./schema-sync-types.js";
export { SCHEMA_SYNC_SCHEMA } from "./schema-sync-schema.js";

// Re-export coordinator store maybe
