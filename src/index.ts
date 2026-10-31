export * from "./database.js";
export * from "./client/client.js";
export * from "./client/pool.js";
export * from "./client/config.js";
export * from "./client/transaction.js";
export * from "./client/stream.js";
export * from "./client/cursor.js";
export * from "./client/check.js";
export * from "./client/shutdown.js";
export * from "./client/batch.js";
export * from "./client/errors.js";
export * from "./client/retry.js";
export * from "./client/hooks.js";
export * from "./column/index.js";
export * from "./schema/index.js";
export * from "./query/sql.js";
export * from "./query/builder.js";
export * from "./model/model.js";
export * from "./json-serialization.js";
export type { SslConfig, RetryOptions, InstrumentationOptions, IdleTransactionTimeout, DatabaseConfig, ShardingConfig, ShardConfig, WeightedShardConfig, ShardRange, ShardStrategyName, ShardingApi, ShardInfo, ShardRoutingContext, ShardStrategy, LegacyShardingConfig, BaseShardingConfig, HashShardingConfig, ConsistentHashShardingConfig, RendezvousShardingConfig, RangeShardingConfig, DirectoryShardingConfig, TenantShardingConfig, CompositeShardingConfig, GeographicShardingConfig, WeightedShardingConfig, ReplicationConfig, ReplicationMode, ReadPreference, ReplicationEndpointConfig, ReplicationEndpointHealth, ReplicationHealthResult, ReplicationApi, ShardReplicationConfig } from "./types.js";
export { ShardManager, ShardRouter, ShardHealth, ShardMigrationTracker, HashShardStrategy, DirectoryShardStrategy, ConsistentHashShardStrategy, RendezvousShardStrategy, RangeShardStrategy, TenantShardStrategy, CompositeShardStrategy, GeographicShardStrategy, WeightedShardStrategy, createShardStrategy, getRegisteredStrategyNames } from "./sharding/index.js";
export type { ShardManagerOptions, ShardRouterOptions, ShardHealthOptions } from "./sharding/index.js";
export { ShardNotFoundError, ShardUnavailableError, ShardingDisabledError, ShardConfigError, CrossShardTransactionError, ShardRoutingError, ShardNotReadyError, ShardCoordinationError, ShardTopologyStaleError, ShardOperationTimeoutError, ShardDrainTimeoutError, ShardMigrationPartialFailureError, isShardNotFoundError, isShardUnavailableError, isShardingDisabledError, isCrossShardTransactionError, isShardRoutingError, isShardNotReadyError, isShardCoordinationError, isShardTopologyStaleError, isShardOperationTimeoutError, isShardDrainTimeoutError, isShardMigrationPartialFailureError } from "./sharding/errors.js";

// Online resharding / data migration
export {
    createReshardingManager,
} from "./resharding/resharding-manager.js";
export type {
    ReshardingApi,
    ReshardingStatus,
    ReshardingStatusInfo,
    ReshardingPlan,
    ReshardingOptions,
    PlanOptions,
    MigrationDescriptor,
    MigrationTableAdapter,
    MigrationUnit,
    CheckpointCursor,
    TopologySnapshot,
    ChangeEvent,
    ProgressEvent,
    ValidationReport,
    ValidationFailure,
    BulkCopyOptions,
    ValidationOptions,
    CutoverOptions,
    ReshardingHooks,
} from "./resharding/resharding-types.js";
export type {
    ReshardingError,
    UnsupportedStrategyError,
    MissingSchemaMetadataError,
    StalePlanError,
    InvalidStateTransitionError,
    MigrationNotFoundError,
    MigrationAlreadyExistsError,
    InconsistentRoutingVersionError,
    DestinationUnavailableError,
    ValidationFailedError,
    UnsafeShardRemovalError,
    UnrecoverableCutoverError,
    WriteFencedError,
    MigrationLockedError,
    isReshardingError,
    isUnsupportedStrategyError,
    isMissingSchemaMetadataError,
    isStalePlanError,
    isInvalidStateTransitionError,
    isMigrationNotFoundError,
    isInconsistentRoutingVersionError,
    isDestinationUnavailableError,
    isValidationFailedError,
    isUnsafeShardRemovalError,
    isWriteFencedError,
    isMigrationLockedError,
} from "./resharding/resharding-errors.js";
export { topologyFromConfig } from "./resharding/resharding-planner.js";
export type { ReshardingSchema } from "./resharding/resharding-schema.js";
export { RESHARDING_SCHEMA_VERSION } from "./resharding/resharding-schema.js";
export { createReshardingSchema } from "./resharding/resharding-schema.js";
