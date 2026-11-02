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
export type { SslConfig, RetryOptions, InstrumentationOptions, IdleTransactionTimeout, DatabaseConfig, Database, QueryOptions, CreateDatabaseOptions, Logger, QueryFragment, SqlFragment, Model, ModelFactory, ModelOptions, ShardingConfig, ShardConfig, WeightedShardConfig, ShardRange, ShardStrategyName, ShardingApi, ShardInfo, ShardRoutingContext, ShardStrategy, LegacyShardingConfig, BaseShardingConfig, HashShardingConfig, ConsistentHashShardingConfig, RendezvousShardingConfig, RangeShardingConfig, DirectoryShardingConfig, TenantShardingConfig, CompositeShardingConfig, GeographicShardingConfig, WeightedShardingConfig, ReplicationConfig, ReplicationMode, ReadPreference, ReplicationEndpointConfig, ReplicationEndpointHealth, ReplicationHealthResult, ReplicationApi, ShardReplicationConfig, CustomShardStrategyRegistration } from "./types.js";
export { ShardManager, ShardRouter, ShardHealth, ShardMigrationTracker, ShardRebalanceTracker, HashShardStrategy, DirectoryShardStrategy, ConsistentHashShardStrategy, RendezvousShardStrategy, RangeShardStrategy, TenantShardStrategy, CompositeShardStrategy, GeographicShardStrategy, WeightedShardStrategy, createShardStrategy, registerShardStrategy, getRegisteredStrategyNames } from "./sharding/index.js";
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
export {
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

export {
    ReplicationManager,
    ReplicationRouter,
    isReplicaEligible,
    validateReplicationConfig,
    ReplicationConfigError,
    ReplicaUnavailableError,
    PrimaryUnavailableError,
    WriteToReplicaError,
    ReadPreferenceError,
    isReplicationConfigError,
    isReplicaUnavailableError,
    isPrimaryUnavailableError,
    isWriteToReplicaError,
    isReadPreferenceError,
} from "./replication/index.js";
export type { ReplicationManagerOptions, ReplicationEndpoint, ReplicationRouterOptions } from "./replication/index.js";

export {
    StaticHaProvider,
    PatroniHaProvider,
    FailoverManager,
    createInitialFailoverState,
    applyFailoverTransition,
    createFailoverEvent,
    nextFailoverState,
    isTerminalFailoverState,
    isTransientFailoverState,
    FailoverError,
    FailoverNotSupportedError,
    SplitBrainPreventedError,
    PrimaryNotVerifiedError,
    isFailoverError,
} from "./failover/index.js";
export type {
    HaNodeRole,
    HaNodeStatus,
    FailoverState,
    HaNodeInfo,
    HaTopology,
    FailoverEvent,
    FailoverProviderConfig,
    HaProvider,
    FailoverConfig,
    ShardFailoverState,
} from "./failover/index.js";

export {
    DistributedTransactionCoordinator,
    DistributedTransactionContext,
    DistributedTransactionError,
    DistributedTxNotFoundError,
    DistributedTxAlreadyResolvedError,
    MaxPreparedTransactionsError,
} from "./distributed-tx/index.js";
export type {
    ParticipantContext,
    DistributedTxCoordinatorOptions,
    DistributedTransactionExecuteOptions,
} from "./distributed-tx/index.js";
