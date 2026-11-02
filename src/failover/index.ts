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
    DistributedTxStatus,
    ParticipantStatus,
    DistributedTxParticipantRecord,
    DistributedTxRecord,
    DistributedTxParticipant,
    DistributedTxOptions,
    DistributedTxResult,
} from "./failover-types.js";
export type {
    FailoverContext,
    FailoverStateVersion,
    PersistedShardState,
    FailoverStateStore,
} from "./failover-state-store.js";
export type {
    RecoveryResult,
    RecoveryAction,
    ValidationIssue,
    ValidationResult,
    FailoverRecoveryOptions,
} from "./failover-recovery.js";
export type {
    TrackedConnection,
    ConnectionInvalidationResult,
    ConnectionTrackerOptions,
} from "./connection-tracker.js";
export type {
    HealthCheckFailureCategory,
    HealthCheckFailureSeverity,
    ClassifiedFailure,
    HealthCheckFailure,
} from "./health-classifier.js";
export type {
    FailoverEventType,
    FailoverEvent as FailoverHistoryEvent,
    FailoverEventFilter,
    FailoverEventStatistics,
    FailoverEventQueryResult,
    FailoverEventStore,
} from "./failover-event-store.js";

export { StaticHaProvider } from "./ha-provider.js";
export { PatroniHaProvider } from "./ha-provider.js";
export { FailoverManager } from "./failover-manager.js";
export {
    createInitialFailoverState,
    applyFailoverTransition,
    createFailoverEvent,
    nextFailoverState,
    isTerminalFailoverState,
    isTransientFailoverState,
} from "./failover-state-machine.js";
export {
    FailoverError,
    FailoverNotSupportedError,
    SplitBrainPreventedError,
    PrimaryNotVerifiedError,
    isFailoverError,
} from "./errors.js";
export {
    PgFailoverStateStore,
    MemoryFailoverStateStore,
    FAILOVER_STATE_SCHEMA,
} from "./failover-state-store.js";
export { FailoverRecoveryManager } from "./failover-recovery.js";
export { ConnectionTracker } from "./connection-tracker.js";
export {
    HealthCheckClassifier,
    defaultHealthCheckClassifier,
} from "./health-classifier.js";
export { HealthCheckTracker, DEFAULT_FAIL_OVER_CONFIG } from "./health-check-tracker.js";
export type {
    NodeHealthStatus,
    NodeFailureThresholds,
    RequiredFailoverConfig,
} from "./health-check-tracker.js";
export {
    FailoverHookRunner,
    defaultHookLogger,
} from "./failover-hooks.js";
export type {
    FailoverHookType,
    FailoverHookContext,
    FailoverHookData,
    FailoverHook,
    FailoverManagerHookAPI,
    FailoverHooks,
} from "./failover-hooks.js";
export { PgFailoverEventStore, MemoryFailoverEventStore, FAILOVER_EVENT_SCHEMA } from "./failover-event-store.js";
export { PoolReconnector, DEFAULT_RECONNECTION_OPTIONS } from "./pool-reconnector.js";
export type {
    ReconnectionResult,
    ReconnectionOptions,
    PoolConfigProvider,
} from "./pool-reconnector.js";
export {
    AdvisoryLockFencingProvider,
    TableBasedFencingProvider,
    FencedError,
    defaultFencingHooks,
} from "./fencing.js";
export type {
    FencingProvider,
    FencingStatus,
    FencingOptions,
    FencingHooks,
    FencingActivationResult,
} from "./fencing.js";
export { FencingManager } from "./fencing-manager.js";
