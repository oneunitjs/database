export { ReplicationManager } from "./replication-manager.js";
export type { ReplicationManagerOptions } from "./replication-manager.js";
export { ReplicationRouter, isReplicaEligible } from "./replication-router.js";
export type { ReplicationEndpoint, ReplicationRouterOptions } from "./replication-router.js";
export {
    WeightedReplicaSelector,
    RoundRobinReplicaSelector,
    LeastConnectionsReplicaSelector,
    FirstEligibleReplicaSelector,
    createReplicaSelector,
    DEFAULT_WEIGHTED_OPTIONS,
} from "./replica-selectors.js";
export type {
    ReplicaSelector,
    ReplicaSelectionPolicy,
    SelectableReplica,
    WeightedReplicaSelectionOptions,
} from "./replica-selectors.js";
export { ReplicationConnectionTracker } from "./replication-connection-tracker.js";
export { ReplicaSelectionStatsTracker } from "./replication-stats.js";
export type {
    ReplicaSelectionEvent,
    ReplicaSelectionStats,
    ReplicaSuccessRate,
    ReplicaLatencyStats,
} from "./replication-stats.js";
export { LsnUtils } from "./lsn-utils.js";
export { WalTracker, DEFAULT_WAL_TRACKING_OPTIONS } from "./wal-tracker.js";
export type { ReplicationLagInfo, WalTrackingOptions, WalTrackableEndpoint } from "./wal-tracker.js";
export { ReadRoutingHookRunner, defaultReadRoutingHookLogger } from "./replication-hooks.js";
export type {
    ReadRoutingHookType,
    ReadRoutingHookContext,
    ReadRoutingHookData,
    ReadRoutingHook,
    ReadRoutingHooks,
    ReplicationRouterHookAPI,
} from "./replication-hooks.js";
export {
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
} from "./errors.js";
export { validateReplicationConfig } from "./replication-manager.js";
