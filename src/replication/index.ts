export { ReplicationManager } from "./replication-manager.js";
export type { ReplicationManagerOptions } from "./replication-manager.js";
export { ReplicationRouter, isReplicaEligible } from "./replication-router.js";
export type { ReplicationEndpoint, ReplicationRouterOptions } from "./replication-router.js";
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
