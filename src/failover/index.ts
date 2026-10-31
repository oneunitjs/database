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
