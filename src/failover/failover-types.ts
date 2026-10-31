// ---------------------------------------------------------------------------
// Automatic Failover — HA Provider Interface
// ---------------------------------------------------------------------------

export type HaNodeRole = "primary" | "replica" | "unknown";

export type HaNodeStatus = "up" | "down" | "unknown";

export type FailoverState =
    | "healthy"
    | "suspect"
    | "primary_unavailable"
    | "failover_requested"
    | "promoting"
    | "verifying"
    | "primary_changed"
    | "recovering"
    | "failed";

export interface HaNodeInfo {
    id: string;
    role: HaNodeRole;
    status: HaNodeStatus;
    lagMs: number | null;
    lagUnknown: boolean;
    lastCheckedAt: string;
    consecutiveFailures: number;
    connection: {
        host?: string;
        port?: number;
        database?: string;
        user?: string;
        connectionString?: string;
    };
}

export interface HaTopology {
    generation: number;
    primary: HaNodeInfo | undefined;
    replicas: HaNodeInfo[];
    clusterId: string | undefined;
    capturedAt: string;
}

export interface FailoverEvent {
    id: string;
    shardId: string;
    fromState: FailoverState;
    toState: FailoverState;
    reason: string;
    triggeredBy: string;
    timestamp: string;
    metadata?: Record<string, unknown>;
}

export interface FailoverProviderConfig {
    [key: string]: unknown;
}

export interface HaProvider {
    readonly name: string;
    readonly supportsPromotion: boolean;
    readonly supportsTopologyGeneration: boolean;
    readonly supportsReplicationLag: boolean;

    /** Initialize the provider. May create coordination resources. */
    initialize(): Promise<void>;

    /** Discover the current HA topology for a shard. */
    discoverTopology(shardId: string): Promise<HaTopology>;

    /** Check the health of a specific node. */
    checkNodeHealth(nodeId: string, connection: HaNodeInfo["connection"]): Promise<HaNodeStatus>;

    /**
     * Request promotion of a replica to primary.
     * Returns the promoted node info if successful.
     * Only called by the failover manager after safety checks.
     */
    promoteReplica?(shardId: string, replicaId: string): Promise<HaNodeInfo>;

    /**
     * Verify that a node is eligible to accept writes as primary.
     * Returns true if the node is confirmed writable and fenced from split-brain.
     */
    verifyPrimary?(nodeId: string, expectedGeneration: number): Promise<boolean>;

    /** Fence the old primary to prevent split-brain writes. */
    fenceOldPrimary?(oldPrimaryId: string): Promise<boolean>;

    /** Get the current topology generation for conflict detection. */
    getTopologyGeneration?(shardId: string): Promise<number>;

    shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Distributed Transaction — Coordinator Types
// ---------------------------------------------------------------------------

export type DistributedTxStatus = "active" | "preparing" | "prepared" | "committing" | "committed" | "aborting" | "aborted" | "recovering";

export type ParticipantStatus = "active" | "prepared" | "committed" | "aborted" | "failed" | "unknown";

export interface DistributedTxParticipantRecord {
    transactionId: string;
    shardId: string;
    preparedTransactionId?: string; // PostgreSQL PREPARE TRANSACTION identifier
    participantStatus: ParticipantStatus;
    preparedAt?: string;
    completedAt?: string;
    errorDetails?: string;
}

export interface DistributedTxRecord {
    transactionId: string;
    status: DistributedTxStatus;
    createdAt: string;
    updatedAt: string;
    coordinatorId: string;
    participants: string[]; // shard IDs
    globalDecision?: "COMMIT" | "ABORT";
    decisionPersistedAt?: string;
    recoveryMetadata?: Record<string, unknown>;
}

export interface DistributedTxParticipant {
    shardId: string;
    pool: import("pg").Pool;
    client?: import("pg").PoolClient;
    preparedTransactionId?: string;
}

export interface DistributedTxOptions {
    timeout?: number;
    isolation?: "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
}

export interface DistributedTxResult<T> {
    transactionId: string;
    status: DistributedTxStatus;
    result?: T;
    participants: Record<string, ParticipantStatus>;
}

// ---------------------------------------------------------------------------
// Failover Configuration
// ---------------------------------------------------------------------------

export interface FailoverConfig {
    enabled: boolean;
    provider: HaProvider;
    healthCheckIntervalMs?: number;
    failureThreshold?: number; // consecutive failures before suspect
    recoveryThreshold?: number; // consecutive successes before healthy
    suspectTimeoutMs?: number; // time in suspect before failover_requested
    promotionTimeoutMs?: number; // time to wait for promotion
    verificationTimeoutMs?: number; // time to wait for verification
    maxPreparedTransactionAgeMs?: number; // max age for prepared transactions before recovery
    replicationLagThresholdMs?: number; // lag above which replica is not eligible
}

export interface ShardFailoverState {
    shardId: string;
    state: FailoverState;
    currentPrimary?: string;
    currentGeneration: number;
    failoverEvents: FailoverEvent[];
    lastStateChange: string;
    lastCheckedAt?: string;
    consecutiveFailures: number;
    consecutiveSuccesses: number;
}
