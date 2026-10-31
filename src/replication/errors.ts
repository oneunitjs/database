import { DatabaseError } from "../client/errors.js";

const REPLICATION_CONFIG_ERROR = "REPLICATION_CONFIG_ERROR";
const REPLICA_UNAVAILABLE = "REPLICA_UNAVAILABLE";
const PRIMARY_UNAVAILABLE = "PRIMARY_UNAVAILABLE";
const WRITE_TO_REPLICA = "WRITE_TO_REPLICA";
const READ_PREFERENCE_ERROR = "READ_PREFERENCE_ERROR";

/**
 * Thrown when replication configuration is invalid before any resources are
 * created (duplicate endpoint IDs, invalid pool settings, missing primary,
 * invalid read preference, …).
 */
export class ReplicationConfigError extends DatabaseError {
    constructor(message: string, options?: { cause?: Error }) {
        super(message, {
            code: REPLICATION_CONFIG_ERROR,
            transient: false,
            ...options,
        });
        this.name = "ReplicationConfigError";
    }
}

/**
 * Thrown when no eligible replica exists for a read that requires one
 * (`replica-only`, or every replica is down / lagging beyond threshold).
 */
export class ReplicaUnavailableError extends DatabaseError {
    constructor(message: string, cause?: Error) {
        super(message, {
            code: REPLICA_UNAVAILABLE,
            transient: true,
            cause,
        });
        this.name = "ReplicaUnavailableError";
    }
}

/**
 * Thrown when the primary endpoint is unavailable for a write or strong-read.
 */
export class PrimaryUnavailableError extends DatabaseError {
    constructor(message: string, cause?: Error) {
        super(message, {
            code: PRIMARY_UNAVAILABLE,
            transient: true,
            cause,
        });
        this.name = "PrimaryUnavailableError";
    }
}

/**
 * Defensive error: an operation that must never target a replica was about to
 * be routed to one. Indicates a router bug rather than an operator error.
 */
export class WriteToReplicaError extends DatabaseError {
    constructor(message: string) {
        super(message, {
            code: WRITE_TO_REPLICA,
            transient: false,
        });
        this.name = "WriteToReplicaError";
    }
}

export class ReadPreferenceError extends DatabaseError {
    constructor(message: string) {
        super(message, {
            code: READ_PREFERENCE_ERROR,
            transient: false,
        });
        this.name = "ReadPreferenceError";
    }
}

export function isReplicationConfigError(error: unknown): boolean {
    return (
        error instanceof ReplicationConfigError ||
        ((error as Record<string, unknown>)?.code === REPLICATION_CONFIG_ERROR)
    );
}

export function isReplicaUnavailableError(error: unknown): boolean {
    return (
        error instanceof ReplicaUnavailableError ||
        ((error as Record<string, unknown>)?.code === REPLICA_UNAVAILABLE)
    );
}

export function isPrimaryUnavailableError(error: unknown): boolean {
    return (
        error instanceof PrimaryUnavailableError ||
        ((error as Record<string, unknown>)?.code === PRIMARY_UNAVAILABLE)
    );
}

export function isWriteToReplicaError(error: unknown): boolean {
    return (
        error instanceof WriteToReplicaError ||
        ((error as Record<string, unknown>)?.code === WRITE_TO_REPLICA)
    );
}

export function isReadPreferenceError(error: unknown): boolean {
    return (
        error instanceof ReadPreferenceError ||
        ((error as Record<string, unknown>)?.code === READ_PREFERENCE_ERROR)
    );
}
