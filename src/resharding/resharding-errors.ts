import { DatabaseError } from "../client/errors.js";

const RESHARDING_ERROR = "RESHARDING_ERROR";
const UNSUPPORTED_STRATEGY = "UNSUPPORTED_STRATEGY";
const MISSING_SCHEMA_METADATA = "MISSING_SCHEMA_METADATA";
const STALE_PLAN = "STALE_PLAN";
const INVALID_STATE_TRANSITION = "INVALID_STATE_TRANSITION";
const MIGRATION_NOT_FOUND = "MIGRATION_NOT_FOUND";
const MIGRATION_ALREADY_EXISTS = "MIGRATION_ALREADY_EXISTS";
const INCONSISTENT_ROUTING_VERSION = "INCONSISTENT_ROUTING_VERSION";
const DESTINATION_UNAVAILABLE = "DESTINATION_UNAVAILABLE";
const VALIDATION_FAILED = "VALIDATION_FAILED";
const UNSAFE_SHARD_REMOVAL = "UNSAFE_SHARD_REMOVAL";
const UNRECOVERABLE_CUTOVER = "UNRECOVERABLE_CUTOVER";
const WRITE_FENCED = "WRITE_FENCED";
const MIGRATION_LOCKED = "MIGRATION_LOCKED";

export class ReshardingError extends DatabaseError {
    constructor(message: string, options?: { code?: string; transient?: boolean; cause?: unknown }) {
        super(message, { cause: options?.cause as Error | undefined });
        this.name = "ReshardingError";
        this.code = options?.code ?? RESHARDING_ERROR;
        this.transient = options?.transient ?? false;
    }
}

export class UnsupportedStrategyError extends ReshardingError {
    constructor(strategy: string, detail?: string) {
        super(
            `Unsupported resharding strategy: ${strategy}${detail ? ` — ${detail}` : ""}`,
            { code: UNSUPPORTED_STRATEGY, transient: false },
        );
        this.name = "UnsupportedStrategyError";
    }
}

export class MissingSchemaMetadataError extends ReshardingError {
    constructor(table: string, detail: string) {
        super(`Missing schema metadata for table "${table}": ${detail}`, {
            code: MISSING_SCHEMA_METADATA,
            transient: false,
        });
        this.name = "MissingSchemaMetadataError";
    }
}

export class StalePlanError extends ReshardingError {
    constructor(migrationId: string, detail: string) {
        super(`Stale resharding plan "${migrationId}": ${detail}`, {
            code: STALE_PLAN,
            transient: false,
        });
        this.name = "StalePlanError";
    }
}

export class InvalidStateTransitionError extends ReshardingError {
    constructor(migrationId: string, from: string, to: string) {
        super(`Invalid state transition for migration "${migrationId}": ${from} -> ${to}`, {
            code: INVALID_STATE_TRANSITION,
            transient: false,
        });
        this.name = "InvalidStateTransitionError";
    }
}

export class MigrationNotFoundError extends ReshardingError {
    constructor(migrationId: string) {
        super(`Resharding migration not found: ${migrationId}`, {
            code: MIGRATION_NOT_FOUND,
            transient: false,
        });
        this.name = "MigrationNotFoundError";
    }
}

export class MigrationAlreadyExistsError extends ReshardingError {
    constructor(migrationId: string) {
        super(`Resharding migration already exists: ${migrationId}`, {
            code: MIGRATION_ALREADY_EXISTS,
            transient: false,
        });
        this.name = "MigrationAlreadyExistsError";
    }
}

export class InconsistentRoutingVersionError extends ReshardingError {
    constructor(expected: number, actual: number, detail?: string) {
        super(
            `Inconsistent routing version: expected ${expected}, actual ${actual}${detail ? ` — ${detail}` : ""}`,
            { code: INCONSISTENT_ROUTING_VERSION, transient: false },
        );
        this.name = "InconsistentRoutingVersionError";
    }
}

export class DestinationUnavailableError extends ReshardingError {
    constructor(shardId: string, cause?: Error) {
        super(`Destination shard unavailable: ${shardId}`, {
            code: DESTINATION_UNAVAILABLE,
            transient: true,
            cause,
        });
        this.name = "DestinationUnavailableError";
    }
}

export class ValidationFailedError extends ReshardingError {
    constructor(migrationId: string, failures: { code: string; message: string }[]) {
        const summary = failures.map((f) => `${f.code}: ${f.message}`).join("; ");
        super(`Validation failed for migration "${migrationId}": ${summary}`, {
            code: VALIDATION_FAILED,
            transient: false,
        });
        this.name = "ValidationFailedError";
        this.failures = failures;
    }
    failures: { code: string; message: string }[];
}

export class UnsafeShardRemovalError extends ReshardingError {
    constructor(shardId: string, detail: string) {
        super(`Unsafe shard removal: "${shardId}" — ${detail}`, {
            code: UNSAFE_SHARD_REMOVAL,
            transient: false,
        });
        this.name = "UnsafeShardRemovalError";
    }
}

export class UnrecoverableCutoverError extends ReshardingError {
    constructor(migrationId: string, detail: string) {
        super(`Unrecoverable cutover state for migration "${migrationId}": ${detail}`, {
            code: UNRECOVERABLE_CUTOVER,
            transient: false,
        });
        this.name = "UnrecoverableCutoverError";
    }
}

export class WriteFencedError extends ReshardingError {
    constructor(shardKey: string, migrationId: string) {
        super(`Writes for shard key "${shardKey}" are fenced by migration "${migrationId}"`, {
            code: WRITE_FENCED,
            transient: false,
        });
        this.name = "WriteFencedError";
    }
}

export class MigrationLockedError extends ReshardingError {
    constructor(migrationId: string, holder?: string) {
        super(`Resharding migration "${migrationId}" is locked by another worker${holder ? ` (holder: ${holder})` : ""}`, {
            code: MIGRATION_LOCKED,
            transient: true,
        });
        this.name = "MigrationLockedError";
    }
}

export function isReshardingError(error: unknown): boolean {
    return error instanceof ReshardingError || (error as Record<string, unknown>)?.code === RESHARDING_ERROR;
}

export function isUnsupportedStrategyError(error: unknown): boolean {
    return error instanceof UnsupportedStrategyError || (error as Record<string, unknown>)?.code === UNSUPPORTED_STRATEGY;
}

export function isMissingSchemaMetadataError(error: unknown): boolean {
    return error instanceof MissingSchemaMetadataError || (error as Record<string, unknown>)?.code === MISSING_SCHEMA_METADATA;
}

export function isStalePlanError(error: unknown): boolean {
    return error instanceof StalePlanError || (error as Record<string, unknown>)?.code === STALE_PLAN;
}

export function isInvalidStateTransitionError(error: unknown): boolean {
    return error instanceof InvalidStateTransitionError || (error as Record<string, unknown>)?.code === INVALID_STATE_TRANSITION;
}

export function isMigrationNotFoundError(error: unknown): boolean {
    return error instanceof MigrationNotFoundError || (error as Record<string, unknown>)?.code === MIGRATION_NOT_FOUND;
}

export function isInconsistentRoutingVersionError(error: unknown): boolean {
    return error instanceof InconsistentRoutingVersionError || (error as Record<string, unknown>)?.code === INCONSISTENT_ROUTING_VERSION;
}

export function isDestinationUnavailableError(error: unknown): boolean {
    return error instanceof DestinationUnavailableError || (error as Record<string, unknown>)?.code === DESTINATION_UNAVAILABLE;
}

export function isValidationFailedError(error: unknown): boolean {
    return error instanceof ValidationFailedError || (error as Record<string, unknown>)?.code === VALIDATION_FAILED;
}

export function isUnsafeShardRemovalError(error: unknown): boolean {
    return error instanceof UnsafeShardRemovalError || (error as Record<string, unknown>)?.code === UNSAFE_SHARD_REMOVAL;
}

export function isWriteFencedError(error: unknown): boolean {
    return error instanceof WriteFencedError || (error as Record<string, unknown>)?.code === WRITE_FENCED;
}

export function isMigrationLockedError(error: unknown): boolean {
    return error instanceof MigrationLockedError || (error as Record<string, unknown>)?.code === MIGRATION_LOCKED;
}
