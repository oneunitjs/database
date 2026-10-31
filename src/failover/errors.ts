export class FailoverError extends Error {
    constructor(message: string, public readonly shardId?: string, public readonly cause?: Error) {
        super(message);
        this.name = "FailoverError";
    }
}

export class FailoverNotSupportedError extends FailoverError {
    constructor(shardId?: string) {
        super("Automatic failover is not supported for this shard — no HA provider with promotion support configured", shardId);
        this.name = "FailoverNotSupportedError";
    }
}

export class SplitBrainPreventedError extends FailoverError {
    constructor(shardId?: string, reason?: string) {
        super(`Split-brain prevented for shard "${shardId ?? "unknown"}": ${reason ?? "unsafe topology"}`, shardId);
        this.name = "SplitBrainPreventedError";
    }
}

export class PrimaryNotVerifiedError extends FailoverError {
    constructor(shardId?: string, reason?: string) {
        super(`Primary not verified for shard "${shardId ?? "unknown"}": ${reason ?? "verification failed"}`, shardId);
        this.name = "PrimaryNotVerifiedError";
    }
}

export function isFailoverError(error: unknown): error is FailoverError {
    return error instanceof FailoverError;
}
