import type { Pool } from "pg";
import type {
    FailoverState,
    ShardFailoverState,
} from "./failover-types.js";
import type {
    FailoverStateStore,
    FailoverContext,
    FailoverStateVersion,
    PersistedShardState,
} from "./failover-state-store.js";

/**
 * Result of a failover state recovery attempt.
 */
export interface RecoveryResult {
    success: boolean;
    state?: ShardFailoverState;
    version: number;
    actions: RecoveryAction[];
    warnings: string[];
    errors: string[];
    durationMs: number;
}

/**
 * A single action recorded during recovery, for auditing and observability.
 */
export interface RecoveryAction {
    type: "lock_acquired" | "lock_released" | "state_loaded" | "state_validated" | "state_reconciled" | "state_restored";
    timestamp: Date;
    description: string;
    details: Record<string, unknown>;
}

/**
 * A validation issue found while checking recovered state integrity.
 */
export interface ValidationIssue {
    type: "corrupted_state" | "stale_state" | "invalid_version" | "unknown_state" | "missing_fields";
    severity: "error" | "warning";
    description: string;
    details: Record<string, unknown>;
}

/**
 * Result of validating a recovered state.
 */
export interface ValidationResult {
    valid: boolean;
    issues: ValidationIssue[];
}

/**
 * Optional recovery behaviour overrides.
 */
export interface FailoverRecoveryOptions {
    /** If true, skip validation errors and restore state anyway. Default: false. */
    forceRecovery?: boolean;
    /** If true, automatically reconcile fixable issues and persist the result. Default: false. */
    autoReconcile?: boolean;
    /** Maximum age in milliseconds for recovered state before flagging as stale. Default: disabled. */
    maxStateAgeMs?: number;
}

/**
 * Recovers failover state from the persistent store after a restart.
 *
 * Responsibilities:
 *  - Acquire a per-shard recovery lock to serialize concurrent recoveries.
 *  - Load and validate persisted state structure and version.
 *  - Optionally auto-reconcile fixable issues and persist the corrected state.
 *  - Release the lock even on failure.
 */
export class FailoverRecoveryManager {
    private readonly validStates: FailoverState[] = [
        "healthy",
        "suspect",
        "primary_unavailable",
        "failover_requested",
        "promoting",
        "verifying",
        "primary_changed",
        "recovering",
        "failed",
    ];

    constructor(
        private readonly stateStore: FailoverStateStore,
        private readonly logger: RequiredFailoverLogger,
        private readonly options: FailoverRecoveryOptions = {},
    ) {}

    /**
     * Recover failover state for a shard. Returns the recovered state on success,
     * or a failure result with errors when recovery cannot proceed safely.
     */
    async recoverState(shardId: string): Promise<RecoveryResult> {
        const startTime = Date.now();
        const actions: RecoveryAction[] = [];
        const warnings: string[] = [];
        const errors: string[] = [];

        const pushAction = (type: RecoveryAction["type"], description: string, details: Record<string, unknown>) => {
            actions.push({ type, timestamp: new Date(), description, details: { shardId, ...details } });
        };

        try {
            // Acquire recovery lock so concurrent instances do not recover the same shard at once.
            pushAction("lock_acquired", "Acquiring recovery lock", {});
            const lockAcquired = await this.stateStore.acquireLock(shardId, 30_000);
            if (!lockAcquired) {
                const err = "Failed to acquire recovery lock — another instance may be recovering";
                errors.push(err);
                return { success: false, version: 0, actions, warnings, errors, durationMs: Date.now() - startTime };
            }

            // Load persisted state.
            pushAction("state_loaded", "Loading failover state", {});
            const state = (await this.stateStore.loadState(shardId)) as PersistedShardState | null;
            if (!state) {
                pushAction("state_restored", "No existing state found", {});
                return {
                    success: true,
                    version: 0,
                    actions,
                    warnings: ["No existing state found"],
                    errors,
                    durationMs: Date.now() - startTime,
                };
            }

            // Validate state structure and version.
            pushAction("state_validated", "Validating failover state", {});
            const validation = this.validateState(state);
            for (const issue of validation.issues) {
                if (issue.severity === "error") errors.push(issue.description);
                else warnings.push(issue.description);
            }

            if (!this.options.forceRecovery && !this.options.autoReconcile && !validation.valid) {
                pushAction("state_restored", "State validation failed", {});
                return { success: false, version: state._version ?? 0, actions, warnings, errors, durationMs: Date.now() - startTime };
            }

            // Optionally auto-reconcile fixable issues and persist.
            if (this.options.autoReconcile && validation.issues.length > 0) {
                const reconciled = this.reconcileState(state, validation);
                if (reconciled) {
                    const saved = await this.stateStore.saveState(shardId, reconciled, state._version ?? 0);
                    pushAction("state_reconciled", "State automatically reconciled", { saved });
                    if (saved) {
                        return {
                            success: true,
                            state: reconciled,
                            version: (state._version ?? 0) + 1,
                            actions,
                            warnings: [...warnings, "State was automatically reconciled"],
                            errors,
                            durationMs: Date.now() - startTime,
                        };
                    }
                }
            }

            pushAction("state_restored", "Failover state restored", {});
            return {
                success: true,
                state,
                version: state._version ?? 0,
                actions,
                warnings,
                errors,
                durationMs: Date.now() - startTime,
            };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            errors.push(message);
            this.logger.error({ error, shardId }, "Recovery failed");
            return { success: false, version: 0, actions, warnings, errors, durationMs: Date.now() - startTime };
        } finally {
            pushAction("lock_released", "Recovery lock released", {});
            await this.stateStore.releaseLock(shardId);
        }
    }

    /**
     * Validate the structure of a recovered state. Returns whether the state is
     * valid along with any issues found.
     */
    validateState(state: PersistedShardState): ValidationResult {
        const issues: ValidationIssue[] = [];

        if (!state.state || !this.validStates.includes(state.state)) {
            issues.push({
                type: "corrupted_state",
                severity: "error",
                description: "State missing or has invalid state value",
                details: { state: state.state, validStates: this.validStates },
            });
        }

        if (!state.shardId || typeof state.shardId !== "string") {
            issues.push({
                type: "corrupted_state",
                severity: "error",
                description: "State missing shardId",
                details: { shardId: state.shardId },
            });
        }

        if (typeof state._version !== "number" || state._version < 0) {
            issues.push({
                type: "invalid_version",
                severity: "error",
                description: "Invalid state version",
                details: { version: state._version },
            });
        }

        if (state.updatedAt) {
            const ageMs = Date.now() - new Date(state.updatedAt).getTime();
            if (this.options.maxStateAgeMs !== undefined && ageMs > this.options.maxStateAgeMs) {
                issues.push({
                    type: "stale_state",
                    severity: "warning",
                    description: "State is older than maxStateAgeMs",
                    details: { ageMs, maxStateAgeMs: this.options.maxStateAgeMs },
                });
            }
        }

        return { valid: issues.every((i) => i.severity !== "error"), issues };
    }

    /**
     * Attempt to reconcile a state with validation issues. Returns the reconciled
     * state if any changes were made, or null if nothing could be fixed.
     */
    reconcileState(state: ShardFailoverState, validation: ValidationResult): ShardFailoverState | null {
        const reconciled: Partial<ShardFailoverState> & { _version?: number; updatedAt?: string } = { ...state };
        let changed = false;

        if (!reconciled.state || !this.validStates.includes(reconciled.state)) {
            if (reconciled.state && reconciled.state !== "failed") {
                reconciled.state = "healthy";
                changed = true;
            }
        }

        if (typeof reconciled._version !== "number") {
            reconciled._version = 0;
            changed = true;
        }

        if (!reconciled.updatedAt) {
            reconciled.updatedAt = new Date().toISOString();
            changed = true;
        }

        return changed ? (reconciled as ShardFailoverState) : null;
    }
}

/**
 * Logger type compatible with FailoverManager's RequiredFailoverLogger.
 * Defined inline here to avoid a circular import.
 */
export type RequiredFailoverLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};
