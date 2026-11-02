// ---------------------------------------------------------------------------
// Shard Rebalance Tracker — progress tracking primitives for shard rebalancing.
// ---------------------------------------------------------------------------

import type {
    ShardRebalanceProgress,
    ShardMapVersion,
} from "../types.js";

function generateRebalanceId(): string {
    return `rebal-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function nowISO(): string {
    return new Date().toISOString();
}

function computePercentage(movedKeys: number, totalKeys: number): number {
    if (totalKeys <= 0) {
        return 100;
    }
    const p = (movedKeys / totalKeys) * 100;
    return Math.min(100, Math.max(0, Math.round(p * 100) / 100));
}

/**
 * In-memory tracker for shard rebalance progress. Provides primitives for
 * creating, updating, querying, and completing rebalance operations. Not
 * durable across process restarts — for production use, wrap with persistent
 * storage.
 */
export class ShardRebalanceTracker {
    private readonly rebalances = new Map<string, ShardRebalanceProgress>();

    createRebalance(
        sourceShardId: string,
        destinationShardId: string,
        totalKeys: number,
    ): ShardRebalanceProgress {
        const rebalanceId = generateRebalanceId();
        const startedAt = nowISO();
        const progress: ShardRebalanceProgress = {
            rebalanceId,
            sourceShardId,
            destinationShardId,
            totalKeys,
            movedKeys: 0,
            failedKeys: 0,
            skippedKeys: 0,
            percentage: 0,
            status: "planning",
            startedAt,
            updatedAt: startedAt,
            errors: [],
        };
        this.rebalances.set(rebalanceId, progress);
        return progress;
    }

    updateProgress(
        rebalanceId: string,
        update: Partial<ShardRebalanceProgress>,
    ): ShardRebalanceProgress | undefined {
        const existing = this.rebalances.get(rebalanceId);
        if (!existing) {
            return undefined;
        }

        const isTerminal =
            existing.status === "completed" ||
            existing.status === "failed" ||
            existing.status === "cancelled";

        if (isTerminal) {
            return existing;
        }

        const next: ShardRebalanceProgress = {
            ...existing,
            ...update,
            rebalanceId,
            updatedAt: nowISO(),
        };

        // Recompute percentage when movedKeys or totalKeys changed.
        if (update.movedKeys !== undefined || update.totalKeys !== undefined) {
            next.percentage = computePercentage(next.movedKeys, next.totalKeys);
        }

        // Auto-transition to completed when all keys moved.
        if (
            next.status !== "completed" &&
            next.status !== "failed" &&
            next.status !== "cancelled" &&
            next.totalKeys > 0 &&
            next.movedKeys + next.failedKeys + next.skippedKeys >= next.totalKeys
        ) {
            next.status = "completed";
        }

        this.rebalances.set(rebalanceId, next);
        return next;
    }

    getRebalance(rebalanceId: string): ShardRebalanceProgress | undefined {
        return this.rebalances.get(rebalanceId);
    }

    listRebalances(): ShardRebalanceProgress[] {
        return [...this.rebalances.values()];
    }

    completeRebalance(rebalanceId: string): ShardRebalanceProgress | undefined {
        const existing = this.rebalances.get(rebalanceId);
        if (!existing) {
            return undefined;
        }
        if (
            existing.status === "completed" ||
            existing.status === "failed" ||
            existing.status === "cancelled"
        ) {
            return existing;
        }
        const next: ShardRebalanceProgress = {
            ...existing,
            status: "completed",
            percentage: 100,
            updatedAt: nowISO(),
        };
        this.rebalances.set(rebalanceId, next);
        return next;
    }

    failRebalance(
        rebalanceId: string,
        error: string,
    ): ShardRebalanceProgress | undefined {
        const existing = this.rebalances.get(rebalanceId);
        if (!existing) {
            return undefined;
        }
        if (
            existing.status === "completed" ||
            existing.status === "failed" ||
            existing.status === "cancelled"
        ) {
            return existing;
        }
        const next: ShardRebalanceProgress = {
            ...existing,
            status: "failed",
            errors: [...existing.errors, error],
            updatedAt: nowISO(),
        };
        this.rebalances.set(rebalanceId, next);
        return next;
    }

    cancelRebalance(rebalanceId: string): ShardRebalanceProgress | undefined {
        const existing = this.rebalances.get(rebalanceId);
        if (!existing) {
            return undefined;
        }
        if (
            existing.status === "completed" ||
            existing.status === "failed" ||
            existing.status === "cancelled"
        ) {
            return existing;
        }
        const next: ShardRebalanceProgress = {
            ...existing,
            status: "cancelled",
            updatedAt: nowISO(),
        };
        this.rebalances.set(rebalanceId, next);
        return next;
    }

    clearCompleted(): number {
        let removed = 0;
        for (const [id, progress] of this.rebalances) {
            if (
                progress.status === "completed" ||
                progress.status === "failed" ||
                progress.status === "cancelled"
            ) {
                this.rebalances.delete(id);
                removed += 1;
            }
        }
        return removed;
    }
}
