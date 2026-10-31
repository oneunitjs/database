// ---------------------------------------------------------------------------
// Per-shard health checking with connectivity/readiness/replication dimensions.
// ---------------------------------------------------------------------------

import type { PoolStats, ShardHealthCheckResult, ShardHealthResult, Logger } from "../types.js";
import { createCheck } from "../client/check.js";
import { ShardManager } from "./shard-manager.js";

export interface ShardHealthOptions {
    timeout?: number;
}

export class ShardHealth {
    private checks = new Map<string, ReturnType<typeof createCheck>>;

    constructor(
        private readonly manager: ShardManager,
        private readonly logger?: Logger,
    ) {}

    /**
     * Get or create a health check for a shard's pool.
     * Returns undefined if the shard doesn't exist or has no pool.
     */
    private getCheck(shardId: string): ReturnType<typeof createCheck> | undefined {
        let check = this.checks.get(shardId);
        if (!check) {
            const shard = this.manager.getShard(shardId);
            if (shard?.pool) {
                check = createCheck(shard.pool);
                this.checks.set(shardId, check);
            }
        }
        return check;
    }

    /**
     * Remove a shard's health check from the cache.
     * Called when a shard is removed via removeShard().
     */
    unregisterShard(shardId: string): void {
        this.checks.delete(shardId);
    }

    /**
     * Check a single shard's health. Returns undefined if the shard is unknown
     * or its pool is not available.
     *
     * Connectivity = can we reach the shard (SELECT 1 succeeds).
     * Readiness = shard is in ACTIVE state and not draining/removed.
     * Replication = lag info from replication manager (when available).
     */
    async shardHealth(shardId: string): Promise<ShardHealthCheckResult | undefined> {
        const check = this.getCheck(shardId);
        const state = this.manager.getShard(shardId);

        if (!check || !state) {
            return undefined;
        }

        const shardState = this.manager.getLifecycleState(shardId) ?? "ACTIVE";

        const start = Date.now();
        let connectivity: "healthy" | "unhealthy" | "unknown" = "unknown";
        let latencyMs = 0;
        let error: string | undefined;
        let code: string | undefined;

        try {
            const checkResult = await check.check({ timeout: undefined });
            connectivity = checkResult.status === "up" ? "healthy" : "unhealthy";
            latencyMs = checkResult.latency.value;
            error = checkResult.error;
            code = checkResult.code;
        } catch (err) {
            connectivity = "unhealthy";
            latencyMs = Date.now() - start;
            error = err instanceof Error ? err.message : String(err);
            code = (err as { code?: string }).code;
            this.manager.recordError(
                shardId,
                err instanceof Error ? err : new Error(String(err)),
                latencyMs,
            );
        }

        const readiness =
            shardState === "ACTIVE" && connectivity === "healthy" ? "ready" : "not-ready";

        if (connectivity === "healthy") {
            this.manager.recordSuccess(shardId, latencyMs);
        } else if (error) {
            this.manager.recordError(
                shardId,
                new Error(error),
                latencyMs,
            );
        }

        const result: ShardHealthCheckResult = {
            shardId,
            connectivity,
            readiness,
            status: connectivity === "healthy" && readiness === "ready" ? "up" : "down",
            latency: { value: latencyMs, unit: "ms" },
            pool: state.pool
                ? {
                      total: state.pool.totalCount,
                      idle: state.pool.idleCount,
                      waiting: state.pool.waitingCount,
                  }
                : undefined,
            error,
            code,
            checkedAt: new Date().toISOString(),
            consecutiveFailures: state.consecutiveFailures,
        };

        return result;
    }

    /**
     * Check all shards' health. Always resolves; down shards are reported
     * in the result rather than throwing.
     */
    async health(): Promise<ShardHealthResult> {
        const checkedAt = new Date().toISOString();
        const results: ShardHealthCheckResult[] = [];
        const downShards: string[] = [];
        const upShards: string[] = [];

        for (const shardId of this.manager.shardIds) {
            const result = await this.shardHealth(shardId);
            if (!result) {
                continue;
            }
            results.push(result);
            if (result.readiness === "not-ready" || result.connectivity === "unhealthy") {
                downShards.push(shardId);
            } else {
                upShards.push(shardId);
            }
        }

        return {
            healthy: downShards.length === 0,
            checkedAt,
            shards: results,
            downShards,
            upShards,
        };
    }

    /**
     * Check if a specific shard is currently healthy (connectivity + readiness).
     */
    async isShardHealthy(shardId: string): Promise<boolean> {
        const result = await this.shardHealth(shardId);
        return result ? result.readiness === "ready" : false;
    }

    /**
     * Get the number of consecutive failures for a shard (from manager state).
     */
    getConsecutiveFailures(shardId: string): number {
        const state = this.manager.getShard(shardId);
        return state?.consecutiveFailures ?? 0;
    }

    /**
     * Get the last error for a shard (from manager state).
     */
    getLastError(shardId: string): string | undefined {
        const state = this.manager.getShard(shardId);
        return state?.lastError;
    }

    /**
     * Get the last check timestamp for a shard (from manager state).
     */
    getLastCheckedAt(shardId: string): string | undefined {
        const state = this.manager.getShard(shardId);
        return state?.lastCheckedAt;
    }
}
