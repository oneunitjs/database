import type { Pool } from "pg";
import type { Logger } from "../types.js";

export interface PoolUsageStats {
    poolId: string;
    total: number;
    idle: number;
    waiting: number;
    active: number;
    max: number;
    utilization: number;
    timestamp: Date;
}

export interface PoolMonitorOptions {
    warningThreshold?: number;
    errorThreshold?: number;
    checkIntervalMs?: number;
    logger?: Logger;
}

const DEFAULT_POOL_MONITOR_OPTIONS: Required<PoolMonitorOptions> = {
    warningThreshold: 0.8,
    errorThreshold: 0.95,
    checkIntervalMs: 5000,
    logger: undefined as any,
};

export class PoolMonitor {
    private readonly pools: Map<string, Pool>;
    private readonly stats: Map<string, PoolUsageStats> = new Map();
    private readonly options: Required<PoolMonitorOptions>;
    private readonly listeners: Set<(stats: PoolUsageStats) => void> = new Set();
    private interval?: ReturnType<typeof setInterval>;

    constructor(pools: Map<string, Pool>, options: PoolMonitorOptions = {}) {
        this.pools = pools;
        this.options = { ...DEFAULT_POOL_MONITOR_OPTIONS, ...options } as Required<PoolMonitorOptions>;
    }

    start(): void {
        if (this.interval) return;
        this.interval = setInterval(() => {
            void this.checkAllPools();
        }, this.options.checkIntervalMs);
        void this.checkAllPools();
    }

    stop(): void {
        if (this.interval) {
            clearInterval(this.interval);
            this.interval = undefined;
        }
    }

    private async checkAllPools(): Promise<void> {
        for (const [poolId, pool] of this.pools) {
            const stats = this.getPoolStats(poolId, pool);
            this.stats.set(poolId, stats);
            this.notifyListeners(stats);
            this.checkThresholds(stats);
        }
    }

    private getPoolStats(poolId: string, pool: Pool): PoolUsageStats {
        const total = pool.totalCount ?? 0;
        const idle = pool.idleCount ?? 0;
        const max = (pool as any).options?.max ?? (pool as any).max ?? 20;
        return {
            poolId,
            total,
            idle,
            waiting: pool.waitingCount ?? 0,
            active: total - idle,
            max,
            utilization: max ? (total - idle) / max : 0,
            timestamp: new Date(),
        };
    }

    getStats(poolId: string): PoolUsageStats | undefined {
        return this.stats.get(poolId);
    }

    getAllStats(): PoolUsageStats[] {
        return Array.from(this.stats.values());
    }

    private checkThresholds(stats: PoolUsageStats): void {
        if (stats.utilization >= this.options.errorThreshold) {
            this.options.logger?.error?.({ poolId: stats.poolId, utilization: stats.utilization }, `Pool ${stats.poolId} utilization at ${(stats.utilization * 100).toFixed(1)}% (error threshold)`);
        } else if (stats.utilization >= this.options.warningThreshold) {
            this.options.logger?.warn?.({ poolId: stats.poolId, utilization: stats.utilization }, `Pool ${stats.poolId} utilization at ${(stats.utilization * 100).toFixed(1)}% (warning threshold)`);
        }
    }

    onStats(listener: (stats: PoolUsageStats) => void): () => void {
        this.listeners.add(listener);
        return () => this.listeners.delete(listener);
    }

    private notifyListeners(stats: PoolUsageStats): void {
        for (const listener of this.listeners) {
            try {
                listener(stats);
            } catch (error) {
                this.options.logger?.error?.(`Pool stats listener error: ${error}`);
            }
        }
    }

    getAverageUtilization(): number {
        if (this.stats.size === 0) return 0;
        let total = 0;
        for (const stats of this.stats.values()) total += stats.utilization;
        return total / this.stats.size;
    }

    getMaxUtilization(): number {
        if (this.stats.size === 0) return 0;
        let max = 0;
        for (const stats of this.stats.values()) max = Math.max(max, stats.utilization);
        return max;
    }

    isNearExhaustion(threshold: number = 0.9): boolean {
        for (const stats of this.stats.values()) {
            if (stats.utilization >= threshold) return true;
        }
        return false;
    }
}
