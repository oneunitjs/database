import type { Pool } from "pg";
import type { PoolUsageStats } from "./pool-monitor.js";

export interface PoolStatistics {
    poolId: string;
    total: number;
    idle: number;
    waiting: number;
    active: number;
    max: number;
    utilization: number;
    timestamp: Date;
}

export class PoolStats {
    constructor(private readonly pool: Pool, private readonly poolId: string = "pool") {}

    getStats(): PoolStatistics {
        const total = (this.pool as any).totalCount ?? 0;
        const idle = (this.pool as any).idleCount ?? 0;
        const waiting = (this.pool as any).waitingCount ?? 0;
        const max = (this.pool as any).options?.max ?? (this.pool as any).max ?? 10;
        return {
            poolId: this.poolId,
            total,
            idle,
            waiting,
            active: total - idle,
            max,
            utilization: max ? (total - idle) / max : 0,
            timestamp: new Date(),
        };
    }

    snapshot(): PoolUsageStats {
        const s = this.getStats();
        return { ...s } as PoolUsageStats;
    }
}

export { PoolStats as PoolStatisticsCollector };
export { PoolStats as PoolStatsCollector };
