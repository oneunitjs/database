import type { Pool } from "pg";
import type { PoolMonitor } from "./pool-monitor.js";

export interface PoolExhaustionOptions {
    threshold?: number;
    logger?: any;
}

export class PoolExhaustionDetector {
    private readonly monitor?: PoolMonitor;
    private readonly pool?: Pool;
    private readonly threshold: number;

    constructor(target: PoolMonitor | Pool | any, options: PoolExhaustionOptions = {}) {
        if (target && typeof (target as any).isNearExhaustion === "function") {
            this.monitor = target as PoolMonitor;
        } else if (target && typeof (target as any).query === "function") {
            this.pool = target as Pool;
        } else {
            this.monitor = target as PoolMonitor;
        }
        this.threshold = options.threshold ?? 0.9;
    }

    isExhausted(threshold: number = this.threshold): boolean {
        if (this.monitor) return this.monitor.isNearExhaustion(threshold);
        if (this.pool) {
            const total = (this.pool as any).totalCount ?? 0;
            const idle = (this.pool as any).idleCount ?? 0;
            const max = (this.pool as any).options?.max ?? 10;
            return max > 0 && (total - idle) / max >= threshold;
        }
        return false;
    }

    isNearExhaustion(threshold: number = this.threshold): boolean {
        return this.isExhausted(threshold);
    }

    getStatus() {
        return { threshold: this.threshold, exhausted: this.isExhausted() };
    }
}

export { PoolExhaustionDetector as PoolExhaustionMonitor };
