import type { Pool } from "pg";

export interface DynamicPoolResizerOptions {
    logger?: any;
}

export class DynamicPoolResizer {
    constructor(private readonly options: DynamicPoolResizerOptions = {}) {}

    async resizePool(pool: Pool, newSize: number): Promise<void> {
        try {
            (pool as any).options = { ...(pool as any).options, max: newSize };
        } catch {
            // best effort
        }
        this.options.logger?.info?.(`Resized pool to ${newSize}`);
    }

    calculateOptimalSize(maxConcurrent: number, shardCount: number, safetyFactor: number = 1.5): number {
        return Math.ceil(maxConcurrent * shardCount * safetyFactor);
    }
}

export { DynamicPoolResizer as PoolResizer };
