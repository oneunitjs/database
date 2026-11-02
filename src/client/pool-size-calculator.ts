export function calculateRecommendedPoolSize(maxConcurrentTx: number, shardCount: number, safetyFactor: number = 1.5): number {
    return Math.ceil(maxConcurrentTx * shardCount * safetyFactor);
}
