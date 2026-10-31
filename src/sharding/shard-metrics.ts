// ---------------------------------------------------------------------------
// Shard Metrics — per-shard and routing observability.
//
// Tracks query counts, errors, latency, and routing decisions per shard.
// Uses bounded-cardinality labels (shardId only, never tenant/shard keys).
// ---------------------------------------------------------------------------

import type { MetricsCollector, QueryContext } from "../types.js";
import { createMetrics } from "../client/hooks.js";

interface ShardMetricState {
    queries: number;
    errors: number;
    slowQueries: number;
    retries: number;
    transactions: number;
    rolledBackTransactions: number;
    totalDurationMs: number;
    maxDurationMs: number;
    lastDurationMs: number;
}

interface RoutingMetricState {
    totalRoutes: number;
    defaultRouteUsage: number;
    missingShardKey: number;
    unknownShardKey: number;
    crossShardViolations: number;
    strategyUsage: Record<string, number>;
}

export interface ShardMetricsSnapshot {
    byShard: Record<string, {
        queries: number;
        errors: number;
        slowQueries: number;
        retries: number;
        transactions: number;
        rolledBackTransactions: number;
        totalDurationMs: number;
        maxDurationMs: number;
        lastDurationMs: number;
        avgDurationMs: number;
    }>;
    routing: {
        totalRoutes: number;
        defaultRouteUsage: number;
        missingShardKey: number;
        unknownShardKey: number;
        crossShardViolations: number;
        strategyUsage: Record<string, number>;
    };
}

/**
 * ShardMetricsCollector — aggregates metrics per shard and for routing
 * decisions. Each shard gets its own MetricsCollector instance.
 */
export function createShardMetrics(): {
    recordQuery(context: QueryContext, durationMs: number): void;
    recordError(context: QueryContext): void;
    recordSlowQuery(context: QueryContext): void;
    recordRetry(context: QueryContext): void;
    recordTransaction(context: QueryContext, rolledBack?: boolean): void;
    recordRoutingDecision({
        shardKey,
        resolvedShardId,
        strategy,
        usedDefault,
        missingKey,
        unknownKey,
        crossShardViolation,
    }: {
        shardKey?: string;
        resolvedShardId: string | null;
        strategy: string | undefined;
        usedDefault?: boolean;
        missingKey?: boolean;
        unknownKey?: boolean;
        crossShardViolation?: boolean;
    }): void;
    snapshot(): ShardMetricsSnapshot;
    forShard(shardId: string): MetricsCollector | undefined;
    reset(): void;
} {
    const shardCollectors = new Map<string, MetricsCollector>();
    const routing = {
        totalRoutes: 0,
        defaultRouteUsage: 0,
        missingShardKey: 0,
        unknownShardKey: 0,
        crossShardViolations: 0,
        strategyUsage: {} as Record<string, number>,
    };

    function getOrCreate(shardId: string): MetricsCollector {
        let collector = shardCollectors.get(shardId);
        if (!collector) {
            collector = createMetrics();
            shardCollectors.set(shardId, collector);
        }
        return collector;
    }

    function recordQuery(context: QueryContext, durationMs: number): void {
        const shardId = context.shardId ?? "primary";
        getOrCreate(shardId).recordQuery(durationMs);
    }

    function recordError(context: QueryContext): void {
        const shardId = context.shardId ?? "primary";
        getOrCreate(shardId).recordError();
    }

    function recordSlowQuery(context: QueryContext): void {
        const shardId = context.shardId ?? "primary";
        getOrCreate(shardId).recordSlowQuery();
    }

    function recordRetry(context: QueryContext): void {
        const shardId = context.shardId ?? "primary";
        getOrCreate(shardId).recordRetry();
    }

    function recordTransaction(context: QueryContext, rolledBack = false): void {
        const shardId = context.shardId ?? "primary";
        getOrCreate(shardId).recordTransaction(rolledBack);
    }

    function recordRoutingDecision({
        shardKey,
        resolvedShardId,
        strategy,
        usedDefault = false,
        missingKey = false,
        unknownKey = false,
        crossShardViolation = false,
    }: {
        shardKey?: string;
        resolvedShardId: string | null;
        strategy: string | undefined;
        usedDefault?: boolean;
        missingKey?: boolean;
        unknownKey?: boolean;
        crossShardViolation?: boolean;
    }): void {
        routing.totalRoutes += 1;
        if (usedDefault) routing.defaultRouteUsage += 1;
        if (missingKey) routing.missingShardKey += 1;
        if (unknownKey) routing.unknownShardKey += 1;
        if (crossShardViolation) routing.crossShardViolations += 1;
        if (strategy) {
            routing.strategyUsage[strategy] = (routing.strategyUsage[strategy] ?? 0) + 1;
        }
    }

    function snapshot(): ShardMetricsSnapshot {
        const byShard: Record<string, any> = {};
        for (const [shardId, collector] of shardCollectors) {
            const snap = collector.snapshot();
            const queries = snap.queries ?? 0;
            const totalDurationMs = snap.totalDurationMs ?? 0;
            byShard[shardId] = {
                queries,
                errors: snap.errors ?? 0,
                slowQueries: snap.slowQueries ?? 0,
                retries: snap.retries ?? 0,
                transactions: snap.transactions ?? 0,
                rolledBackTransactions: snap.rolledBackTransactions ?? 0,
                totalDurationMs,
                maxDurationMs: snap.maxDurationMs ?? 0,
                lastDurationMs: snap.lastDurationMs ?? 0,
                avgDurationMs: queries > 0 ? totalDurationMs / queries : 0,
            };
        }
        return {
            byShard,
            routing: { ...routing },
        };
    }

    function forShard(shardId: string): MetricsCollector | undefined {
        return shardCollectors.get(shardId);
    }

    function reset(): void {
        shardCollectors.clear();
        routing.totalRoutes = 0;
        routing.defaultRouteUsage = 0;
        routing.missingShardKey = 0;
        routing.unknownShardKey = 0;
        routing.crossShardViolations = 0;
        routing.strategyUsage = {};
    }

    return {
        recordQuery,
        recordError,
        recordSlowQuery,
        recordRetry,
        recordTransaction,
        recordRoutingDecision,
        snapshot,
        forShard,
        reset,
    };
}
