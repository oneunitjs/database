import type { QueryHooks, QueryContext, MetricsCollector, HooksConfig, NormalizedError } from "../types.js";

const NOOP = () => {};

function now(): number {
    return Number(process.hrtime.bigint() / 1_000_000n);
}

/**
 * Lightweight metrics collector. Deliberately dependency-free and allocation
 * conscious: every finished query mutates a handful of counters.
 */
export function createMetrics(): MetricsCollector {
    const state: Record<string, number> = {
        queries: 0,
        errors: 0,
        slowQueries: 0,
        retries: 0,
        transactions: 0,
        rolledBackTransactions: 0,
        totalDurationMs: 0,
        maxDurationMs: 0,
        lastDurationMs: 0,
    };

    return {
        recordQuery(durationMs = 0) {
            state.queries += 1;
            state.totalDurationMs += durationMs;
            state.lastDurationMs = durationMs;
            if (durationMs > state.maxDurationMs) {
                state.maxDurationMs = durationMs;
            }
        },
        recordError() {
            state.errors += 1;
        },
        recordSlowQuery() {
            state.slowQueries += 1;
        },
        recordRetry() {
            state.retries += 1;
        },
        recordTransaction(rolledBack = false) {
            state.transactions += 1;
            if (rolledBack) {
                state.rolledBackTransactions += 1;
            }
        },
        snapshot() {
            return { ...state };
        },
        reset() {
            for (const key of Object.keys(state)) {
                state[key] = 0;
            }
        },
    };
}

/**
 * Build the instrumentation surface used by the client/transaction layers.
 *
 * Parameter values are never logged unless `logParameters` is explicitly set.
 * Query text is only logged when `logQueries` is enabled, so credentials that
 * may appear in inline SQL are not leaked by default.
 */
export function createHooks({
    logger,
    metrics,
    instrumentation = {},
    ...rest
}: {
    logger?: unknown;
    metrics?: MetricsCollector;
    instrumentation?: Partial<HooksConfig>;
    [key: string]: unknown;
} = {}): QueryHooks {
    const merged = { ...instrumentation, ...rest };
    const config: HooksConfig = {
        logQueries: merged.logQueries ?? false,
        logParameters: merged.logParameters ?? false,
        slowQueryMs: merged.slowQueryMs ?? 0,
        onQuery: merged.onQuery,
        onError: merged.onError,
        onRetry: merged.onRetry,
        ...merged,
    };

    const collector = metrics ?? createMetrics();

    function beforeQuery(context: QueryContext): void {
        context.startedAt = now();
        if (config.logQueries) {
            (logger as { debug?: (meta: object, msg: string) => void })?.debug?.(
                {
                    sql: context.sql,
                    parameters: config.logParameters ? context.parameters : undefined,
                    parameterCount: context.parameters?.length,
                },
                "Database query started",
            );
        }
    }

    function afterQuery(context: QueryContext): void {
        const durationMs = context.startedAt ? now() - context.startedAt : 0;
        collector.recordQuery(durationMs);

        const slowQueryMs = config.slowQueryMs ?? 0;
        if (slowQueryMs > 0 && durationMs >= slowQueryMs) {
            collector.recordSlowQuery();
            (logger as { warn?: (meta: object, msg: string) => void })?.warn?.(
                {
                    durationMs,
                    rowCount: context.rowCount,
                    parameterCount: context.parameters?.length,
                },
                "Slow database query",
            );
        }

        try {
            config.onQuery?.({
                sql: context.sql,
                parameters: config.logParameters ? context.parameters : undefined,
                durationMs,
                rowCount: context.rowCount,
                success: true,
            });
        } catch {
            // Hooks must not affect query execution.
        }
    }

    function onQueryError(context: QueryContext, error: Error): void {
        const durationMs = context.startedAt ? now() - context.startedAt : 0;
        collector.recordError();

        (logger as { error?: (meta: object, msg: string) => void })?.error?.(
            {
                err: error,
                durationMs,
                code: (error as { code?: string }).code,
                parameterCount: context.parameters?.length,
            },
            "Database query failed",
        );

        try {
            config.onError?.({ error, sql: context.sql, durationMs });
        } catch {
            // Hooks must not affect query execution.
        }
    }

    function recordRetry(info: { error: NormalizedError; attempt: number; delay: number; retries: number }): void {
        collector.recordRetry();
        try {
            config.onRetry?.(info);
        } catch {
            // ignore
        }
    }

    return {
        metrics: collector,
        beforeQuery,
        afterQuery,
        onQueryError,
        recordRetry,
        config,
    };
}

export { NOOP };