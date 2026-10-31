import type { Pool, PoolClient, QueryConfig, QueryResult } from "pg";
import type { ClientApi, QueryHooks, RetryOptions, QueryOptions, PreparedStatement } from "../types.js";
import { isConnectionError, normalizeError, TimeoutError } from "./errors.js";
import { createHooks } from "./hooks.js";
import { withRetry } from "./retry.js";

function isPlainObject(value: unknown): boolean {
    return value !== null && typeof value === "object" && !Array.isArray(value) && !Buffer.isBuffer(value);
}

function isQueryInstance(value: unknown): value is QueryConfig {
    return (
        isPlainObject(value) &&
        typeof (value as { submit?: unknown }).submit === "function" &&
        typeof (value as { text?: unknown }).text === "string"
    );
}

/**
 * Normalize the many supported call shapes into a single `pg` query config.
 *
 *   query(sql, values, options)
 *   query({ text, values }, options)
 *   query({ name, text, values }, options)
 *   query(QueryInstance, options)
 */
function buildQuery(text: unknown, values: unknown, options: unknown): { config: QueryConfig; options: QueryOptions } {
    if (isQueryInstance(text)) {
        return { config: text, options: isPlainObject(values) ? (values as QueryOptions) : (options as QueryOptions ?? {}) };
    }

    if (isPlainObject(text)) {
        const config = { ...(text as Record<string, unknown>) } as unknown as QueryConfig;
        if (Array.isArray(values)) {
            config.values = values;
            return { config, options: (options as QueryOptions) ?? {} };
        }
        return { config, options: isPlainObject(values) ? (values as QueryOptions) : (options as QueryOptions ?? {}) };
    }

    if (Array.isArray(values)) {
        return { config: { text: String(text), values: values as unknown[] }, options: (options as QueryOptions) ?? {} };
    }

    if (isPlainObject(values) && (options === undefined || options === null || (isPlainObject(options) && Object.keys(options).length === 0))) {
        const valuesObj = values as { values?: unknown[] };
        if (valuesObj.values !== undefined) {
            return { config: { text: String(text), values: valuesObj.values ?? [] }, options: values as QueryOptions };
        }
        return { config: { text: String(text), values: [] }, options: values as QueryOptions };
    }

    const vals = (values ?? []) as unknown[];
    return {
        config: { text: String(text), values: vals },
        options: (options as QueryOptions) ?? {},
    };
}

function normalizeRetryOption(option: unknown): RetryOptions | null {
    if (option === true) {
        return {};
    }
    if (typeof option === "number") {
        return { retries: option };
    }
    if (isPlainObject(option)) {
        return option as RetryOptions;
    }
    return null;
}

export function createClient(
    pool: Pool,
    logger: unknown,
    options: {
        hooks?: QueryHooks;
        instrumentation?: Record<string, unknown>;
        retry?: RetryOptions | boolean | number;
        queryTimeout?: number;
        timeout?: number;
        connectTimeout?: number;
    } = {},
): ClientApi {
    const hooks = options.hooks ?? createHooks({ logger, ...(options.instrumentation ?? {}) });
    const configuredRetry = normalizeRetryOption(options.retry);
    const defaultRetry = configuredRetry
        ? { retryOnTimeout: false, ...configuredRetry }
        : null;
    const defaultTimeout = options.queryTimeout ?? options.timeout ?? 0;
    const defaultConnectTimeout = options.connectTimeout ?? 0;

    async function getClient(clientOptions: { timeout?: number } = {}): Promise<PoolClient> {
        const timeoutMs = clientOptions.timeout ?? defaultConnectTimeout;

        if (!timeoutMs) {
            return pool.connect();
        }

        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                reject(new TimeoutError(`Acquiring a connection timed out after ${timeoutMs}ms`, {
                    timeout: timeoutMs,
                }));
            }, timeoutMs);

            pool.connect().then(
                (client) => {
                    clearTimeout(timer);
                    resolve(client);
                },
                (error) => {
                    clearTimeout(timer);
                    reject(error);
                },
            );
        });
    }

    function runQuery(
        client: PoolClient,
        config: QueryConfig,
        timeoutMs: number,
        markDestroy: () => void,
    ): Promise<QueryResult> {
        if (!timeoutMs) {
            return client.query(config);
        }

        return new Promise<QueryResult>((resolve, reject) => {
            let settled = false;
            const timer = setTimeout(() => {
                if (settled) {
                    return;
                }
                settled = true;
                markDestroy();
                reject(new TimeoutError(`Query timed out after ${timeoutMs}ms`, {
                    timeout: timeoutMs,
                }));
            }, timeoutMs);

            const finish = (handler: (value: QueryResult) => void) => (value: QueryResult) => {
                if (settled) {
                    return;
                }
                settled = true;
                clearTimeout(timer);
                handler(value);
            };

            client.query(config).then(
                finish(resolve),
                finish(reject),
            );
        });
    }

    async function executeOnce(config: QueryConfig, queryOptions: QueryOptions): Promise<QueryResult> {
        const client = await getClient(queryOptions);
        const context: {
            sql?: string;
            parameters?: unknown[];
            rowCount?: number;
            shardId?: string;
            routingStrategy?: string;
        } = {
            sql: typeof config?.text === "string" ? config.text : config?.name,
            parameters: config?.values,
            shardId: (queryOptions as { __shardId?: string }).__shardId,
            routingStrategy: (queryOptions as { __routingStrategy?: string }).__routingStrategy,
        };
        let destroy = false;

        hooks.beforeQuery(context);

        let caught: Error | undefined;

        try {
            const result = await runQuery(
                client,
                config,
                queryOptions.timeout ?? defaultTimeout,
                () => {
                    destroy = true;
                },
            );

            context.rowCount = result?.rowCount ?? undefined;
            hooks.afterQuery(context);
            return result;
        } catch (error) {
            caught = error instanceof Error ? error : new Error(String(error));
            hooks.onQueryError(context, normalizeError(error));

            if (queryOptions.destroyOnError !== false && isConnectionError(error)) {
                destroy = true;
            }
            throw error;
        } finally {
            if (destroy) {
                client.release?.(caught ?? new Error("destroying connection"));
            } else {
                client.release?.();
            }
        }
    }

    async function query(text: unknown, values: unknown, queryOptions: QueryOptions = {}): Promise<QueryResult> {
        const { config, options: parsedOptions } = buildQuery(text, values, queryOptions);
        const options_ = parsedOptions ?? {};

        const retryOption =
            normalizeRetryOption(options_.retry) ??
            (options_.retry === false ? null : defaultRetry);

        if (!retryOption) {
            return executeOnce(config, options_);
        }

        return withRetry(
            () => executeOnce(config, options_),
            {
                ...retryOption,
                onRetry: (info) => {
                    hooks.recordRetry(info);
                    retryOption.onRetry?.(info);
                },
            },
        );
    }

    async function queryOne(text: unknown, values: unknown, queryOptions: QueryOptions = {}): Promise<QueryResult["rows"][0] | null> {
        const result = await query(text, values, queryOptions);
        return result?.rows?.[0] ?? null;
    }

    function releaseClient(client: PoolClient | undefined, error: Error | undefined): void {
        if (!client) {
            return;
        }
        if (error) {
            client.release?.(error);
        } else {
            client.release?.();
        }
    }

    /**
     * Create a named prepared-statement handle. PostgreSQL caches the parsed
     * statement per connection, so the handle is safe to reuse across calls.
     */
    function prepare(name: string, text: string): PreparedStatement {
        if (!name) {
            throw new TypeError("prepare(name, text) requires a statement name");
        }
        if (typeof text !== "string" || text.length === 0) {
            throw new TypeError("prepare(name, text) requires a SQL string");
        }

        return {
            name,
            text,
            execute(values: unknown[], queryOptions: QueryOptions = {}) {
                return query({ name, text, values }, queryOptions);
            },
        };
    }

    function prepared(name: string, text: string, values: unknown[], queryOptions: QueryOptions = {}): Promise<QueryResult> {
        return query({ name, text, values }, queryOptions);
    }

    return {
        getClient,
        query,
        queryOne,
        releaseClient,
        prepare,
        prepared,
        hooks,
    };
}