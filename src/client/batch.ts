import type { PoolClient, QueryResult } from "pg";
import type {
    InsertManyOptions,
    BatchApi,
    QueryOptions,
    BatchInsertResult,
    BatchUpdateDeleteResult,
    BatchSelectResult,
    BatchStats,
    BatchProgressEvent,
    BatchRowError,
    RetryOptions,
} from "../types.js";
import { quoteIdent, quoteIdentList, isFragment, sql, raw } from "../query/sql.js";
import { QueryBuilder } from "../query/builder.js";

const DEFAULT_CHUNK_SIZE = 500;
const MAX_PARAMETERS = 65_535;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BASE_DELAY_MS = 500;
const DEFAULT_MAX_DELAY_MS = 30_000;

function inferColumns(rows: Record<string, unknown>[]): string[] {
    const columns: string[] = [];
    const seen = new Set<string>();

    for (const row of rows) {
        for (const key of Object.keys(row)) {
            if (!seen.has(key)) {
                seen.add(key);
                columns.push(key);
            }
        }
    }

    return columns;
}

function buildConflictClause(onConflict: InsertManyOptions["onConflict"]): string {
    if (!onConflict) {
        return "";
    }

    if (onConflict === true || onConflict === "nothing") {
        return "ON CONFLICT DO NOTHING";
    }

    if (typeof onConflict === "string") {
        throw new TypeError(
            "onConflict must be an object, true, or 'nothing' (raw SQL is not accepted)",
        );
    }

    const parts = ["ON CONFLICT"];

    if (onConflict.constraint) {
        parts.push(`ON CONSTRAINT ${quoteIdent(onConflict.constraint)}`);
    } else if (onConflict.columns?.length) {
        parts.push(`(${quoteIdentList(onConflict.columns)})`);
    }

    if (onConflict.do === "nothing" || onConflict.action === "nothing") {
        parts.push("DO NOTHING");
    } else if (onConflict.update?.length || onConflict.columns?.length) {
        const updateColumns = onConflict.update?.length
            ? onConflict.update
            : onConflict.columns!.filter((column) => column !== onConflict.id);
        const assignments = updateColumns
            .map((column) => `${quoteIdent(column)} = EXCLUDED.${quoteIdent(column)}`)
            .join(", ");
        parts.push(`DO UPDATE SET ${assignments}`);
    } else {
        parts.push("DO NOTHING");
    }

    return parts.join(" ");
}

function resolveRetry(options: RetryOptions | boolean | number | undefined): {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs: number;
} {
    if (options === false || options === 0) {
        return { maxRetries: 0, baseDelayMs: 0, maxDelayMs: 0 };
    }
    if (options === true || options === undefined) {
        return { maxRetries: DEFAULT_MAX_RETRIES, baseDelayMs: DEFAULT_BASE_DELAY_MS, maxDelayMs: DEFAULT_MAX_DELAY_MS };
    }
    if (typeof options === "number") {
        return { maxRetries: options, baseDelayMs: DEFAULT_BASE_DELAY_MS, maxDelayMs: DEFAULT_MAX_DELAY_MS };
    }
    return {
        maxRetries: options.maxRetries ?? DEFAULT_MAX_RETRIES,
        baseDelayMs: options.baseDelay ?? DEFAULT_BASE_DELAY_MS,
        maxDelayMs: options.maxDelay ?? DEFAULT_MAX_DELAY_MS,
    };
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function checkAborted(signal: AbortSignal | undefined): void {
    if (signal?.aborted) {
        const err = new DOMException("The operation was aborted.", "AbortError");
        throw err;
    }
}

function makeStats(startTime: number): BatchStats {
    return {
        chunks: 0,
        rowsProcessed: 0,
        rowsSucceeded: 0,
        rowsFailed: 0,
        durationMs: 0,
        errors: [],
    };
}

function finalizeStats(stats: BatchStats, startTime: number): BatchStats {
    stats.durationMs = Date.now() - startTime;
    return stats;
}

function progressEvent(
    operation: BatchProgressEvent["operation"],
    chunkIndex: number,
    totalChunks: number,
    rowsProcessed: number,
    rowsSucceeded: number,
    rowsFailed: number,
    durationMs: number,
): BatchProgressEvent {
    return {
        operation,
        chunkIndex,
        totalChunks,
        rowsProcessed,
        rowsSucceeded,
        rowsFailed,
        durationMs,
    };
}

/**
 * Bulk helpers built on parameterized multi-row statements. They never
 * interpolate row values into SQL text and they chunk automatically so the
 * statement stays under PostgreSQL's parameter limit.
 */
export function createBatch({
    query,
    transaction,
}: {
    query: (text: string, params: unknown[], options?: Record<string, unknown>) => Promise<QueryResult>;
    transaction?: <T>(callback: (client: PoolClient) => Promise<T> | T) => Promise<T>;
}): BatchApi {
    async function runChunk(
        sqlText: string,
        params: unknown[],
        options: InsertManyOptions = {},
    ): Promise<QueryResult> {
        if (options.client) {
            return options.client.query(sqlText, params);
        }
        const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
        if (options.shardKey && mergedOptions.shardKey === undefined) {
            mergedOptions.shardKey = options.shardKey;
        }
        if (options.shardId && mergedOptions.shardId === undefined) {
            mergedOptions.shardId = options.shardId;
        }
        return query(sqlText, params, mergedOptions);
    }

    async function runWithRetry<T>(
        fn: () => Promise<T>,
        retry: { maxRetries: number; baseDelayMs: number; maxDelayMs: number },
        signal: AbortSignal | undefined,
        context: string,
    ): Promise<T> {
        let lastError: unknown;
        for (let attempt = 0; attempt <= retry.maxRetries; attempt++) {
            checkAborted(signal);
            try {
                return await fn();
            } catch (error) {
                lastError = error;
                if (attempt >= retry.maxRetries) {
                    break;
                }
                checkAborted(signal);
                const delay = Math.min(
                    retry.baseDelayMs * Math.pow(2, attempt),
                    retry.maxDelayMs,
                );
                await sleep(delay);
            }
        }
        throw lastError;
    }

    async function insertMany(
        table: string,
        rows: Record<string, unknown>[],
        options: InsertManyOptions = {},
    ): Promise<BatchInsertResult> {
        const startTime = Date.now();
        const stats: BatchStats = makeStats(startTime);
        const errors: BatchRowError[] = [];

        if (!Array.isArray(rows) || rows.length === 0) {
            return {
                rows: [],
                rowCount: 0,
                stats: finalizeStats(stats, startTime),
            };
        }

        const columns = options.columns?.length ? options.columns : inferColumns(rows);

        if (columns.length === 0) {
            throw new TypeError("insertMany requires at least one column");
        }

        const returning = options.returning
            ? `RETURNING ${Array.isArray(options.returning) ? quoteIdentList(options.returning) : options.returning}`
            : "";
        const conflict = buildConflictClause(options.onConflict);

        const requestedChunk = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const chunkSize = Math.max(1, Math.min(requestedChunk, Math.floor(MAX_PARAMETERS / columns.length)));

        const columnList = quoteIdentList(columns);
        const collected: unknown[] = [];
        let rowsSucceeded = 0;
        let rowsFailed = 0;

        const executeChunk = async (
            chunk: Record<string, unknown>[],
            chunkIndex: number,
            totalChunks: number,
            chunkOptions: InsertManyOptions = options,
        ): Promise<QueryResult> => {
            const params: unknown[] = [];
            const tuples = chunk.map((row) => {
                const tuple = columns.map((column) => {
                    params.push(row[column] ?? null);
                    return `$${params.length}`;
                });
                return `(${tuple.join(", ")})`;
            });

            const text = [
                `INSERT INTO ${quoteIdent(table)} (${columnList})`,
                `VALUES ${tuples.join(", ")}`,
                conflict,
                returning,
            ]
                .filter(Boolean)
                .join(" ");

            const chunkStart = Date.now();
            let chunkSucceeded = 0;
            let chunkFailed = 0;

            const result = await runWithRetry(
                () => runChunk(text, params, chunkOptions),
                resolveRetry(options.retry),
                options.signal,
                "insert",
            );

            if (returning) {
                collected.push(...(result?.rows ?? []));
                chunkSucceeded = result.rows?.length ?? 0;
            } else {
                chunkSucceeded = result.rowCount ?? chunk.length;
            }

            stats.chunks++;
            stats.rowsProcessed += chunk.length;
            stats.rowsSucceeded += chunkSucceeded;
            stats.rowsFailed += chunkFailed;

            options.onProgress?.(
                progressEvent("insert", chunkIndex, totalChunks, chunk.length, chunkSucceeded, chunkFailed, Date.now() - chunkStart),
            );

            return result;
        };

        const totalChunks = Math.ceil(rows.length / chunkSize);

        if (options.transaction && transaction) {
            await transaction(async (client: PoolClient) => {
                let savepointIndex = 0;
                for (let index = 0; index < rows.length; index += chunkSize) {
                    checkAborted(options.signal);
                    const chunk = rows.slice(index, index + chunkSize);
                    const chunkIndex = Math.floor(index / chunkSize);

                    if (options.savepoint) {
                        await client.query(`SAVEPOINT batch_sp_${savepointIndex++}`);
                    }

                    try {
                        await executeChunk(chunk, chunkIndex, totalChunks, { ...options, client });
                    } catch (error) {
                        if (options.savepoint) {
                            await client.query(`ROLLBACK TO SAVEPOINT batch_sp_${savepointIndex - 1}`);
                            await client.query(`RELEASE SAVEPOINT batch_sp_${savepointIndex - 1}`);
                        }
                        const failedRow: BatchRowError = {
                            rowIndex: index,
                            row: chunk[0] ?? {},
                            error,
                        };
                        errors.push(failedRow);
                        stats.rowsFailed += chunk.length;
                        options.onRowError?.(chunk[0] ?? {}, error, index);
                        if (!options.savepoint) {
                            throw error;
                        }
                    }
                }
            });
        } else {
            for (let index = 0; index < rows.length; index += chunkSize) {
                checkAborted(options.signal);
                const chunk = rows.slice(index, index + chunkSize);
                const chunkIndex = Math.floor(index / chunkSize);
                await executeChunk(chunk, chunkIndex, totalChunks);
            }
        }

        return {
            rows: returning ? collected : [],
            rowCount: returning ? collected.length : rows.length,
            stats: finalizeStats(stats, startTime),
        };
    }

    /**
     * Batch SELECT with chunked execution.
     * Supports both OFFSET-based and keyset-based pagination.
     */
    async function selectMany(
        builderOrSql: QueryBuilder | { text: string; values: unknown[] } | string,
        options: {
            chunkSize?: number;
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            client?: PoolClient;
            onChunk?: (rows: unknown[]) => Promise<void> | void;
            keysetColumn?: string;
            lastKeysetValue?: unknown;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        } = {},
    ): Promise<BatchSelectResult> {
        const startTime = Date.now();
        const stats: BatchStats = makeStats(startTime);

        let sqlText: string;
        let params: unknown[] = [];

        if (builderOrSql instanceof QueryBuilder) {
            ({ text: sqlText, values: params } = builderOrSql.toSQL());
        } else if (typeof builderOrSql === "string") {
            sqlText = builderOrSql;
        } else {
            sqlText = builderOrSql.text;
            params = builderOrSql.values;
        }

        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const collected: unknown[] = [];
        const retry = resolveRetry(options.retry ?? false);

        let offset = 0;
        let lastKeysetValue = options.lastKeysetValue;
        let chunkIndex = 0;
        let hasMore = true;

        const executeChunk = async (
            paginatedText: string,
            chunkParams: unknown[],
            idx: number,
            total: number,
        ): Promise<unknown[]> => {
            const chunkStart = Date.now();
            let chunkRows: unknown[] = [];

            const result = await runWithRetry(
                () => {
                    const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
                    if (options.shardKey && mergedOptions.shardKey === undefined) {
                        mergedOptions.shardKey = options.shardKey;
                    }
                    if (options.shardId && mergedOptions.shardId === undefined) {
                        mergedOptions.shardId = options.shardId;
                    }
                    if (options.client) {
                        return options.client.query(paginatedText, chunkParams);
                    }
                    return query(paginatedText, chunkParams, mergedOptions);
                },
                retry,
                options.signal,
                "select",
            );

            chunkRows = result.rows ?? [];
            stats.chunks++;
            stats.rowsProcessed += chunkRows.length;
            stats.rowsSucceeded += chunkRows.length;

            options.onProgress?.(
                progressEvent("select", idx, total, chunkRows.length, chunkRows.length, 0, Date.now() - chunkStart),
            );

            return chunkRows;
        };

        while (hasMore) {
            checkAborted(options.signal);

            let paginatedText: string;
            let chunkParams: unknown[];

            if (options.keysetColumn) {
                const whereClause = lastKeysetValue !== undefined
                    ? ` WHERE ${quoteIdent(options.keysetColumn)} > $${params.length + 1}`
                    : "";
                paginatedText = `${sqlText}${whereClause} ORDER BY ${quoteIdent(options.keysetColumn)} LIMIT ${chunkSize}`;
                chunkParams = lastKeysetValue !== undefined ? [...params, lastKeysetValue] : [...params];
            } else {
                paginatedText = `${sqlText} LIMIT ${chunkSize} OFFSET ${offset}`;
                chunkParams = [...params];
            }

            const rows = await executeChunk(paginatedText, chunkParams, chunkIndex, Math.ceil(collected.length / chunkSize) + 1);

            if (rows.length === 0) {
                hasMore = false;
            } else {
                if (options.onChunk) {
                    await options.onChunk(rows);
                }
                collected.push(...rows);

                if (options.keysetColumn) {
                    const lastRow = rows[rows.length - 1] as Record<string, unknown>;
                    lastKeysetValue = lastRow[options.keysetColumn];
                } else {
                    offset += chunkSize;
                }
                hasMore = rows.length >= chunkSize;
                chunkIndex++;
            }
        }

        return {
            rows: collected,
            rowCount: collected.length,
            stats: finalizeStats(stats, startTime),
        };
    }

    /**
     * Batch UPDATE with chunked execution and transaction support.
     */
    async function updateMany(
        table: string,
        values: Record<string, unknown>,
        whereConditions: {
            column: string;
            operator: string;
            values: unknown[];
        }[],
        options: {
            chunkSize?: number;
            returning?: string[];
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            client?: PoolClient;
            transaction?: boolean;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onRowError?: (row: Record<string, unknown>, error: unknown, rowIndex: number) => void;
            savepoint?: boolean;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        } = {},
    ): Promise<BatchUpdateDeleteResult> {
        const startTime = Date.now();
        const stats: BatchStats = makeStats(startTime);
        const errors: BatchRowError[] = [];

        const columns = Object.keys(values);
        if (columns.length === 0) {
            throw new TypeError("updateMany requires at least one column to update");
        }

        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const collected: unknown[] = [];
        const retry = resolveRetry(options.retry ?? false);
        const valueChunk = columns.map((col) => values[col]);

        const conditionCount = whereConditions.length;

        const executeChunk = async (
            conditionChunk: unknown[],
            idx: number,
            total: number,
        ): Promise<QueryResult> => {
            const params: unknown[] = [...valueChunk, ...conditionChunk];

            const setClause = columns.map((col, i) => `${quoteIdent(col)} = $${i + 1}`).join(", ");

            const whereClauses = whereConditions.map((cond, idx) => {
                const valueOffset = columns.length + idx * 2;
                const placeholder = `$${valueOffset + 1}`;
                return `${quoteIdent(cond.column)} ${cond.operator} ${placeholder}`;
            }).join(" AND ");

            const returning = options.returning
                ? `RETURNING ${quoteIdentList(options.returning)}`
                : "";

            const text = `UPDATE ${quoteIdent(table)} SET ${setClause} WHERE ${whereClauses} ${returning}`;

            const chunkStart = Date.now();
            let chunkSucceeded = 0;

            const result = await runWithRetry(
                () => {
                    const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
                    if (options.shardKey && mergedOptions.shardKey === undefined) {
                        mergedOptions.shardKey = options.shardKey;
                    }
                    if (options.shardId && mergedOptions.shardId === undefined) {
                        mergedOptions.shardId = options.shardId;
                    }
                    if (options.client) {
                        return options.client.query(text, params);
                    }
                    return query(text, params, mergedOptions);
                },
                retry,
                options.signal,
                "update",
            );

            if (returning) {
                collected.push(...(result.rows ?? []));
                chunkSucceeded = result.rows?.length ?? 0;
            } else {
                chunkSucceeded = result.rowCount ?? 0;
            }

            stats.chunks++;
            stats.rowsProcessed += 1;
            stats.rowsSucceeded += chunkSucceeded;

            options.onProgress?.(
                progressEvent("update", idx, total, 1, chunkSucceeded, 0, Date.now() - chunkStart),
            );

            return result;
        };

        const totalChunks = Math.ceil(conditionCount / chunkSize);

        const runChunkWithClient = async (
            client: PoolClient,
            conditionChunk: unknown[],
            idx: number,
            total: number,
        ): Promise<QueryResult> => {
            const params: unknown[] = [...valueChunk, ...conditionChunk];

            const setClause = columns.map((col, i) => `${quoteIdent(col)} = $${i + 1}`).join(", ");

            const whereClauses = whereConditions.map((cond, idx) => {
                const valueOffset = columns.length + idx * 2;
                const placeholder = `$${valueOffset + 1}`;
                return `${quoteIdent(cond.column)} ${cond.operator} ${placeholder}`;
            }).join(" AND ");

            const returning = options.returning
                ? `RETURNING ${quoteIdentList(options.returning)}`
                : "";

            const text = `UPDATE ${quoteIdent(table)} SET ${setClause} WHERE ${whereClauses} ${returning}`;

            const chunkStart = Date.now();
            let chunkSucceeded = 0;

            const result = await runWithRetry(
                () => client.query(text, params),
                retry,
                options.signal,
                "update",
            );

            if (returning) {
                collected.push(...(result.rows ?? []));
                chunkSucceeded = result.rows?.length ?? 0;
            } else {
                chunkSucceeded = result.rowCount ?? 0;
            }

            stats.chunks++;
            stats.rowsProcessed += 1;
            stats.rowsSucceeded += chunkSucceeded;

            options.onProgress?.(
                progressEvent("update", idx, total, 1, chunkSucceeded, 0, Date.now() - chunkStart),
            );

            return result;
        };

        if (options.transaction && transaction) {
            let savepointIndex = 0;
            await transaction(async (client: PoolClient) => {
                for (let index = 0; index < conditionCount; index += chunkSize) {
                    checkAborted(options.signal);
                    const conditionChunk = whereConditions.slice(index, index + chunkSize).flatMap((cond) => cond.values);
                    const chunkIndex = Math.floor(index / chunkSize);

                    if (options.savepoint) {
                        await client.query(`SAVEPOINT batch_sp_${savepointIndex++}`);
                    }

                    try {
                        await runChunkWithClient(client, conditionChunk, chunkIndex, totalChunks);
                    } catch (error) {
                        if (options.savepoint) {
                            await client.query(`ROLLBACK TO SAVEPOINT batch_sp_${savepointIndex - 1}`);
                            await client.query(`RELEASE SAVEPOINT batch_sp_${savepointIndex - 1}`);
                        }
                        const failedRow: BatchRowError = {
                            rowIndex: index,
                            row: {},
                            error,
                        };
                        errors.push(failedRow);
                        stats.rowsFailed += 1;
                        options.onRowError?.({}, error, index);
                        if (!options.savepoint) {
                            throw error;
                        }
                    }
                }
            });
        } else {
            for (let index = 0; index < conditionCount; index += chunkSize) {
                checkAborted(options.signal);
                const conditionChunk = whereConditions.slice(index, index + chunkSize).flatMap((cond) => cond.values);
                const chunkIndex = Math.floor(index / chunkSize);

                try {
                    await executeChunk(conditionChunk, chunkIndex, totalChunks);
                } catch (error) {
                    const failedRow: BatchRowError = {
                        rowIndex: index,
                        row: {},
                        error,
                    };
                    errors.push(failedRow);
                    stats.rowsFailed += 1;
                    options.onRowError?.({}, error, index);
                    if (!options.savepoint && !options.onRowError) {
                        throw error;
                    }
                }
            }
        }

        return {
            rows: collected,
            rowCount: collected.length,
            stats: finalizeStats(stats, startTime),
        };
    }

    /**
     * Batch DELETE with chunked execution and transaction support.
     */
    async function deleteMany(
        table: string,
        whereConditions: {
            column: string;
            operator: string;
            values: unknown[];
        }[],
        options: {
            chunkSize?: number;
            returning?: string[];
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            client?: PoolClient;
            transaction?: boolean;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onRowError?: (row: Record<string, unknown>, error: unknown, rowIndex: number) => void;
            savepoint?: boolean;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        } = {},
    ): Promise<BatchUpdateDeleteResult> {
        const startTime = Date.now();
        const stats: BatchStats = makeStats(startTime);
        const errors: BatchRowError[] = [];

        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const collected: unknown[] = [];
        const retry = resolveRetry(options.retry ?? false);

        const executeChunk = async (
            conditionChunk: unknown[],
            idx: number,
            total: number,
        ): Promise<QueryResult> => {
            const params: unknown[] = [...conditionChunk];

            const whereClauses = whereConditions.map((cond, idx) => {
                const placeholder = `$${idx + 1}`;
                return `${quoteIdent(cond.column)} ${cond.operator} ${placeholder}`;
            }).join(" AND ");

            const returning = options.returning
                ? `RETURNING ${quoteIdentList(options.returning)}`
                : "";

            const text = `DELETE FROM ${quoteIdent(table)} WHERE ${whereClauses} ${returning}`;

            const chunkStart = Date.now();
            let chunkSucceeded = 0;

            const result = await runWithRetry(
                () => {
                    const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
                    if (options.shardKey && mergedOptions.shardKey === undefined) {
                        mergedOptions.shardKey = options.shardKey;
                    }
                    if (options.shardId && mergedOptions.shardId === undefined) {
                        mergedOptions.shardId = options.shardId;
                    }
                    if (options.client) {
                        return options.client.query(text, params);
                    }
                    return query(text, params, mergedOptions);
                },
                retry,
                options.signal,
                "delete",
            );

            if (returning) {
                collected.push(...(result.rows ?? []));
                chunkSucceeded = result.rows?.length ?? 0;
            } else {
                chunkSucceeded = result.rowCount ?? 0;
            }

            stats.chunks++;
            stats.rowsProcessed += 1;
            stats.rowsSucceeded += chunkSucceeded;

            options.onProgress?.(
                progressEvent("delete", idx, total, 1, chunkSucceeded, 0, Date.now() - chunkStart),
            );

            return result;
        };

        const totalChunks = Math.ceil(whereConditions.length / chunkSize);

        const runChunkWithClient = async (
            client: PoolClient,
            conditionChunk: unknown[],
            idx: number,
            total: number,
        ): Promise<QueryResult> => {
            const params: unknown[] = [...conditionChunk];

            const whereClauses = whereConditions.map((cond, idx) => {
                const placeholder = `$${idx + 1}`;
                return `${quoteIdent(cond.column)} ${cond.operator} ${placeholder}`;
            }).join(" AND ");

            const returning = options.returning
                ? `RETURNING ${quoteIdentList(options.returning)}`
                : "";

            const text = `DELETE FROM ${quoteIdent(table)} WHERE ${whereClauses} ${returning}`;

            const chunkStart = Date.now();
            let chunkSucceeded = 0;

            const result = await runWithRetry(
                () => client.query(text, params),
                retry,
                options.signal,
                "delete",
            );

            if (returning) {
                collected.push(...(result.rows ?? []));
                chunkSucceeded = result.rows?.length ?? 0;
            } else {
                chunkSucceeded = result.rowCount ?? 0;
            }

            stats.chunks++;
            stats.rowsProcessed += 1;
            stats.rowsSucceeded += chunkSucceeded;

            options.onProgress?.(
                progressEvent("delete", idx, total, 1, chunkSucceeded, 0, Date.now() - chunkStart),
            );

            return result;
        };

        if (options.transaction && transaction) {
            let savepointIndex = 0;
            await transaction(async (client: PoolClient) => {
                for (let index = 0; index < whereConditions.length; index += chunkSize) {
                    checkAborted(options.signal);
                    const conditionChunk = whereConditions.slice(index, index + chunkSize).flatMap((cond) => cond.values);
                    const chunkIndex = Math.floor(index / chunkSize);

                    if (options.savepoint) {
                        await client.query(`SAVEPOINT batch_sp_${savepointIndex++}`);
                    }

                    try {
                        await runChunkWithClient(client, conditionChunk, chunkIndex, totalChunks);
                    } catch (error) {
                        if (options.savepoint) {
                            await client.query(`ROLLBACK TO SAVEPOINT batch_sp_${savepointIndex - 1}`);
                            await client.query(`RELEASE SAVEPOINT batch_sp_${savepointIndex - 1}`);
                        }
                        const failedRow: BatchRowError = {
                            rowIndex: index,
                            row: {},
                            error,
                        };
                        errors.push(failedRow);
                        stats.rowsFailed += 1;
                        options.onRowError?.({}, error, index);
                        if (!options.savepoint) {
                            throw error;
                        }
                    }
                }
            });
        } else {
            for (let index = 0; index < whereConditions.length; index += chunkSize) {
                checkAborted(options.signal);
                const conditionChunk = whereConditions.slice(index, index + chunkSize).flatMap((cond) => cond.values);
                const chunkIndex = Math.floor(index / chunkSize);

                try {
                    await executeChunk(conditionChunk, chunkIndex, totalChunks);
                } catch (error) {
                    const failedRow: BatchRowError = {
                        rowIndex: index,
                        row: {},
                        error,
                    };
                    errors.push(failedRow);
                    stats.rowsFailed += 1;
                    options.onRowError?.({}, error, index);
                    if (!options.savepoint && !options.onRowError) {
                        throw error;
                    }
                }
            }
        }

        return {
            rows: collected,
            rowCount: collected.length,
            stats: finalizeStats(stats, startTime),
        };
    }

    /**
     * Execute a batch operation using QueryBuilders with transaction, retry, cancellation, and statistics support.
     */
    async function executeBatch(
        builders: QueryBuilder[],
        options: {
            chunkSize?: number;
            transaction?: boolean;
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
            retry?: boolean | number | RetryOptions;
            signal?: AbortSignal;
            timeout?: number;
            onProgress?: (progress: BatchProgressEvent) => void;
            adaptive?: boolean;
        } = {},
    ): Promise<QueryResult[]> {
        const results: QueryResult[] = [];
        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const retry = resolveRetry(options.retry ?? false);

        const executeInTransaction = options.transaction && transaction;

        if (executeInTransaction) {
            await transaction(async (client: PoolClient) => {
                for (let i = 0; i < builders.length; i += chunkSize) {
                    checkAborted(options.signal);
                    const chunk = builders.slice(i, i + chunkSize);
                    const chunkIndex = Math.floor(i / chunkSize);
                    const totalChunks = Math.ceil(builders.length / chunkSize);
                    const chunkStart = Date.now();

                    const chunkResults = await Promise.all(
                        chunk.map(async (builder) => {
                            const { text, values } = builder.toSQL();
                            return runWithRetry(
                                () => client.query(text, values),
                                retry,
                                options.signal,
                                "executeBatch",
                            );
                        })
                    );

                    results.push(...chunkResults);

                    options.onProgress?.(
                        progressEvent("executeBatch", chunkIndex, totalChunks, chunk.length, chunk.length, 0, Date.now() - chunkStart),
                    );
                }
            });
        } else {
            for (let i = 0; i < builders.length; i += chunkSize) {
                checkAborted(options.signal);
                const chunk = builders.slice(i, i + chunkSize);
                const chunkIndex = Math.floor(i / chunkSize);
                const totalChunks = Math.ceil(builders.length / chunkSize);
                const chunkStart = Date.now();

                const chunkResults = await Promise.all(
                    chunk.map(async (builder) => {
                        const { text, values } = builder.toSQL();
                        const mergedOptions: Record<string, unknown> = {
                            ...(options.queryOptions ?? {}),
                            shardKey: options.shardKey,
                            shardId: options.shardId,
                        };
                        return runWithRetry(
                            () => query(text, values, mergedOptions),
                            retry,
                            options.signal,
                            "executeBatch",
                        );
                    })
                );

                results.push(...chunkResults);

                options.onProgress?.(
                    progressEvent("executeBatch", chunkIndex, totalChunks, chunk.length, chunk.length, 0, Date.now() - chunkStart),
                );
            }
        }

        return results;
    }

    return {
        insertMany,
        selectMany,
        updateMany,
        deleteMany,
        executeBatch,
    };
}
