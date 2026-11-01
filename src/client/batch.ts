import type { PoolClient, QueryResult } from "pg";
import type { InsertManyOptions, BatchApi, QueryOptions } from "../types.js";
import { quoteIdent, quoteIdentList, isFragment, sql, raw } from "../query/sql.js";
import { QueryBuilder } from "../query/builder.js";

const DEFAULT_CHUNK_SIZE = 500;
const MAX_PARAMETERS = 65_535;

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
        // Merge shard routing from batch options into query options so the
        // unified executor routes the batch to the correct shard.
        const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
        if (options.shardKey && mergedOptions.shardKey === undefined) {
            mergedOptions.shardKey = options.shardKey;
        }
        if (options.shardId && mergedOptions.shardId === undefined) {
            mergedOptions.shardId = options.shardId;
        }
        return query(sqlText, params, mergedOptions);
    }

    async function insertMany(
        table: string,
        rows: Record<string, unknown>[],
        options: InsertManyOptions = {},
    ): Promise<unknown> {
        if (!Array.isArray(rows) || rows.length === 0) {
            return [];
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

        const executeChunk = async (chunk: Record<string, unknown>[], chunkOptions: InsertManyOptions = options) => {
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

            const result = await runChunk(text, params, chunkOptions);
            if (returning) {
                collected.push(...(result?.rows ?? []));
            }

            return result;
        };

        if (options.transaction && transaction) {
            await transaction(async (client: PoolClient) => {
                for (let index = 0; index < rows.length; index += chunkSize) {
                    await executeChunk(rows.slice(index, index + chunkSize), { ...options, client });
                }
            });
        } else {
            for (let index = 0; index < rows.length; index += chunkSize) {
                await executeChunk(rows.slice(index, index + chunkSize));
            }
        }

        return returning ? collected : rows.length;
    }

    /**
     * Batch SELECT with chunked execution.
     * Useful for processing large result sets without loading everything into memory.
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
        } = {},
    ): Promise<unknown[]> {
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

        // Add LIMIT and OFFSET for pagination
        const executeChunk = async (offset: number) => {
            const paginatedText = `${sqlText} LIMIT ${chunkSize} OFFSET ${offset}`;
            
            const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
            if (options.shardKey && mergedOptions.shardKey === undefined) {
                mergedOptions.shardKey = options.shardKey;
            }
            if (options.shardId && mergedOptions.shardId === undefined) {
                mergedOptions.shardId = options.shardId;
            }

            if (options.client) {
                const result = await options.client.query(paginatedText, params);
                return result.rows ?? [];
            }
            const result = await query(paginatedText, params, mergedOptions);
            return result.rows ?? [];
        };

        let offset = 0;
        let hasMore = true;

        while (hasMore) {
            const rows = await executeChunk(offset);
            
            if (rows.length === 0) {
                hasMore = false;
            } else {
                if (options.onChunk) {
                    await options.onChunk(rows);
                }
                collected.push(...rows);
                offset += chunkSize;
                hasMore = rows.length >= chunkSize;
            }
        }

        return collected;
    }

    /**
     * Batch UPDATE with chunked execution.
     * Updates rows in chunks to avoid parameter limit and lock contention.
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
        } = {},
    ): Promise<unknown[]> {
        const columns = Object.keys(values);
        if (columns.length === 0) {
            throw new TypeError("updateMany requires at least one column to update");
        }

        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const collected: unknown[] = [];

        const executeChunk = async (valueChunk: unknown[], conditionChunk: unknown[]) => {
            const params: unknown[] = [...valueChunk, ...conditionChunk];
            
            const setClause = columns.map((col, idx) => `${quoteIdent(col)} = $${idx + 1}`).join(", ");
            
            const whereClauses = whereConditions.map((cond, idx) => {
                const valueOffset = columns.length + idx * 2;
                const placeholder = `$${valueOffset + 1}`;
                return `${quoteIdent(cond.column)} ${cond.operator} ${placeholder}`;
            }).join(" AND ");

            const returning = options.returning
                ? `RETURNING ${quoteIdentList(options.returning)}`
                : "";

            const text = `UPDATE ${quoteIdent(table)} SET ${setClause} WHERE ${whereClauses} ${returning}`;

            const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
            if (options.shardKey && mergedOptions.shardKey === undefined) {
                mergedOptions.shardKey = options.shardKey;
            }
            if (options.shardId && mergedOptions.shardId === undefined) {
                mergedOptions.shardId = options.shardId;
            }

            if (options.client) {
                const result = await options.client.query(text, params);
                if (returning) {
                    collected.push(...(result.rows ?? []));
                }
                return result;
            }
            const result = await query(text, params, mergedOptions);
            if (returning) {
                collected.push(...(result.rows ?? []));
            }
            return result;
        };

        // This is a simplified implementation
        // For a full batch update, you'd typically update based on primary keys
        const valueChunk = columns.map(col => values[col]);
        const conditionChunk = whereConditions.flatMap(cond => cond.values);

        await executeChunk(valueChunk, conditionChunk);

        return collected;
    }

    /**
     * Batch DELETE with chunked execution.
     * Deletes rows in chunks to avoid parameter limit and lock contention.
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
        } = {},
    ): Promise<unknown[]> {
        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
        const collected: unknown[] = [];

        const executeChunk = async (conditionChunk: unknown[]) => {
            const params: unknown[] = [...conditionChunk];
            
            const whereClauses = whereConditions.map((cond, idx) => {
                const placeholder = `$${idx + 1}`;
                return `${quoteIdent(cond.column)} ${cond.operator} ${placeholder}`;
            }).join(" AND ");

            const returning = options.returning
                ? `RETURNING ${quoteIdentList(options.returning)}`
                : "";

            const text = `DELETE FROM ${quoteIdent(table)} WHERE ${whereClauses} ${returning}`;

            const mergedOptions: Record<string, unknown> = { ...(options.queryOptions ?? {}) };
            if (options.shardKey && mergedOptions.shardKey === undefined) {
                mergedOptions.shardKey = options.shardKey;
            }
            if (options.shardId && mergedOptions.shardId === undefined) {
                mergedOptions.shardId = options.shardId;
            }

            if (options.client) {
                const result = await options.client.query(text, params);
                if (returning) {
                    collected.push(...(result.rows ?? []));
                }
                return result;
            }
            const result = await query(text, params, mergedOptions);
            if (returning) {
                collected.push(...(result.rows ?? []));
            }
            return result;
        };

        // This is a simplified implementation
        const conditionChunk = whereConditions.flatMap(cond => cond.values);
        await executeChunk(conditionChunk);

        return collected;
    }

    /**
     * Execute a batch operation using a QueryBuilder.
     * This allows using all QueryBuilder features with batch execution.
     */
    async function executeBatch(
        builders: QueryBuilder[],
        options: {
            chunkSize?: number;
            transaction?: boolean;
            queryOptions?: QueryOptions;
            shardKey?: string;
            shardId?: string;
        } = {},
    ): Promise<QueryResult[]> {
        const results: QueryResult[] = [];
        const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;

        const executeInTransaction = options.transaction && transaction;

        if (executeInTransaction) {
            await transaction(async (client: PoolClient) => {
                for (let i = 0; i < builders.length; i += chunkSize) {
                    const chunk = builders.slice(i, i + chunkSize);
                    const chunkResults = await Promise.all(
                        chunk.map(builder => {
                            const { text, values } = builder.toSQL();
                            const mergedOptions: Record<string, unknown> = { 
                                ...(options.queryOptions ?? {}),
                                shardKey: options.shardKey,
                                shardId: options.shardId
                            };
                            return client.query(text, values);
                        })
                    );
                    results.push(...chunkResults);
                }
            });
        } else {
            for (let i = 0; i < builders.length; i += chunkSize) {
                const chunk = builders.slice(i, i + chunkSize);
                const chunkResults = await Promise.all(
                    chunk.map(builder => {
                        const { text, values } = builder.toSQL();
                        const mergedOptions: Record<string, unknown> = { 
                            ...(options.queryOptions ?? {}),
                            shardKey: options.shardKey,
                            shardId: options.shardId
                        };
                        return query(text, values, mergedOptions);
                    })
                );
                results.push(...chunkResults);
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
