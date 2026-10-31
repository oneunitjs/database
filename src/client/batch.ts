import type { PoolClient, QueryResult } from "pg";
import type { InsertManyOptions, BatchApi } from "../types.js";
import { quoteIdent, quoteIdentList } from "../query/sql.js";

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
    query: (text: string, params: unknown[], options?: { timeout?: number }) => Promise<QueryResult>;
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

    return {
        insertMany,
    };
}