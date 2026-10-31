import type { QueryBuilder as QueryBuilderType, QueryRunner, ModelFactory, Model, QueryResultRow, ModelFindOptions } from "../types.js";
import { QueryBuilder } from "../query/builder.js";
import { raw } from "../query/sql.js";

function isPlainObject(value: unknown): boolean {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Thin model helper around `QueryBuilder`. It intentionally exposes the SQL
 * rather than hiding it: every method returns rows and accepts the same query
 * options (`client`, `timeout`, `retry`) as the raw client.
 */
export function createModel({
    query,
    table,
    idColumn = "id",
    logger,
    shardKey,
    shardId,
}: {
    query?: QueryRunner;
    table: string;
    idColumn?: string;
    logger?: unknown;
    shardKey?: string;
    shardId?: string;
} = { table: "" }): Model {
    if (!table) {
        throw new TypeError("createModel requires a table name");
    }

    function builder(): QueryBuilderType {
        return new QueryBuilder({ query: query as QueryRunner, table, logger, shardKey, shardId });
    }

    async function run(builderInstance: QueryBuilderType, options: ModelFindOptions = {}): Promise<{ rows: QueryResultRow[] }> {
        const { text, values } = builderInstance.toSQL();
        // Merge model-level shard routing into per-call options. Per-call
        // shardKey/shardId take precedence over model-level configuration.
        const mergedOptions: ModelFindOptions = { ...options };
        if (shardKey && mergedOptions.shardKey === undefined) {
            mergedOptions.shardKey = shardKey;
        }
        if (shardId && mergedOptions.shardId === undefined) {
            mergedOptions.shardId = shardId;
        }
        if (options.client?.query) {
            const result = await options.client.query(text, values);
            return { rows: (result?.rows ?? []) as QueryResultRow[] };
        }
        if (!query) {
            throw new TypeError("No query function available");
        }
        const result = await query(text, values, mergedOptions);
        return { rows: (result?.rows ?? []) as QueryResultRow[] };
    }

    async function find(where: Record<string, unknown> = {}, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        const q = builder().select(...((options.columns as string[]) ?? ["*"]));
        if (isPlainObject(where) && Object.keys(where).length > 0) {
            q.where(where);
        }
        if (options.orderBy) {
            if (Array.isArray(options.orderBy)) {
                for (const [column, direction] of options.orderBy as [string, string][]) {
                    q.orderBy(column, direction);
                }
            } else {
                q.orderBy(String(options.orderBy), String(options.direction));
            }
        }
        if (options.limit !== undefined) {
            q.limit(Number(options.limit));
        }
        if (options.offset !== undefined) {
            q.offset(Number(options.offset));
        }
        const result = await run(q, options);
        return result.rows ?? [];
    }

    async function findOne(where: Record<string, unknown> = {}, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        const rows = await find(where, { ...options, limit: 1 });
        return rows[0] ?? null;
    }

    function findById(id: unknown, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        return findOne({ [idColumn]: id }, options);
    }

    async function insert(data: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        const q = builder().insert(data).returning(...((options.returning as string[]) ?? ["*"]));
        const result = await run(q, options);
        return result.rows?.[0] ?? null;
    }

    async function insertMany(rows: Record<string, unknown>[], options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        if (!Array.isArray(rows) || rows.length === 0) {
            return [];
        }
        const q = builder().insert(rows).returning(...((options.returning as string[]) ?? ["*"]));
        const result = await run(q, options);
        return result.rows ?? [];
    }

    async function update(where: Record<string, unknown>, data: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        const q = builder().update(data).returning(...((options.returning as string[]) ?? ["*"]));
        if (Object.keys(where).length > 0) {
            q.where(where);
        }
        const result = await run(q, options);
        return result.rows ?? [];
    }

    async function updateById(id: unknown, data: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        const rows = await update({ [idColumn]: id }, data, options);
        return rows[0] ?? null;
    }

    async function remove(where: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        const q = builder().delete().returning(...((options.returning as string[]) ?? ["*"]));
        if (Object.keys(where).length > 0) {
            q.where(where);
        }
        const result = await run(q, options);
        return result.rows ?? [];
    }

    async function removeById(id: unknown, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        const rows = await remove({ [idColumn]: id }, options);
        return rows[0] ?? null;
    }

    async function count(where: Record<string, unknown> = {}, options: ModelFindOptions = {}): Promise<number> {
        const q = builder().select(raw("COUNT(*)::int AS count"));
        if (Object.keys(where).length > 0) {
            q.where(where);
        }
        const result = await run(q, options);
        return (result?.rows?.[0]?.count as number) ?? 0;
    }

    async function exists(where: Record<string, unknown> = {}, options: ModelFindOptions = {}): Promise<boolean> {
        const q = builder().select(raw("1 AS exists")).limit(1);
        if (Object.keys(where).length > 0) {
            q.where(where);
        }
        const result = await run(q, options);
        return Boolean(result?.rows?.length);
    }

    return {
        table,
        idColumn,
        query: builder,
        builder,
        select: builder,
        find,
        findOne,
        findById,
        insert,
        insertMany,
        update,
        updateById,
        delete: remove,
        deleteById: removeById,
        remove,
        removeById,
        count,
        exists,
    };
}

export function createModelFactory({
    query,
    logger,
}: {
    query?: QueryRunner;
    logger?: unknown;
} = {}): ModelFactory {
    return function model(table: string, options: Record<string, unknown> = {}) {
        return createModel({ query, table, logger, ...options });
    };
}