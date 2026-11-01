import type { QueryBuilder as QueryBuilderType, QueryRunner, ModelFactory, Model, QueryResultRow, ModelFindOptions, ExtendedModelFindOptions, ModelOptions, ModelHooks, PoolClient } from "../types.js";
import { QueryBuilder } from "../query/builder.js";
import { raw } from "../query/sql.js";

type ClientCapableQuery = QueryRunner & {
    getClient?: () => Promise<PoolClient>;
    releaseClient?: (client: PoolClient) => void;
};

function isPlainObject(value: unknown): boolean {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Symbol to mark soft-deleted models
 */
export const SOFT_DELETE_SYMBOL = Symbol('softDelete');

/**
 * Thin model helper around `QueryBuilder`. It intentionally exposes the SQL
 * rather than hiding it: every method returns rows and accepts the same query
 * options (`client`, `timeout`, `retry`) as the raw client.
 *
 * Enhanced with support for:
 * - Complex WHERE conditions (comparison operators, IN, LIKE, etc.)
 * - Soft deletes
 * - Automatic timestamps
 * - Lifecycle hooks
 * - Runtime validation
 */
export function createModel<T extends QueryResultRow = QueryResultRow>({
    query,
    table,
    idColumn = "id",
    logger,
    shardKey,
    shardId,
    softDelete: softDeleteEnabled = false,
    softDeleteField = "deleted_at",
    timestamps: timestampsEnabled = false,
    createdAtField = "created_at",
    updatedAtField = "updated_at",
    validate,
    hooks,
    relationships,
    coerce: coerceEnabled = false,
    coerceTypes,
}: ModelOptions<T> = { table: "" }): Model {
    if (!table) {
        throw new TypeError("createModel requires a table name");
    }

    const clientQuery = query as unknown as ClientCapableQuery | undefined;

    function builder(): QueryBuilderType {
        return new QueryBuilder({ query: query as QueryRunner, table, logger, shardKey, shardId });
    }

    /**
     * Apply soft delete filter to a query if enabled
     */
    function applySoftDeleteFilter(q: QueryBuilderType, withDeleted: boolean = false): QueryBuilderType {
        if (softDeleteEnabled && !withDeleted) {
            return q.whereNull(softDeleteField);
        }
        return q;
    }

    /**
     * Apply automatic timestamps to data
     */
    function applyTimestamps(data: Record<string, unknown>, operation: 'create' | 'update'): Record<string, unknown> {
        if (!timestampsEnabled) return data;
        
        const now = new Date();
        const result = { ...data };
        
        if (operation === 'create') {
            result[createdAtField as string] = now;
            result[updatedAtField as string] = now;
        } else if (operation === 'update') {
            result[updatedAtField as string] = now;
        }
        
        return result;
    }

    /**
     * Run validation on data
     */
    async function runValidation(data: Record<string, unknown>, operation: 'create' | 'update'): Promise<Record<string, unknown>> {
        if (!validate) return data;
        return validate(data, operation);
    }

    /**
     * Run lifecycle hooks
     */
    async function runHook<TResult>(
        hookType: keyof ModelHooks<T>,
        data: unknown,
        context?: unknown
    ): Promise<TResult> {
        if (!hooks || !hooks[hookType]) {
            return data as TResult;
        }
        
        const hook = hooks[hookType] as ((data: unknown, context?: unknown) => TResult) | undefined;
        if (!hook) {
            return data as TResult;
        }
        
        try {
            const result = hook(data, context);
            if (result instanceof Promise) {
                return await result;
            }
            return result;
        } catch (error) {
            throw error;
        }
    }

    /**
     * Coerce a single value to the target type.
     * Supported types: 'string', 'number', 'boolean', 'date'.
     * Returns the original value if coercion is not applicable or safe.
     */
    function coerceValue(value: unknown, targetType: string): unknown {
        if (value === null || value === undefined) {
            return value;
        }

        switch (targetType) {
            case 'string':
                return String(value);
            case 'number': {
                const num = Number(value);
                return Number.isNaN(num) ? value : num;
            }
            case 'boolean': {
                if (typeof value === 'boolean') return value;
                if (typeof value === 'number') return value !== 0;
                if (typeof value === 'string') {
                    const lower = String(value).trim().toLowerCase();
                    return lower === 'true' || lower === '1' || lower === 'yes' || lower === 'on';
                }
                return Boolean(value);
            }
            case 'date': {
                if (value instanceof Date) return value;
                if (typeof value !== 'string' && typeof value !== 'number') return value;
                const d = new Date(value as string | number);
                return Number.isNaN(d.getTime()) ? value : d;
            }
            default:
                return value;
        }
    }

    /**
     * Coerce data values based on coerceTypes mapping or heuristic detection.
     * Only coerces when coerce is enabled.
     */
    function coerceData(data: Record<string, unknown>): Record<string, unknown> {
        if (!coerceEnabled) return data;

        const result: Record<string, unknown> = { ...data };
        const types = coerceTypes ?? {};

        for (const [key, value] of Object.entries(result)) {
            const targetType = types[key];
            if (targetType) {
                result[key] = coerceValue(value, targetType);
                continue;
            }

            // Heuristic coercion for unregistered columns
            if (typeof value === 'string') {
                // Try number
                const trimmed = value.trim();
                if (trimmed !== '' && !Number.isNaN(Number(trimmed)) && trimmed.match(/^-?\d+(\.\d+)?$/)) {
                    result[key] = Number(trimmed);
                    continue;
                }
                // Try boolean
                const lower = trimmed.toLowerCase();
                if (lower === 'true' || lower === 'false' || lower === '1' || lower === '0' || lower === 'yes' || lower === 'no' || lower === 'on' || lower === 'off') {
                    result[key] = lower === 'true' || lower === '1' || lower === 'yes' || lower === 'on';
                    continue;
                }
                // Try date (ISO format)
                if (trimmed.match(/^\d{4}-\d{2}-\d{2}/)) {
                    const d = new Date(trimmed);
                    if (!Number.isNaN(d.getTime())) {
                        result[key] = d;
                        continue;
                    }
                }
            }
        }

        return result;
    }

    /**
     * Run an operation inside an automatic transaction when no client is
     * provided. When a `client` is already present (e.g. the caller is inside
     * `db.transaction()`), it is used directly without beginning/committing,
     * so the operation joins the existing transaction instead of nesting one.
     */
    async function withAtomicClient<T>(
        options: ModelFindOptions,
        op: (client: Pick<PoolClient, "query">) => Promise<T>
    ): Promise<T> {
        const client = options.client;
        if (client) {
            return op(client);
        }
        if (!(options.atomic ?? false)) {
            throw new TypeError("withAtomicClient requires atomic:true when no client is provided");
        }
        const acquired = await clientQuery?.getClient?.();
        if (!acquired) {
            throw new TypeError(
                "Atomic operation requires a transaction client. Either pass { client } or ensure query has getClient()."
            );
        }
        try {
            await acquired.query("BEGIN");
            const result = await op(acquired);
            await acquired.query("COMMIT");
            return result;
        } catch (error) {
            try {
                await acquired.query("ROLLBACK");
            } catch {
                // Swallow rollback errors; the original error takes precedence.
            }
            throw error;
        } finally {
            if (clientQuery?.releaseClient) {
                clientQuery.releaseClient(acquired);
            }
        }
    }

    /**
     * Execute a builder query, auto-wrapping in a transaction when `atomic: true`
     * and no transaction client is supplied. When a client is supplied the
     * operation joins the existing transaction (no nested BEGIN/COMMIT).
     */
    async function runQuery(q: QueryBuilderType, options: ModelFindOptions = {}): Promise<{ rows: QueryResultRow[] }> {
        if (options.client) {
            return run(q, options);
        }
        if (options.atomic) {
            return withAtomicClient(options, (client) => run(q, { ...options, client }));
        }
        return run(q, options);
    }

    /**
     * Join another table (delegates to QueryBuilder.join).
     * Returns a QueryBuilder that can be passed via { queryBuilder } to find/findOne/count/exists.
     */
    function join(table: string | QueryBuilderType, on: string | unknown, alias?: string): QueryBuilderType {
        return builder().join(table, on as never, alias);
    }

    /**
     * INNER JOIN another table.
     */
    function innerJoin(table: string | QueryBuilderType, on: string | unknown, alias?: string): QueryBuilderType {
        return builder().innerJoin(table, on as never, alias);
    }

    /**
     * LEFT JOIN another table.
     */
    function leftJoin(table: string | QueryBuilderType, on: string | unknown, alias?: string): QueryBuilderType {
        return builder().leftJoin(table, on as never, alias);
    }

    /**
     * RIGHT JOIN another table.
     */
    function rightJoin(table: string | QueryBuilderType, on: string | unknown, alias?: string): QueryBuilderType {
        return builder().rightJoin(table, on as never, alias);
    }

    /**
     * FULL JOIN another table.
     */
    function fullJoin(table: string | QueryBuilderType, on: string | unknown, alias?: string): QueryBuilderType {
        return builder().fullJoin(table, on as never, alias);
    }

    /**
     * CROSS JOIN another table.
     */
    function crossJoin(table: string | QueryBuilderType, alias?: string): QueryBuilderType {
        return builder().crossJoin(table, alias);
    }

    /**
     * Build WHERE clause from various input types
     */
    function buildWhereClause(
        q: QueryBuilderType,
        where: Record<string, unknown> | Array<Record<string, unknown>> | undefined
    ): QueryBuilderType {
        if (!where) return q;
        
        if (Array.isArray(where)) {
            // Handle array of conditions (AND/OR groups)
            for (const condition of where) {
                const cond = condition as Record<string, unknown>;
                if (cond.AND && Array.isArray(cond.AND)) {
                    q.whereGroup(subQ => {
                        for (const andCond of cond.AND as Array<Record<string, unknown>>) {
                            buildWhereClause(subQ, andCond);
                        }
                        return subQ;
                    });
                } else if (cond.OR && Array.isArray(cond.OR)) {
                    q.orWhereGroup(subQ => {
                        for (const orCond of cond.OR as Array<Record<string, unknown>>) {
                            buildWhereClause(subQ, orCond);
                        }
                        return subQ;
                    });
                } else {
                    q.where(cond);
                }
            }
            return q;
        }
        
        // Simple object - handle special operators
        if (isPlainObject(where)) {
            const simpleWhere: Record<string, unknown> = {};
            const complexConditions: Array<(q: QueryBuilderType) => QueryBuilderType> = [];
            
            for (const [key, value] of Object.entries(where)) {
                // Handle special operators like $gt, $lt, $in, etc.
                if (isPlainObject(value)) {
                    const objValue = value as Record<string, unknown>;
                    
                    // Check for comparison operators
                    if ('$gt' in objValue) {
                        complexConditions.push(q => q.where(key, '>', objValue.$gt));
                    }
                    if ('$gte' in objValue) {
                        complexConditions.push(q => q.where(key, '>=', objValue.$gte));
                    }
                    if ('$lt' in objValue) {
                        complexConditions.push(q => q.where(key, '<', objValue.$lt));
                    }
                    if ('$lte' in objValue) {
                        complexConditions.push(q => q.where(key, '<=', objValue.$lte));
                    }
                    if ('$ne' in objValue) {
                        complexConditions.push(q => q.where(key, '!=', objValue.$ne));
                    }
                    if ('$in' in objValue) {
                        complexConditions.push(q => q.whereIn(key, objValue.$in as unknown[]));
                    }
                    if ('$nin' in objValue || '$notIn' in objValue) {
                        complexConditions.push(q => q.whereNotIn(key, (objValue.$nin ?? objValue.$notIn) as unknown[]));
                    }
                    if ('$like' in objValue) {
                        complexConditions.push(q => q.where(key, 'LIKE', objValue.$like));
                    }
                    if ('$ilike' in objValue) {
                        complexConditions.push(q => q.where(key, 'ILIKE', objValue.$ilike));
                    }
                    if ('$null' in objValue) {
                        if (objValue.$null === true) {
                            complexConditions.push(q => q.whereNull(key));
                        } else {
                            complexConditions.push(q => q.whereNotNull(key));
                        }
                    }
                    // Don't add to simpleWhere - it's already handled by complexConditions
                } else {
                    simpleWhere[key] = value;
                }
            }
            
            if (Object.keys(simpleWhere).length > 0) {
                q.where(simpleWhere);
            }
            
            for (const condition of complexConditions) {
                condition(q);
            }
            
            return q;
        }
        
        return q;
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

    async function find(where: Record<string, unknown> | Array<Record<string, unknown>> = {}, options: ExtendedModelFindOptions = {}): Promise<QueryResultRow[]> {
        const q = options.queryBuilder ?? builder().select(...((options.columns as string[]) ?? ["*"]));
        
        // Apply soft delete filter if enabled
        applySoftDeleteFilter(q, options.withDeleted);
        
        // Build WHERE clause
        buildWhereClause(q, where as Record<string, unknown> | Array<Record<string, unknown>>);
        
        // Apply ordering, limit, offset
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

    async function findOne(where: Record<string, unknown> | Array<Record<string, unknown>> = {}, options: ExtendedModelFindOptions = {}): Promise<QueryResultRow | null> {
        const rows = await find(where, { ...options, limit: 1 });
        return rows[0] ?? null;
    }

    function findById(id: unknown, options: ExtendedModelFindOptions = {}): Promise<QueryResultRow | null> {
        return findOne({ [idColumn]: id }, options);
    }

    async function insert(data: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        // Coerce types
        let processedData = coerceData(data);
        
        // Apply timestamps
        processedData = applyTimestamps(processedData, 'create');
        
        // Run validation
        processedData = await runValidation(processedData, 'create');
        
        // Run beforeCreate hook
        processedData = await runHook<Record<string, unknown>>('beforeCreate', processedData);
        
        const q = builder().insert(processedData).returning(...((options.returning as string[]) ?? ["*"]));
        const result = await runQuery(q, options);
        
        // Run afterCreate hook
        const finalResult = result.rows?.[0] ?? null;
        const hookedResult = await runHook<QueryResultRow | null>('afterCreate', finalResult);
        
        return hookedResult ?? null;
    }

    async function insertMany(rows: Record<string, unknown>[], options: ModelFindOptions & { atomic?: boolean } = {}): Promise<QueryResultRow[]> {
        if (!Array.isArray(rows) || rows.length === 0) {
            return [];
        }
        
        // Check if we should use atomic transaction
        const useAtomic = options.atomic ?? true; // Default to true for data integrity
        
        if (useAtomic && rows.length > 1 && !options.client) {
            // Auto-start transaction if atomic is requested but no client provided
            const client = await clientQuery?.getClient?.();
            if (!client) {
                throw new TypeError("Atomic insertMany requires a transaction client. Either pass { client } or ensure query has getClient().");
            }
            
            try {
                const results: QueryResultRow[] = [];
                for (const row of rows) {
                    // Apply timestamps
                    let processedData = applyTimestamps(row, 'create');
                    
                    // Run validation
                    processedData = await runValidation(processedData, 'create');
                    
                    // Run beforeCreate hook
                    processedData = await runHook<Record<string, unknown>>('beforeCreate', processedData);
                    
                    const q = builder().insert(processedData).returning(...((options.returning as string[]) ?? ["*"]));
                    const result = await run(q, { ...options, client });
                    
                    if (result.rows && result.rows.length > 0) {
                        // Run afterCreate hook
                        const hookedResult = await runHook<QueryResultRow | null>('afterCreate', result.rows[0]);
                        if (hookedResult) {
                            results.push(hookedResult);
                        }
                    }
                }
                
                // Commit if all inserts succeeded
                await client.query('COMMIT');
                return results;
            } catch (error) {
                // Rollback on any error
                await client.query('ROLLBACK');
                throw error;
            } finally {
                if (clientQuery?.releaseClient) {
                    clientQuery.releaseClient(client);
                }
            }
        }
        
        // Non-atomic or single row: use QueryBuilder's built-in batching
        // Apply timestamps and validation to all rows
        const processedRows = await Promise.all(rows.map(async row => {
            let processed = applyTimestamps(row, 'create');
            processed = await runValidation(processed, 'create');
            processed = await runHook<Record<string, unknown>>('beforeCreate', processed);
            return processed;
        }));
        
        const q = builder().insert(processedRows).returning(...((options.returning as string[]) ?? ["*"]));
        const result = await run(q, options);
        
        // Run afterCreate hooks for all results
        const finalResults = result.rows ?? [];
        const hookedResults = await Promise.all(
            finalResults.map(row => runHook<QueryResultRow | null>('afterCreate', row))
        );
        
        return hookedResults.filter(Boolean) as QueryResultRow[];
    }

    async function update(where: Record<string, unknown>, data: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        // Coerce types
        let processedData = coerceData(data);
        
        // Apply timestamps
        processedData = applyTimestamps(processedData, 'update');
        
        // Run validation
        processedData = await runValidation(processedData, 'update');
        
        // Run beforeUpdate hook
        processedData = await runHook<Record<string, unknown>>('beforeUpdate', processedData, where);
        
        const q = builder().update(processedData).returning(...((options.returning as string[]) ?? ["*"]));
        
        // Build WHERE clause with soft delete support
        if (softDeleteEnabled && !options.forceDelete) {
            // Only update non-deleted records
            const whereWithSoftDelete = { ...where, [softDeleteField]: null };
            buildWhereClause(q, whereWithSoftDelete);
        } else {
            buildWhereClause(q, where);
        }
        
        const result = await runQuery(q, options);
        
        // Run afterUpdate hooks
        const finalResults = result.rows ?? [];
        const hookedResults = await Promise.all(
            finalResults.map(row => runHook<QueryResultRow | null>('afterUpdate', row, where))
        );
        
        return hookedResults.filter(Boolean) as QueryResultRow[];
    }

    async function updateById(id: unknown, data: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        const rows = await update({ [idColumn]: id }, data, options);
        return rows[0] ?? null;
    }

    async function remove(where: Record<string, unknown>, options: ModelFindOptions & { forceDelete?: boolean } = {}): Promise<QueryResultRow[]> {
        // Handle soft delete
        if (softDeleteEnabled && !options.forceDelete) {
            // Instead of deleting, set deleted_at timestamp
            const now = new Date();
            const updateData = { [softDeleteField]: now };
            
            // Run beforeDelete hook
            const whereWithHook = await runHook<Record<string, unknown>>('beforeDelete', where);
            
            const result = await update(whereWithHook, updateData, options);
            
            // Run afterDelete hooks
            const finalResults = result;
            const hookedResults = await Promise.all(
                finalResults.map(row => runHook<QueryResultRow | null>('afterDelete', row, where))
            );
            
            return hookedResults.filter(Boolean) as QueryResultRow[];
        }
        
        // Hard delete
        const q2 = builder().delete().returning(...((options.returning as string[]) ?? ["*"]));
        
        // Apply soft delete filter if enabled
        if (softDeleteEnabled) {
            const whereWithHook = await runHook<Record<string, unknown>>('beforeDelete', where);
            const whereWithSoftDelete2 = { ...whereWithHook, [softDeleteField]: null };
            buildWhereClause(q2, whereWithSoftDelete2);
        } else {
            const whereWithHook = await runHook<Record<string, unknown>>('beforeDelete', where);
            buildWhereClause(q2, whereWithHook);
        }
        
        const result = await runQuery(q2, options);
        
        // Run afterDelete hooks
        const finalResults = result.rows ?? [];
        const hookedResults = await Promise.all(
            finalResults.map(row => runHook<QueryResultRow | null>('afterDelete', row, where))
        );
        
        return hookedResults.filter(Boolean) as QueryResultRow[];
    }

    async function removeById(id: unknown, options: ModelFindOptions & { forceDelete?: boolean } = {}): Promise<QueryResultRow | null> {
        const rows = await remove({ [idColumn]: id }, options);
        return rows[0] ?? null;
    }

    async function count(where: Record<string, unknown> | Array<Record<string, unknown>> = {}, options: ExtendedModelFindOptions = {}): Promise<number> {
        const q = options.queryBuilder ?? builder();
        q.select(raw("COUNT(*)::int AS count"));
        
        // Apply soft delete filter if enabled
        applySoftDeleteFilter(q, options.withDeleted);
        
        // Build WHERE clause
        buildWhereClause(q, where as Record<string, unknown> | Array<Record<string, unknown>>);
        
        const result = await run(q, options);
        return (result?.rows?.[0]?.count as number) ?? 0;
    }

    async function exists(where: Record<string, unknown> | Array<Record<string, unknown>> = {}, options: ExtendedModelFindOptions = {}): Promise<boolean> {
        const q = options.queryBuilder ?? builder();
        q.select(raw("1 AS exists")).limit(1);
        
        // Apply soft delete filter if enabled
        applySoftDeleteFilter(q, options.withDeleted);
        
        // Build WHERE clause
        buildWhereClause(q, where as Record<string, unknown> | Array<Record<string, unknown>>);
        
        const result = await run(q, options);
        return Boolean(result?.rows?.length);
    }

    /**
     * Find with soft-deleted records included
     */
    async function findWithDeleted(where: Record<string, unknown> | Array<Record<string, unknown>> = {}, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        return find(where, { ...options, withDeleted: true });
    }

    /**
     * Permanently delete records (bypasses soft delete)
     */
    async function forceDelete(where: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        return remove(where, { ...options, forceDelete: true });
    }

    /**
     * Permanently delete by primary key (bypasses soft delete)
     */
    async function forceDeleteById(id: unknown, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        return removeById(id, { ...options, forceDelete: true });
    }
    
    /**
     * Delete records with cascade to related tables
     * @param where - WHERE conditions
     * @param options - Options including cascade relationships
     */
    async function deleteWithCascade(
        where: Record<string, unknown>,
        options: ModelFindOptions & { cascade?: string[] } = {}
    ): Promise<QueryResultRow[]> {
        if (!relationships || !options.cascade || options.cascade.length === 0) {
            return remove(where, options);
        }
        
        // First, find the records to delete
        const recordsToDelete = await find(where, {});
        
        // For each relationship to cascade
        for (const relName of options.cascade) {
            const relConfig = relationships[relName];
            if (!relConfig) continue;
            
            const relatedModel = (query as any).model(relConfig.model);
            
            // Get IDs and foreign key
            const ids = recordsToDelete.map(r => r[idColumn]).filter(id => id !== undefined);
            if (ids.length === 0) continue;
            
            let relatedWhere: Record<string, unknown> | undefined;
            
            switch (relConfig.type) {
                case 'hasMany':
                case 'hasOne': {
                    const fk1 = relConfig.foreignKey || `${table}_id`;
                    relatedWhere = { [fk1]: { $in: ids } };
                    break;
                }
                case 'belongsTo': {
                    const fk2 = relConfig.foreignKey || `${relName}_id`;
                    relatedWhere = { [idColumn]: { $in: ids } };
                    break;
                }
                case 'manyToMany': {
                    if (!relConfig.through) continue;
                    const throughModel = (query as any).model(relConfig.through);
                    const throughFk = relConfig.throughForeignKey || `${table}_id`;
                    await throughModel.delete({ [throughFk]: { $in: ids } });
                    continue;
                }
                default:
                    continue;
            }
            
            // Delete related records
            if (relatedWhere) {
                await relatedModel.delete(relatedWhere);
            }
        }
        
        // Finally, delete the main records
        return remove(where, options);
    }
    
    /**
     * Delete by primary key with cascade to related tables
     */
    async function deleteByIdWithCascade(
        id: unknown,
        options: ModelFindOptions & { cascade?: string[] } = {}
    ): Promise<QueryResultRow | null> {
        const rows = await deleteWithCascade({ [idColumn]: id }, options);
        return rows[0] ?? null;
    }

    /**
     * Restore soft-deleted records
     */
    async function restore(where: Record<string, unknown>, options: ModelFindOptions = {}): Promise<QueryResultRow[]> {
        if (!softDeleteEnabled) {
            throw new TypeError("restore() requires softDelete to be enabled");
        }
        
        const updateData = { [softDeleteField]: null } as Record<string, unknown>;
        return update(where, updateData, options);
    }

    /**
     * Restore soft-deleted record by primary key
     */
    async function restoreById(id: unknown, options: ModelFindOptions = {}): Promise<QueryResultRow | null> {
        const rows = await restore({ [idColumn]: id }, options);
        return rows[0] ?? null;
    }

    /**
     * Load related data for records (eager loading)
     */
    async function loadRelated(
        records: QueryResultRow[],
        relationshipName: string,
        dbInstance: unknown
    ): Promise<QueryResultRow[]> {
        if (!relationships || !relationships[relationshipName]) {
            throw new TypeError(`Relationship '${relationshipName}' is not defined`);
        }
        
        const relConfig = relationships[relationshipName];
        const relatedModel = (dbInstance as any).model(relConfig.model);
        
        // Get IDs from parent records
        const parentIds = records.map(r => r[idColumn]).filter(id => id !== undefined);
        if (parentIds.length === 0) {
            return records;
        }
        
        let relatedRecords: QueryResultRow[] = [];
        
        switch (relConfig.type) {
            case 'hasMany': {
                // For hasMany: foreignKey in related table points to this table
                const fkHasMany = relConfig.foreignKey || `${table}_id`;
                relatedRecords = await relatedModel.find({ [fkHasMany]: { $in: parentIds } });
                break;
            }
            case 'belongsTo': {
                // For belongsTo: foreignKey in this table points to related table
                const fkBelongsTo = relConfig.foreignKey || `${relationshipName}_id`;
                const relatedIdsBelongsTo = records.map(r => r[fkBelongsTo]).filter(id => id !== undefined);
                relatedRecords = await relatedModel.find({ [idColumn]: { $in: relatedIdsBelongsTo } });
                break;
            }
            case 'hasOne': {
                // For hasOne: similar to hasMany but expect single record
                const fkHasOne = relConfig.foreignKey || `${table}_id`;
                relatedRecords = await relatedModel.find({ [fkHasOne]: { $in: parentIds } });
                break;
            }
            case 'manyToMany': {
                // For manyToMany: need to query through the join table
                if (!relConfig.through) {
                    throw new TypeError(`Many-to-many relationship '${relationshipName}' requires a 'through' table`);
                }
                const throughModel = (dbInstance as any).model(relConfig.through);
                const throughFk = relConfig.throughForeignKey || `${table}_id`;
                const throughOtherFk = relConfig.throughOtherKey || `${relConfig.model}_id`;
                
                const throughRecords = await throughModel.find({ [throughFk]: { $in: parentIds } });
                const relatedIdsMany = (throughRecords as QueryResultRow[]).map((r) => r[throughOtherFk]).filter((id): id is unknown => id !== undefined);
                relatedRecords = await relatedModel.find({ [idColumn]: { $in: relatedIdsMany } });
                break;
            }
        }
        
        // Map related records to parent records
        return records.map(parentRecord => {
            const parentId = parentRecord[idColumn];
            let relatedData: QueryResultRow | QueryResultRow[] | undefined;
            
            switch (relConfig.type) {
                case 'hasMany':
                case 'manyToMany':
                    relatedData = relatedRecords.filter(r => {
                        const fk = relConfig.foreignKey || `${table}_id`;
                        return r[fk] === parentId;
                    });
                    break;
                case 'belongsTo':
                case 'hasOne':
                    const fk = relConfig.foreignKey || `${relationshipName}_id`;
                    relatedData = relatedRecords.find(r => r[idColumn] === parentRecord[fk]);
                    break;
            }
            
            return {
                ...parentRecord,
                [relationshipName]: relatedData
            };
        });
    }

    /**
     * Find records and include related data (eager loading)
     */
    async function findWithIncludes(
        where: Record<string, unknown> | Array<Record<string, unknown>> = {},
        options: ExtendedModelFindOptions = {},
        include: string[] = []
    ): Promise<QueryResultRow[]> {
        if (!relationships || include.length === 0) {
            return find(where, options);
        }
        
        let records = await find(where, options);
        
        for (const relName of include) {
            records = await loadRelated(records, relName, query);
        }
        
        return records;
    }

    /**
     * Find one record and include related data (eager loading)
     */
    async function findOneWithIncludes(
        where: Record<string, unknown> | Array<Record<string, unknown>> = {},
        options: ExtendedModelFindOptions = {},
        include: string[] = []
    ): Promise<QueryResultRow | null> {
        const records = await findWithIncludes(where, { ...options, limit: 1 }, include);
        return records[0] ?? null;
    }

    /**
     * Find by primary key and include related data (eager loading)
     */
    async function findByIdWithIncludes(
        id: unknown,
        options: ExtendedModelFindOptions = {},
        include: string[] = []
    ): Promise<QueryResultRow | null> {
        return findOneWithIncludes({ [idColumn]: id }, options, include);
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
        // New methods
        findWithDeleted,
        forceDelete,
        forceDeleteById,
        restore,
        restoreById,
        // Relationship methods
        findWithIncludes,
        findOneWithIncludes,
        findByIdWithIncludes,
        // Cascade delete methods
        deleteWithCascade,
        deleteByIdWithCascade,
        // Configuration getters
        softDeleteEnabled,
        timestampsEnabled,
        hooks: hooks as ModelHooks<QueryResultRow> | undefined,
        validate: validate as ((data: Record<string, unknown>, operation: 'create' | 'update') => Record<string, unknown> | Promise<Record<string, unknown>>) | undefined,
        relationships,
        coerceEnabled,
        // Join methods
        join,
        innerJoin,
        leftJoin,
        rightJoin,
        fullJoin,
        crossJoin,
    };
}

export function createModelFactory({
    query,
    logger,
}: {
    query?: QueryRunner;
    logger?: unknown;
} = {}): ModelFactory {
    return function model(table: string, options: Partial<ModelOptions> = {}) {
        return createModel({ query, table, logger, ...options } as ModelOptions);
    };
}
