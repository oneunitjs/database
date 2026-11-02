import type { QueryBuilder as QueryBuilderType, QueryRunner, QueryLike, QueryFragment, QueryResult, QueryResultRow, SqlFragment } from "../types.js";
import { quoteIdent, quoteIdentList, raw, isFragment, sql, identifier } from "./sql.js";

const ALLOWED_OPERATORS = new Set([
    "=",
    "<>",
    "!=",
    "<",
    "<=",
    ">",
    ">=",
    "LIKE",
    "ILIKE",
    "NOT LIKE",
    "NOT ILIKE",
    "IN",
    "NOT IN",
    "IS",
    "IS NOT",
    "@>",
    "<@",
    "&&",
    "?",
    "?|",
    "?&",
    "@?",
    "~",
    "~*",
    "!~",
    "!~*",
]);

const DIRECTIONS = new Set([
    "ASC",
    "DESC",
    "ASC NULLS FIRST",
    "ASC NULLS LAST",
    "DESC NULLS FIRST",
    "DESC NULLS LAST",
]);

const JOIN_TYPES = new Set([
    "JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "FULL JOIN",
    "FULL OUTER JOIN",
    "CROSS JOIN",
    "LEFT OUTER JOIN",
    "RIGHT OUTER JOIN",
]);

class ParamCollector {
    values: unknown[] = [];

    add(value: unknown): string {
        this.values.push(value);
        return `$${this.values.length}`;
    }
}

function assertOperator(operator: string): string {
    const normalized = String(operator).trim().toUpperCase();
    if (!ALLOWED_OPERATORS.has(normalized)) {
        throw new TypeError(`Unsupported SQL operator: ${operator}`);
    }
    return normalized;
}

function assertInteger(value: unknown, name: string): number {
    const number = Number(value);
    if (!Number.isInteger(number) || number < 0) {
        throw new TypeError(`${name} must be a non-negative integer`);
    }
    return number;
}

/**
 * Compile EXPLAIN options into a PostgreSQL-compatible option string.
 * Returns empty string when no options are set (plain `EXPLAIN`).
 */
function _compileExplainOptions(options: {
    analyze?: boolean;
    verbose?: boolean;
    buffers?: boolean;
    costs?: boolean;
    timing?: boolean;
    summary?: boolean;
    settings?: boolean;
    format?: "text" | "json" | "xml" | "yaml";
}): string {
    const parts: string[] = [];
    if (options.analyze) parts.push("ANALYZE");
    if (options.buffers) parts.push("BUFFERS");
    if (options.verbose) parts.push("VERBOSE");
    if (options.costs === true) parts.push("COSTS");
    if (options.timing === true) parts.push("TIMING");
    if (options.summary === true) parts.push("SUMMARY");
    if (options.settings === true) parts.push("SETTINGS");
    if (options.format) parts.push(`FORMAT ${options.format.toUpperCase()}`);

    if (parts.length === 0) {
        return "";
    }
    return `(${parts.join(", ")})`;
}

interface WhereCondition {
    connector: "AND" | "OR";
    column?: string;
    operator?: string;
    value?: unknown;
    text?: string;
    values?: unknown[];
    // For nested groups
    conditions?: WhereCondition[];
}

interface JoinClause {
    type: string;
    table: string | QueryBuilder | QueryFragment;
    on: string | SqlFragment | string[] | QueryBuilder;
    alias?: string;
}

interface HavingCondition {
    column: string;
    operator: string;
    value: unknown;
}

interface OrderClause {
    column: string | QueryFragment | QueryBuilder;
    direction: string;
    nulls?: "FIRST" | "LAST";
}

interface ShardRoutingHint {
    shardKey?: string;
    shardId?: string;
}

// For CTEs (WITH clauses)
interface CTE {
    name: string;
    query: QueryBuilder | QueryFragment | string;
    columns?: string[];
    materialized?: boolean;
}

// For Window Functions
interface WindowDefinition {
    name: string;
    partitionBy?: string[];
    orderBy?: Array<{ column: string; direction: string }>;
    frame?: {
        type: "ROWS" | "RANGE";
        start: "UNBOUNDED PRECEDING" | "CURRENT ROW" | number | string;
        end: "UNBOUNDED FOLLOWING" | "CURRENT ROW" | number | string;
    };
}

// For CASE expressions
interface CaseExpression {
    value?: string | QueryFragment;
    when: Array<{ condition: string | QueryFragment; result: unknown }>;
    else?: unknown;
}

// For UNION/INTERSECT/EXCEPT
interface CombinedQuery {
    query: QueryBuilder;
    type: "UNION" | "UNION ALL" | "INTERSECT" | "INTERSECT ALL" | "EXCEPT" | "EXCEPT ALL";
}

/**
 * Immutable-ish fluent builder for the common CRUD shapes. It produces fully
 * parameterized statements and leaves anything complex to the caller via
 * `.whereRaw(sqlFragment)` or plain `db.query`.
 */
export class QueryBuilder implements QueryBuilderType {
    private _query: QueryRunner | undefined;
    private _logger: unknown;
    private _table: string;
    private _mode: "select" | "insert" | "update" | "delete" = "select";
    private _params: ParamCollector = new ParamCollector();
    private _wheres: WhereCondition[] = [];
    private _selects: (string | QueryFragment | QueryBuilder)[] = [];
    private _joins: JoinClause[] = [];
    private _groups: string[] = [];
    private _havings: HavingCondition[] = [];
    private _orders: OrderClause[] = [];
    private _limit: number | null = null;
    private _offset: number | null = null;
    private _distinct = false;
    private _distinctOn: string[] | null = null;
    private _insertRows: Record<string, unknown>[] | null = null;
    private _updateValues: Record<string, unknown> | null = null;
    private _conflict: unknown = null;
    private _returning: string[] | null = null;
    private _shardKey: string | undefined;
    private _shardId: string | undefined;
    private _ctes: CTE[] = [];
    private _windowDefinitions: WindowDefinition[] = [];
    private _forUpdate: boolean | string = false;
    private _forNoKeyUpdate: boolean = false;
    private _skipLocked: boolean = false;
    private _combinedQueries: CombinedQuery[] = [];
    private _queryLoggingContext: Record<string, unknown> = {};
    private _explainOptions: { analyze?: boolean; verbose?: boolean; buffers?: boolean; costs?: boolean; format?: "text" | "json" | "xml" | "yaml"; timing?: boolean; summary?: boolean; settings?: boolean } | null = null;

    constructor({
        query,
        table,
        logger,
        shardKey,
        shardId,
    }: {
        query?: QueryRunner;
        table: string;
        logger?: unknown;
        shardKey?: string;
        shardId?: string;
    } = { table: "" }) {
        if (!table) {
            throw new TypeError("QueryBuilder requires a table name");
        }
        this._query = query;
        this._logger = logger;
        this._table = table;
        this._shardKey = shardKey;
        this._shardId = shardId;
        this._params = new ParamCollector();
    }

    clone(): QueryBuilder {
        const next = new QueryBuilder({ query: this._query, table: this._table, logger: this._logger });
        next._mode = this._mode;
        next._params = new ParamCollector();
        next._params.values = [...this._params.values];
        next._wheres = this._wheres.map(w => ({
            ...w,
            conditions: w.conditions ? w.conditions.map(c => ({ ...c })) : undefined
        }));
        next._selects = this._selects.map(s => {
            if (s instanceof QueryBuilder) {
                return s.clone();
            }
            return s;
        });
        next._joins = this._joins.map(j => ({
            ...j,
            table: j.table instanceof QueryBuilder ? j.table.clone() : j.table,
            on: j.on instanceof QueryBuilder ? j.on.clone() : j.on
        }));
        next._groups = [...this._groups];
        next._havings = [...this._havings];
        next._orders = this._orders.map(o => ({
            ...o,
            column: o.column instanceof QueryBuilder ? o.column.clone() : o.column
        }));
        next._limit = this._limit;
        next._offset = this._offset;
        next._distinct = this._distinct;
        next._distinctOn = this._distinctOn ? [...this._distinctOn] : null;
        next._insertRows = this._insertRows;
        next._updateValues = this._updateValues;
        next._conflict = this._conflict;
        next._returning = this._returning;
        next._shardKey = this._shardKey;
        next._shardId = this._shardId;
        next._ctes = this._ctes.map(cte => ({
            ...cte,
            query: cte.query instanceof QueryBuilder ? cte.query.clone() : cte.query
        }));
        next._windowDefinitions = this._windowDefinitions.map(w => ({ ...w }));
        next._forUpdate = this._forUpdate;
        next._forNoKeyUpdate = this._forNoKeyUpdate;
        next._skipLocked = this._skipLocked;
        next._combinedQueries = this._combinedQueries.map(c => ({
            ...c,
            query: c.query instanceof QueryBuilder ? c.query.clone() : c.query
        }));
        next._queryLoggingContext = { ...this._queryLoggingContext };
        next._explainOptions = this._explainOptions;
        return next;
    }

    // ==================== CTE (WITH clause) Support ====================

    /**
     * Add a Common Table Expression (CTE) to the query.
     * Can be chained to add multiple CTEs.
     *
     * @example
     * db.select().from('users')
     *   .with('active_users', db.select().from('users').where({ active: true }))
     *   .with('recent_orders', db.select().from('orders').where({ created_at: '> 2023-01-01' }))
     *   .select('au.name', 'ro.amount')
     *   .from('active_users au')
     *   .join('recent_orders ro', 'au.id = ro.user_id')
     */
    with(name: string, query: QueryBuilder | QueryFragment | string, columns?: string[], materialized = false): this {
        this._ctes.push({ name, query, columns, materialized });
        return this;
    }

    /**
     * Add a recursive CTE (for PostgreSQL).
     */
    withRecursive(name: string, query: QueryBuilder | QueryFragment | string, columns?: string[]): this {
        const cte: CTE = { name, query, columns, materialized: false };
        // Mark as recursive by adding to the beginning
        this._ctes.unshift(cte);
        return this;
    }

    // ==================== Window Functions Support ====================

    /**
     * Define a window function for reuse in SELECT or ORDER BY.
     *
     * @example
     * db.select()
     *   .window('ranking', { partitionBy: ['department'], orderBy: [{ column: 'salary', direction: 'DESC' }] })
     *   .select('name', 'salary', sql`RANK() OVER ranking`)
     *   .from('employees')
     */
    window(name: string, definition: {
        partitionBy?: string[];
        orderBy?: Array<{ column: string; direction?: string }>;
        frame?: {
            type: "ROWS" | "RANGE";
            start: "UNBOUNDED PRECEDING" | "CURRENT ROW" | number | string;
            end: "UNBOUNDED FOLLOWING" | "CURRENT ROW" | number | string;
        };
    }): this {
        this._windowDefinitions.push({
            name,
            partitionBy: definition.partitionBy,
            orderBy: definition.orderBy?.map(o => ({ 
                column: o.column, 
                direction: o.direction?.toUpperCase() || "ASC" 
            })),
            frame: definition.frame,
        });
        return this;
    }

    // ==================== Complex Joins Support ====================

    join(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "JOIN", table, on, alias });
        return this;
    }

    innerJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "INNER JOIN", table, on, alias });
        return this;
    }

    leftJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "LEFT JOIN", table, on, alias });
        return this;
    }

    rightJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "RIGHT JOIN", table, on, alias });
        return this;
    }

    fullJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "FULL JOIN", table, on, alias });
        return this;
    }

    fullOuterJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "FULL OUTER JOIN", table, on, alias });
        return this;
    }

    leftOuterJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "LEFT OUTER JOIN", table, on, alias });
        return this;
    }

    rightOuterJoin(table: string | QueryBuilder, on: string | SqlFragment | string[] | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "RIGHT OUTER JOIN", table, on, alias });
        return this;
    }

    crossJoin(table: string | QueryBuilder, alias?: string): this {
        this._joins.push({ type: "CROSS JOIN", table, on: "", alias });
        return this;
    }

    // ==================== Subqueries Support ====================

    /**
     * Use a subquery in the FROM clause.
     *
     * @example
     * db.select('name')
     *   .from(db.select().from('users').where({ active: true }), 'active_users')
     */
    from(subquery: QueryBuilder | QueryFragment | string, alias?: string): this {
        if (subquery instanceof QueryBuilder) {
            this._table = `(${subquery.toSQL().text})`;
            if (alias) {
                this._table += ` AS ${quoteIdent(alias)}`;
            }
        } else if (isFragment(subquery)) {
            this._table = `(${subquery.text})`;
            if (alias) {
                this._table += ` AS ${quoteIdent(alias)}`;
            }
        } else {
            this._table = String(subquery);
        }
        return this;
    }

    /**
     * Add a subquery to the SELECT clause.
     *
     * @example
     * db.select(
     *   'users.name',
     *   db.select(sql`COUNT(*)`).from('orders').where('orders.user_id = users.id'), 'order_count'
     * ).from('users')
     */
    selectSubquery(subquery: QueryBuilder | QueryFragment, alias: string): this {
        if (subquery instanceof QueryBuilder) {
            const { text, values } = subquery.toSQL();
            this._selects.push(sql`(${raw(text)}) AS ${identifier(alias)}`);
            // Note: values are not merged here - need to handle in compilation
        } else {
            this._selects.push(sql`(${subquery}) AS ${identifier(alias)}`);
        }
        return this;
    }

    /**
     * Use a subquery in WHERE clause with EXISTS.
     *
     * @example
     * db.select().from('users')
     *   .whereExists(db.select().from('orders').where('orders.user_id = users.id'))
     */
    whereExists(subquery: QueryBuilder | QueryFragment): this {
        if (subquery instanceof QueryBuilder) {
            const { text } = subquery.toSQL();
            this._wheres.push({ connector: "AND", text: `EXISTS (${text})`, values: [] });
        } else if (isFragment(subquery)) {
            this._wheres.push({ connector: "AND", text: `EXISTS (${subquery.text})`, values: subquery.values ?? [] });
        }
        return this;
    }

    /**
     * Use a subquery in WHERE clause with NOT EXISTS.
     */
    whereNotExists(subquery: QueryBuilder | QueryFragment): this {
        if (subquery instanceof QueryBuilder) {
            const { text } = subquery.toSQL();
            this._wheres.push({ connector: "AND", text: `NOT EXISTS (${text})`, values: [] });
        } else if (isFragment(subquery)) {
            this._wheres.push({ connector: "AND", text: `NOT EXISTS (${subquery.text})`, values: subquery.values ?? [] });
        }
        return this;
    }

    /**
     * Use a subquery in WHERE clause with IN.
     *
     * @example
     * db.select().from('users')
     *   .whereInSubquery('id', db.select('user_id').from('orders'))
     */
    whereInSubquery(column: string, subquery: QueryBuilder | QueryFragment): this {
        if (subquery instanceof QueryBuilder) {
            const { text } = subquery.toSQL();
            this._wheres.push({ 
                connector: "AND", 
                text: `${quoteIdent(column)} IN (${text})`, 
                values: [] 
            });
        } else if (isFragment(subquery)) {
            this._wheres.push({ 
                connector: "AND", 
                text: `${quoteIdent(column)} IN (${subquery.text})`, 
                values: subquery.values ?? [] 
            });
        }
        return this;
    }

    /**
     * Use a subquery in WHERE clause with NOT IN.
     */
    whereNotInSubquery(column: string, subquery: QueryBuilder | QueryFragment): this {
        if (subquery instanceof QueryBuilder) {
            const { text } = subquery.toSQL();
            this._wheres.push({ 
                connector: "AND", 
                text: `${quoteIdent(column)} NOT IN (${text})`, 
                values: [] 
            });
        } else if (isFragment(subquery)) {
            this._wheres.push({ 
                connector: "AND", 
                text: `${quoteIdent(column)} NOT IN (${subquery.text})`, 
                values: subquery.values ?? [] 
            });
        }
        return this;
    }

    // ==================== Nested WHERE Groups Support ====================

    /**
     * Start a nested WHERE group with AND connector.
     *
     * @example
     * db.select().from('users')
     *   .whereGroup(q => q.where('status', 'active').orWhere('status', 'pending'))
     *   .where('age', '>', 18)
     * // WHERE (status = 'active' OR status = 'pending') AND age > 18
     */
    whereGroup(callback: (builder: QueryBuilder) => QueryBuilder): this {
        const subBuilder = new QueryBuilder({ table: this._table });
        callback(subBuilder);
        
        // Extract the conditions from the sub-builder
        const conditions = subBuilder._wheres.map(w => ({ ...w }));
        
        // Wrap in a nested condition
        this._wheres.push({
            connector: "AND",
            conditions,
        });
        
        return this;
    }

    /**
     * Start a nested WHERE group with OR connector.
     */
    orWhereGroup(callback: (builder: QueryBuilder) => QueryBuilder): this {
        const subBuilder = new QueryBuilder({ table: this._table });
        callback(subBuilder);
        
        const conditions = subBuilder._wheres.map(w => ({ ...w }));
        
        this._wheres.push({
            connector: "OR",
            conditions,
        });
        
        return this;
    }

    // ==================== UNION/INTERSECT/EXCEPT Support ====================

    /**
     * Combine this query with another using UNION.
     */
    union(query: QueryBuilder): this {
        this._combinedQueries.push({ query: query.clone(), type: "UNION" });
        return this;
    }

    /**
     * Combine this query with another using UNION ALL.
     */
    unionAll(query: QueryBuilder): this {
        this._combinedQueries.push({ query: query.clone(), type: "UNION ALL" });
        return this;
    }

    /**
     * Combine this query with another using INTERSECT.
     */
    intersect(query: QueryBuilder): this {
        this._combinedQueries.push({ query: query.clone(), type: "INTERSECT" });
        return this;
    }

    /**
     * Combine this query with another using INTERSECT ALL.
     */
    intersectAll(query: QueryBuilder): this {
        this._combinedQueries.push({ query: query.clone(), type: "INTERSECT ALL" });
        return this;
    }

    /**
     * Combine this query with another using EXCEPT.
     */
    except(query: QueryBuilder): this {
        this._combinedQueries.push({ query: query.clone(), type: "EXCEPT" });
        return this;
    }

    /**
     * Combine this query with another using EXCEPT ALL.
     */
    exceptAll(query: QueryBuilder): this {
        this._combinedQueries.push({ query: query.clone(), type: "EXCEPT ALL" });
        return this;
    }

    // ==================== CASE Expression Support ====================

    /**
     * Add a CASE expression to the SELECT clause.
     *
     * @example
     * db.select()
     *   .selectCase('status_category', (caseBuilder) => caseBuilder
     *     .when('status = \'active\'', 'Active')
     *     .when('status = \'pending\'', 'Pending')
     *     .else('Inactive')
     *   )
     *   .from('users')
     */
    selectCase(alias: string, callback: (builder: CaseBuilder) => CaseBuilder): this {
        const caseBuilder = new CaseBuilder();
        callback(caseBuilder);
        this._selects.push(sql`${caseBuilder.build()} AS ${identifier(alias)}`);
        return this;
    }

    // ==================== DISTINCT ON Support ====================

    /**
     * Use PostgreSQL's DISTINCT ON.
     *
     * @example
     * db.select().from('orders')
     *   .distinctOn(['user_id'])
     *   .orderBy('user_id', 'created_at DESC')
     *   .select('user_id', 'order_date', 'amount')
     */
    distinctOn(columns: string[]): this {
        this._distinct = true;
        this._distinctOn = columns;
        return this;
    }

    // ==================== FOR UPDATE Support ====================

    /**
     * Lock rows for update (SELECT ... FOR UPDATE).
     *
     * @example
     * db.select().from('accounts').where({ id: 1 }).forUpdate()
     */
    forUpdate(): this {
        this._forUpdate = true;
        return this;
    }

    /**
     * Lock rows for update without key lock (SELECT ... FOR NO KEY UPDATE).
     */
    forNoKeyUpdate(): this {
        this._forNoKeyUpdate = true;
        return this;
    }

    /**
     * Skip locked rows (SELECT ... FOR UPDATE SKIP LOCKED).
     */
    skipLocked(): this {
        this._skipLocked = true;
        return this;
    }

    // ==================== Keyset Pagination ====================

    /**
     * Apply keyset pagination (seek method) using the last seen row values.
     * More efficient than OFFSET for deep pagination because it avoids
     * scanning and discarding skipped rows.
     *
     * Requires at least one `.orderBy()` clause. For each order column,
     * adds a WHERE condition using `>` for ASC and `<` for DESC.
     *
     * @example
     * // Page 2: get rows after the last seen id
     * db.select().from('users')
     *   .orderBy('id', 'ASC')
     *   .keysetPagination({ id: lastId })
     *   .limit(20);
     * // => SELECT * FROM "users" WHERE "id" > $1 ORDER BY "id" ASC LIMIT $2
     *
     * @example
     * // Composite keyset (id ASC, created_at DESC)
     * db.select().from('orders')
     *   .orderBy('id', 'ASC')
     *   .orderBy('created_at', 'DESC')
     *   .keysetPagination({ id: lastId, created_at: lastDate })
     *   .limit(20);
     */
    keysetPagination(lastValues: Record<string, unknown>): this {
        if (this._orders.length === 0) {
            throw new TypeError("keysetPagination requires at least one orderBy clause");
        }

        const conditions: Array<{ column: string; operator: string; value: unknown }> = [];

        for (const order of this._orders) {
            const column = typeof order.column === "string" ? order.column : String(order.column);
            const value = lastValues[column];

            if (value === undefined) {
                throw new TypeError(
                    `keysetPagination requires a value for order column "${column}"`,
                );
            }

            const isDesc = order.direction.startsWith("DESC");
            const operator = isDesc ? "<" : ">";

            conditions.push({ column, operator, value });
        }

        // Build composite keyset condition: (c1 > v1) OR (c1 = v1 AND c2 > v2) OR ...
        // For single column: simply c1 > v1
        if (conditions.length === 1) {
            const { column, operator, value } = conditions[0];
            this._wheres.push({
                connector: "AND",
                column,
                operator,
                value,
            });
        } else {
            // Composite: build OR of AND groups
            const parts: string[] = [];
            const paramValues: unknown[] = [];

            for (let i = 0; i < conditions.length; i++) {
                const andParts: string[] = [];
                // All previous columns must be equal
                for (let j = 0; j < i; j++) {
                    const prev = conditions[j];
                    paramValues.push(prev.value);
                    andParts.push(`${quoteIdent(prev.column)} = $${paramValues.length}`);
                }
                // Current column uses > or <
                const curr = conditions[i];
                paramValues.push(curr.value);
                andParts.push(`${quoteIdent(curr.column)} ${curr.operator} $${paramValues.length}`);
                parts.push(`(${andParts.join(" AND ")})`);
            }

            const text = parts.join(" OR ");
            this._wheres.push({
                connector: "AND",
                text,
                values: paramValues,
            });
        }

        return this;
    }

    // ==================== JSON Operators Support ====================

    /**
     * Add a JSON path query condition.
     *
     * @example
     * db.select().from('users')
     *   .whereJson('metadata->>\'color\'', '=', 'blue')
     */
    whereJson(column: string, operator: string, value: unknown): this {
        const op = assertOperator(operator);
        this._wheres.push({ 
            connector: "AND", 
            text: `${quoteIdent(column)} ${op} $X`, 
            value 
        });
        return this;
    }

    /**
     * Check if JSON field contains a value.
     *
     * @example
     * db.select().from('users')
     *   .whereJsonContains('metadata', { color: 'blue' })
     */
    whereJsonContains(column: string, value: unknown): this {
        this._wheres.push({ 
            connector: "AND", 
            text: `${quoteIdent(column)} @> $X`,
            value 
        });
        return this;
    }

    /**
     * Check if JSON field is contained in a value.
     */
    whereJsonContained(column: string, value: unknown): this {
        this._wheres.push({ 
            connector: "AND", 
            text: `${quoteIdent(column)} <@ $X`,
            value 
        });
        return this;
    }

    // ==================== Query Logging Context ====================

    /**
     * Add context for query logging (file, line, request ID, etc.).
     */
    withQueryContext(context: Record<string, unknown>): this {
        this._queryLoggingContext = { ...this._queryLoggingContext, ...context };
        return this;
    }

    /**
     * Get the current query logging context.
     */
    getQueryContext(): Record<string, unknown> {
        return { ...this._queryLoggingContext };
    }

    // ==================== Transaction Timeout ====================

    /**
     * Set a timeout for the query execution (in milliseconds).
     */
    timeout(ms: number): this {
        // This will be handled by the query execution layer
        this.withQueryContext({ timeout: ms });
        return this;
    }

    // ==================== Query Execution Plan (EXPLAIN) ====================

    /**
     * Prefix the compiled query with `EXPLAIN` to inspect its execution plan.
     *
     * @example
     * const { text, values } = db.select().from('users').where({ id: 1 }).explain().toSQL();
     * // text => "EXPLAIN SELECT * FROM \"users\" WHERE \"id\" = $1"
     *
     * @example
     * const { text } = db.select().from('users').explain({ analyze: true, buffers: true, format: 'json' }).toSQL();
     * // text => "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM \"users\""
     */
    explain(options: {
        analyze?: boolean;
        verbose?: boolean;
        buffers?: boolean;
        costs?: boolean;
        timing?: boolean;
        summary?: boolean;
        settings?: boolean;
        format?: "text" | "json" | "xml" | "yaml";
    } = {}): this {
        this._explainOptions = options;
        return this;
    }

    // ==================== Original Methods ====================

    select(...columns: Array<string | string[] | SqlFragment>): this {
        this._mode = "select";
        let processedColumns: (string | SqlFragment | QueryBuilder)[];
        if (columns.length === 1 && Array.isArray(columns[0])) {
            processedColumns = columns[0] as (string | SqlFragment | QueryBuilder)[];
        } else {
            processedColumns = columns as (string | SqlFragment | QueryBuilder)[];
        }
        this._selects.push(...(processedColumns.length ? processedColumns : ["*"]));
        return this;
    }

    distinct(value = true): this {
        this._distinct = value;
        if (value) {
            this._distinctOn = null;
        }
        return this;
    }

    insert(values: Record<string, unknown> | Record<string, unknown>[]): this {
        this._mode = "insert";
        this._insertRows = Array.isArray(values) ? values : [values];
        return this;
    }

    values(values: Record<string, unknown> | Record<string, unknown>[]): this {
        return this.insert(values);
    }

    update(values: Record<string, unknown>): this {
        this._mode = "update";
        this._updateValues = values;
        return this;
    }

    delete(): this {
        this._mode = "delete";
        return this;
    }

    returning(...columns: (string | string[])[]): this {
        this._returning = columns.length === 1 && Array.isArray(columns[0]) ? columns[0] : (columns as string[]);
        return this;
    }

    where(column: string | Record<string, unknown> | QueryFragment, operator?: string, value?: unknown): this {
        if (column && typeof column === "object" && !isFragment(column)) {
            if ("text" in column && typeof column.text === "string" && "values" in column) {
                const frag = column as { text: string; values?: unknown[] };
                this._wheres.push({ connector: "AND", text: frag.text, values: frag.values ?? [] });
                return this;
            }
            for (const [key, item] of Object.entries(column)) {
                this.where(key, item as string | undefined);
            }
            return this;
        }

        if (value === undefined) {
            value = operator;
            operator = "=";
        }

        if (value === undefined || value === null) {
            return this.whereNull(column as string);
        }

        if (Array.isArray(value)) {
            return this.whereIn(column as string, value);
        }

        const op = assertOperator(operator ?? "=");
        this._wheres.push({ connector: "AND", column: column as string, operator: op, value });
        return this;
    }

    orWhere(column: string, operator?: string, value?: unknown): this {
        if (value === undefined) {
            value = operator;
            operator = "=";
        }

        if (value === null || value === undefined) {
            this._wheres.push({ connector: "OR", column, operator: "IS", value: null });
            return this;
        }

        if (Array.isArray(value)) {
            this._wheres.push({ connector: "OR", column, operator: "IN", value });
            return this;
        }

        this._wheres.push({ connector: "OR", column, operator: assertOperator(operator ?? "="), value });
        return this;
    }

    whereNull(column: string): this {
        this._wheres.push({ connector: "AND", column, operator: "IS", value: null });
        return this;
    }

    whereNotNull(column: string): this {
        this._wheres.push({ connector: "AND", column, operator: "IS NOT", value: null });
        return this;
    }

    whereIn(column: string, values: unknown[]): this {
        this._wheres.push({ connector: "AND", column, operator: "IN", value: values });
        return this;
    }

    whereNotIn(column: string, values: unknown[]): this {
        this._wheres.push({ connector: "AND", column, operator: "NOT IN", value: values });
        return this;
    }

    whereRaw(fragment: QueryFragment): this {
        this._wheres.push({ connector: "AND", text: fragment.text, values: fragment.values ?? [] });
        return this;
    }

    groupBy(...columns: (string | string[])[]): this {
        let groups: string[];
        if (columns.length === 1 && Array.isArray(columns[0])) {
            groups = columns[0] as string[];
        } else {
            groups = columns as string[];
        }
        this._groups.push(...groups);
        return this;
    }

    having(column: string, operator: string, value: unknown): this {
        this._havings.push({ column, operator: assertOperator(operator), value });
        return this;
    }

    orderBy(column: string | SqlFragment | QueryBuilder, direction = "ASC"): this {
        const dir = String(direction).toUpperCase();
        if (!DIRECTIONS.has(dir)) {
            throw new TypeError(`Invalid order direction: ${direction}`);
        }
        this._orders.push({ column, direction: dir });
        return this;
    }

    limit(value: number): this {
        this._limit = assertInteger(value, "limit");
        return this;
    }

    offset(value: number): this {
        this._offset = assertInteger(value, "offset");
        return this;
    }

    onConflict(options: Record<string, unknown>): this {
        this._conflict = options;
        return this;
    }

    /**
     * Pin this builder (and all clones) to a specific shard key. The shard key
     * is merged into execution options at `.execute()` time and routed through
     * the unified executor. Calling `withShard()` after routing has already
     * been established replaces the previous shard key.
     */
    withShard(shardKey: string): this {
        this._shardKey = shardKey;
        this._shardId = undefined;
        return this;
    }

    /**
     * Pin this builder (and all clones) to a specific shard id.
     */
    withShardId(shardId: string): this {
        this._shardId = shardId;
        this._shardKey = undefined;
        return this;
    }

    // ==================== Compilation Methods ====================

    private _compileCTEs(): string {
        if (this._ctes.length === 0) {
            return "";
        }

        const parts: string[] = [];
        for (const cte of this._ctes) {
            let queryText: string;
            
            if (cte.query instanceof QueryBuilder) {
                const subBuilder = cte.query._buildCloneWithFreshParams();
                queryText = subBuilder._compile();
                // Merge parameters
                this._params.values.push(...subBuilder._params.values);
            } else if (isFragment(cte.query)) {
                queryText = cte.query.text;
                this._params.values.push(...(cte.query.values ?? []));
            } else {
                queryText = String(cte.query);
            }

            let cteText = `${quoteIdent(cte.name)} AS (${queryText})`;
            
            if (cte.materialized) {
                cteText = `${quoteIdent(cte.name)} AS MATERIALIZED (${queryText})`;
            }
            
            if (cte.columns && cte.columns.length > 0) {
                cteText = `${quoteIdent(cte.name)} (${quoteIdentList(cte.columns)}) AS (${queryText})`;
            }
            
            parts.push(cteText);
        }

        return `WITH ${parts.join(", ")} `;
    }

    private _compileWindowDefinitions(): string {
        // Window definitions are referenced in SELECT, not declared separately
        return "";
    }

    private _compileSelectList(): string {
        const columns = this._selects.length ? this._selects : ["*"];
        
        const projection = columns.map((column) => {
            if (column instanceof QueryBuilder) {
                const subBuilder = column._buildCloneWithFreshParams();
                const text = subBuilder._compile();
                this._params.values.push(...subBuilder._params.values);
                return `(${text})`;
            }
            
            if (column && typeof column === "object" && "text" in column) {
                const frag = column as QueryFragment;
                // Handle parameter renumbering
                let text = frag.text;
                const offset = this._params.values.length;
                
                if (frag.values && frag.values.length > 0) {
                    text = text.replace(/\$(\d+)/g, (_, position) => {
                        const idx = Number(position) - 1;
                        if (idx < frag.values!.length) {
                            const paramIndex = offset + idx + 1;
                            this._params.values.push(frag.values![idx]);
                            return `$${paramIndex}`;
                        }
                        return `$${position}`;
                    });
                }
                return text;
            }
            
            if (column === "*") {
                return "*";
            }
            
            return quoteIdent(column as string);
        }).join(", ");

        return projection;
    }

    private _compileFrom(): string {
        // If we used .from() with a subquery, _table already contains the subquery
        if (typeof this._table === "string" && this._table.startsWith("(")) {
            return `FROM ${this._table}`;
        }
        return `FROM ${quoteIdent(this._table)}`;
    }

    private _compileJoins(): string {
        if (this._joins.length === 0) {
            return "";
        }

        const parts: string[] = [];
        
        for (const join of this._joins) {
            let tableText: string;
            let joinType = join.type;
            
            // Normalize join type
            if (!JOIN_TYPES.has(joinType)) {
                throw new TypeError(`Invalid join type: ${joinType}`);
            }

            // Handle table (could be a subquery or table name)
            if (join.table instanceof QueryBuilder) {
                const subBuilder = join.table._buildCloneWithFreshParams();
                tableText = `(${subBuilder._compile()})`;
                this._params.values.push(...subBuilder._params.values);
            } else if (isFragment(join.table)) {
                tableText = join.table.text;
                this._params.values.push(...(join.table.values ?? []));
            } else {
                tableText = quoteIdent(join.table as string);
            }

            // Add alias if provided
            if (join.alias) {
                tableText += ` AS ${quoteIdent(join.alias)}`;
            }

            // Handle ON clause
            let onText = "";
            if (join.on) {
                if (join.on instanceof QueryBuilder) {
                    const subBuilder = join.on._buildCloneWithFreshParams();
                    const onSql = subBuilder._compile();
                    this._params.values.push(...subBuilder._params.values);
                    onText = ` ON ${onSql}`;
                } else if (isFragment(join.on)) {
                    let text = join.on.text;
                    const offset = this._params.values.length;
                    
                    if (join.on.values && join.on.values.length > 0) {
                        text = text.replace(/\$(\d+)/g, (_, position) => {
                            const idx = Number(position) - 1;
                            if (idx < (join.on as QueryFragment).values!.length) {
                                const paramIndex = offset + idx + 1;
                                this._params.values.push((join.on as QueryFragment).values![idx]);
                                return `$${paramIndex}`;
                            }
                            return `$${position}`;
                        });
                    }
                    onText = ` ON ${text}`;
                } else if (Array.isArray(join.on)) {
                    // Handle array of conditions
                    const conditions = join.on.map(cond => {
                        if (typeof cond === "string") {
                            return cond;
                        }
                        return "";
                    }).filter(c => c).join(" AND ");
                    onText = conditions ? ` ON ${conditions}` : "";
                } else if (typeof join.on === "string") {
                    onText = join.on ? ` ON ${join.on}` : "";
                }
            }

            // CROSS JOIN doesn't have ON clause
            if (joinType.includes("CROSS")) {
                onText = "";
            }

            parts.push(` ${joinType} ${tableText}${onText}`);
        }

        return parts.join("");
    }

    private _compileWhere(): string {
        if (this._wheres.length === 0) {
            return "";
        }

        const parts = this._wheres.map((condition, index) => {
            let text = "";

            if (condition.conditions) {
                // Nested condition group
                const nestedParts = condition.conditions.map((nestedCond, nestedIndex) => {
                    let nestedText = "";
                    
                    if (nestedCond.text !== undefined) {
                        nestedText = nestedCond.text;
                        const offset = this._params.values.length;
                        if (nestedCond.values && nestedCond.values.length > 0) {
                            nestedText = nestedText.replace(/\$(\d+)/g, (_, position) => {
                                const idx = Number(position) - 1;
                                if (idx < nestedCond.values!.length) {
                                    const paramIndex = offset + idx + 1;
                                    this._params.values.push(nestedCond.values![idx]);
                                    return `$${paramIndex}`;
                                }
                                return `$${position}`;
                            });
                        }
                    } else {
                        const column = quoteIdent(nestedCond.column ?? "");
                        const operator = nestedCond.operator;

                        if (operator === "IS" || operator === "IS NOT") {
                            nestedText = nestedCond.value === null ? `${column} ${operator} NULL` : `${column} ${operator} $X`;
                            if (nestedCond.value !== null) {
                                nestedText = `${column} ${operator} ${this._params.add(nestedCond.value)}`;
                            }
                        } else if (operator === "IN" || operator === "NOT IN") {
                            const val = nestedCond.value;
                            if (!Array.isArray(val) || val.length === 0) {
                                nestedText = operator === "IN" ? "FALSE" : "TRUE";
                            } else {
                                const placeholders = val.map((v) => this._params.add(v));
                                nestedText = `${column} ${operator} (${placeholders.join(", ")})`;
                            }
                        } else {
                            nestedText = `${column} ${operator} ${this._params.add(nestedCond.value)}`;
                        }
                    }
                    return nestedIndex === 0 ? nestedText : `${condition.connector} ${nestedText}`;
                });
                
                text = `(${nestedParts.join(" ")})`;
            } else if (condition.text !== undefined) {
                text = condition.text;
                const offset = this._params.values.length;
                if (condition.values && condition.values.length > 0) {
                    text = text.replace(/\$(\d+)/g, (_, position) => {
                        const idx = Number(position) - 1;
                        if (idx < condition.values!.length) {
                            const paramIndex = offset + idx + 1;
                            this._params.values.push(condition.values![idx]);
                            return `$${paramIndex}`;
                        }
                        return `$${position}`;
                    });
                }
            } else {
                const column = quoteIdent(condition.column ?? "");
                const operator = condition.operator;

                if (operator === "IS" || operator === "IS NOT") {
                    text = condition.value === null ? `${column} ${operator} NULL` : `${column} ${operator} $X`;
                    if (condition.value !== null) {
                        text = `${column} ${operator} ${this._params.add(condition.value)}`;
                    }
                } else if (operator === "IN" || operator === "NOT IN") {
                    const val = condition.value;
                    if (!Array.isArray(val) || val.length === 0) {
                        text = operator === "IN" ? "FALSE" : "TRUE";
                    } else {
                        const placeholders = val.map((v) => this._params.add(v));
                        text = `${column} ${operator} (${placeholders.join(", ")})`;
                    }
                } else {
                    text = `${column} ${operator} ${this._params.add(condition.value)}`;
                }
            }

            return index === 0 ? text : `${condition.connector} ${text}`;
        });

        return ` WHERE ${parts.join(" ")}`;
    }

    private _compileGroupBy(): string {
        if (this._groups.length === 0) {
            return "";
        }
        return ` GROUP BY ${quoteIdentList(this._groups)}`;
    }

    private _compileHaving(): string {
        if (this._havings.length === 0) {
            return "";
        }
        return ` HAVING ${this._havings.map(h => `${quoteIdent(h.column)} ${h.operator} ${this._params.add(h.value)}`).join(" AND ")}`;
    }

    private _compileOrderBy(): string {
        if (this._orders.length === 0) {
            return "";
        }
        return ` ORDER BY ${this._orders.map(order => {
            if (isFragment(order.column)) {
                let text = order.column.text;
                const offset = this._params.values.length;
                if (order.column.values && order.column.values.length > 0) {
                    text = text.replace(/\$(\d+)/g, (_, position) => {
                        const idx = Number(position) - 1;
                        if (idx < (order.column as QueryFragment).values!.length) {
                            const paramIndex = offset + idx + 1;
                            this._params.values.push((order.column as QueryFragment).values![idx]);
                            return `$${paramIndex}`;
                        }
                        return `$${position}`;
                    });
                }
                return `${text} ${order.direction}`;
            } else if (order.column instanceof QueryBuilder) {
                const subBuilder = order.column._buildCloneWithFreshParams();
                const text = subBuilder._compile();
                this._params.values.push(...subBuilder._params.values);
                return `(${text}) ${order.direction}`;
            }
            return `${quoteIdent(order.column as string)} ${order.direction}`;
        }).join(", ")}`;
    }

    private _compileLimit(): string {
        if (this._limit !== null) {
            return ` LIMIT ${this._params.add(this._limit)}`;
        }
        return "";
    }

    private _compileOffset(): string {
        if (this._offset !== null) {
            return ` OFFSET ${this._params.add(this._offset)}`;
        }
        return "";
    }

    private _compileDistinct(): string {
        if (this._distinctOn) {
            return `DISTINCT ON (${quoteIdentList(this._distinctOn)}) `;
        }
        if (this._distinct) {
            return "DISTINCT ";
        }
        return "";
    }

    private _compileReturning(): string {
        if (!this._returning) {
            return "";
        }
        const columns = Array.isArray(this._returning) ? this._returning : [this._returning];
        return ` RETURNING ${quoteIdentList(columns)}`;
    }

    private _compileForUpdate(): string {
        if (!this._forUpdate && !this._forNoKeyUpdate) {
            return "";
        }

        let text = " FOR ";
        if (this._forNoKeyUpdate) {
            text += "NO KEY ";
        }
        text += "UPDATE";

        if (this._skipLocked) {
            text += " SKIP LOCKED";
        }

        return text;
    }

    private _compileConflict(): string {
        const conflict = this._conflict;
        if (!conflict) {
            return "";
        }

        if (conflict === true || conflict === "nothing") {
            return " ON CONFLICT DO NOTHING";
        }

        let text = " ON CONFLICT";
        const c = conflict as Record<string, unknown>;

        if (c.constraint) {
            text += ` ON CONSTRAINT ${quoteIdent(String(c.constraint))}`;
        } else if (c.columns && Array.isArray(c.columns) && c.columns.length) {
            text += ` (${quoteIdentList(c.columns as string[])})`;
        }

        if (c.do === "nothing" || c.action === "nothing") {
            text += " DO NOTHING";
        } else if (c.update && Array.isArray(c.update) && c.update.length) {
            const updateColumns = (c.update as string[]).filter((column) => c.id ? column !== c.id : true);
            const assignments = updateColumns
                .map((column) => `${quoteIdent(column)} = EXCLUDED.${quoteIdent(column)}`)
                .join(", ");
            text += ` DO UPDATE SET ${assignments}`;
        } else if (c.columns && Array.isArray(c.columns) && c.columns.length) {
            const updateColumns = (c.columns as string[]).filter((column) => c.id ? column !== c.id : true);
            const assignments = updateColumns
                .map((column) => `${quoteIdent(column)} = EXCLUDED.${quoteIdent(column)}`)
                .join(", ");
            text += ` DO UPDATE SET ${assignments}`;
        } else {
            text += " DO NOTHING";
        }

        return text;
    }

    private _compileCombinedQueries(): string {
        if (this._combinedQueries.length === 0) {
            return "";
        }

        const parts = this._combinedQueries.map((combined) => {
            const subBuilder = combined.query._buildCloneWithFreshParams();
            const text = subBuilder._compile();
            this._params.values.push(...subBuilder._params.values);
            return ` ${combined.type} ${text}`;
        });

        return parts.join("");
    }

    private _compileInsert(): string {
        const rows = this._insertRows ?? [];
        if (rows.length === 0) {
            throw new TypeError("insert requires at least one row");
        }

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

        const tuples = rows.map((row) => {
            const placeholders = columns.map((column) => this._params.add(row[column] ?? null));
            return `(${placeholders.join(", ")})`;
        });

        let text = `INSERT INTO ${quoteIdent(this._table)} (${quoteIdentList(columns)}) VALUES ${tuples.join(", ")}`;
        text += this._compileConflict();
        text += this._compileReturning();
        return text;
    }

    private _compileUpdate(): string {
        const values = this._updateValues ?? {};
        const columns = Object.keys(values);
        if (columns.length === 0) {
            throw new TypeError("update requires at least one column value");
        }

        const assignments = columns
            .map((column) => `${quoteIdent(column)} = ${this._params.add(values[column] ?? null)}`)
            .join(", ");

        let text = `UPDATE ${quoteIdent(this._table)} SET ${assignments}`;
        text += this._compileWhere();
        text += this._compileReturning();
        return text;
    }

    private _compileDelete(): string {
        let text = `DELETE FROM ${quoteIdent(this._table)}`;
        text += this._compileWhere();
        text += this._compileReturning();
        return text;
    }

    private _compileSelect(): string {
        const distinct = this._compileDistinct();
        const columns = this._compileSelectList();
        const from = this._compileFrom();
        const joins = this._compileJoins();
        const where = this._compileWhere();
        const groupBy = this._compileGroupBy();
        const having = this._compileHaving();
        const orderBy = this._compileOrderBy();
        const limit = this._compileLimit();
        const offset = this._compileOffset();
        const forUpdate = this._compileForUpdate();

        let text = `SELECT ${distinct}${columns} ${from}${joins}${where}${groupBy}${having}${orderBy}${limit}${offset}${forUpdate}`;
        
        return text;
    }

    toSQL(): { text: string; values: unknown[] } {
        const builder = this._buildCloneWithFreshParams();
        
        // Handle combined queries (UNION, INTERSECT, EXCEPT)
        if (builder._combinedQueries.length > 0) {
            // Get the main query SQL
            const mainSql = builder._compile();
            const combinedSql = builder._compileCombinedQueries();
            const ctesSql = builder._compileCTEs();
            
            let text = `${ctesSql}${mainSql}${combinedSql}`;
            if (builder._explainOptions) {
                const explainPrefix = _compileExplainOptions(builder._explainOptions);
                text = explainPrefix ? `EXPLAIN ${explainPrefix} ${text}` : `EXPLAIN ${text}`;
            }
            return { 
                text, 
                values: builder._params.values 
            };
        }
        
        // Normal query
        let text = builder._compile();
        if (builder._explainOptions) {
            const explainPrefix = _compileExplainOptions(builder._explainOptions);
            text = explainPrefix ? `EXPLAIN ${explainPrefix} ${text}` : `EXPLAIN ${text}`;
        }
        return { text, values: builder._params.values };
    }

    private _buildCloneWithFreshParams(): QueryBuilder {
        const clone = this.clone();
        clone._params = new ParamCollector();
        return clone;
    }

    private _compile(): string {
        const ctes = this._compileCTEs();
        
        switch (this._mode) {
            case "insert":
                return `${ctes}${this._compileInsert()}`;
            case "update":
                return `${ctes}${this._compileUpdate()}`;
            case "delete":
                return `${ctes}${this._compileDelete()}`;
            default:
                return `${ctes}${this._compileSelect()}`;
        }
    }

    async execute(queryOptions: Record<string, unknown> = {}): Promise<QueryResult> {
        const { text, values } = this.toSQL();
        const runner = this._query;
        if (typeof runner !== "function") {
            throw new TypeError("QueryBuilder was not given a query function to execute with");
        }
        
        // Add query logging context to options
        const baseOptions: Record<string, unknown> = typeof queryOptions === 'object' && queryOptions !== null ? queryOptions : {};
        const optionsWithContext = {
            ...baseOptions,
            queryContext: { 
                ...this._queryLoggingContext, 
                ...(baseOptions.queryContext as object ?? {})
            }
        };
        
        // Merge builder-level shard routing into execution options
        const mergedOptions: Record<string, unknown> = { ...optionsWithContext };
        if (this._shardKey && mergedOptions.shardKey === undefined) {
            mergedOptions.shardKey = this._shardKey;
        }
        if (this._shardId && mergedOptions.shardId === undefined) {
            mergedOptions.shardId = this._shardId;
        }
        
        // Handle timeout if set in context
        if (this._queryLoggingContext.timeout) {
            mergedOptions.timeout = this._queryLoggingContext.timeout;
        }
        
        // Support AbortSignal for query cancellation
        const signal = mergedOptions.signal as AbortSignal | undefined;
        if (signal?.aborted) {
            throw new DOMException("The operation was aborted.", "AbortError");
        }

        const result = await new Promise<QueryResult>((resolve, reject) => {
            let settled = false;
            const abortHandler = () => {
                if (!settled) {
                    settled = true;
                    reject(new DOMException("The operation was aborted.", "AbortError"));
                }
            };
            if (signal) {
                signal.addEventListener("abort", abortHandler, { once: true });
            }
            runner(text, values, mergedOptions).then(
                (result) => {
                    if (signal) {
                        signal.removeEventListener("abort", abortHandler);
                    }
                    if (!settled) {
                        settled = true;
                        resolve(result ?? { rows: [], command: "", rowCount: 0, oid: 0, fields: [] });
                    }
                },
                (error) => {
                    if (signal) {
                        signal.removeEventListener("abort", abortHandler);
                    }
                    if (!settled) {
                        settled = true;
                        reject(error);
                    }
                },
            );
        });
        return result;
    }

    async first(queryOptions: Record<string, unknown> = {}): Promise<QueryResultRow | null> {
        this.limit(1);
        const result = await this.execute(queryOptions);
        return result.rows?.[0] ?? null;
    }

    then<TResult1, TResult2>(
        onFulfilled?: (value: QueryResult) => TResult1 | PromiseLike<TResult1>,
        onRejected?: (reason: unknown) => TResult2 | PromiseLike<TResult2>,
    ): Promise<TResult1 | TResult2> {
        return this.execute().then(onFulfilled, onRejected);
    }

    catch<TResult>(onRejected?: (reason: unknown) => TResult | PromiseLike<TResult>): Promise<TResult | QueryResult> {
        return this.execute().catch(onRejected);
    }
}

// Helper class for building CASE expressions
export class CaseBuilder {
    private _value: string | QueryFragment | null = null;
    private _when: Array<{ condition: string | QueryFragment; result: unknown }> = [];
    private _else: unknown | null = null;

    value(value: string | QueryFragment): this {
        this._value = value;
        return this;
    }

    when(condition: string | QueryFragment, result: unknown): this {
        this._when.push({ condition, result });
        return this;
    }

    else(result: unknown): this {
        this._else = result;
        return this;
    }

    build(): QueryFragment {
        const params: unknown[] = [];
        const parts: string[] = [];

        if (this._value) {
            if (isFragment(this._value)) {
                parts.push(`CASE ${this._value.text}`);
                params.push(...(this._value.values ?? []));
            } else {
                parts.push(`CASE ${this._value}`);
            }
        } else {
            parts.push("CASE");
        }

        for (const { condition, result } of this._when) {
            if (isFragment(condition)) {
                parts.push(`WHEN ${condition.text}`);
                params.push(...(condition.values ?? []));
            } else {
                parts.push(`WHEN ${condition}`);
            }
            
            if (isFragment(result)) {
                parts.push(`THEN ${result.text}`);
                params.push(...(result.values ?? []));
            } else if (result instanceof QueryBuilder) {
                const { text, values } = result.toSQL();
                parts.push(`THEN (${text})`);
                params.push(...values);
            } else {
                parts.push(`THEN $${params.length + 1}`);
                params.push(result);
            }
        }

        if (this._else !== null) {
            if (isFragment(this._else)) {
                parts.push(`ELSE ${this._else.text}`);
                params.push(...(this._else.values ?? []));
            } else if (this._else instanceof QueryBuilder) {
                const { text, values } = (this._else as QueryBuilder).toSQL();
                parts.push(`ELSE (${text})`);
                params.push(...values);
            } else {
                parts.push(`ELSE $${params.length + 1}`);
                params.push(this._else);
            }
        }

        parts.push("END");

        return raw(parts.join(" "));
    }
}

export function createQueryBuilder({ query, logger, shardKey, shardId }: {
    query?: QueryRunner;
    logger?: unknown;
    shardKey?: string;
    shardId?: string;
} = {}): (table: string) => QueryBuilderType {
    return function from(table: string) {
        return new QueryBuilder({ query, table, logger, shardKey, shardId });
    };
}

export { sql, raw, identifier, quoteIdent, quoteIdentList, isFragment };
