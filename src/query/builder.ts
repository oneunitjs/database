import type { QueryBuilder as QueryBuilderType, QueryRunner, QueryLike, QueryFragment, QueryResult, QueryResultRow, SqlFragment } from "../types.js";
import { quoteIdent, quoteIdentList, raw, isFragment, sql } from "./sql.js";

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
]);

const DIRECTIONS = new Set([
    "ASC",
    "DESC",
    "ASC NULLS FIRST",
    "ASC NULLS LAST",
    "DESC NULLS FIRST",
    "DESC NULLS LAST",
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

interface WhereCondition {
    connector: "AND" | "OR";
    column?: string;
    operator?: string;
    value?: unknown;
    text?: string;
    values?: unknown[];
}

interface JoinClause {
    type: string;
    table: string;
    on: string | SqlFragment | string[];
}

interface HavingCondition {
    column: string;
    operator: string;
    value: unknown;
}

interface OrderClause {
    column: string;
    direction: string;
}

interface ShardRoutingHint {
    shardKey?: string;
    shardId?: string;
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
    private _selects: (string | QueryFragment)[] = [];
    private _joins: JoinClause[] = [];
    private _groups: string[] = [];
    private _havings: HavingCondition[] = [];
    private _orders: OrderClause[] = [];
    private _limit: number | null = null;
    private _offset: number | null = null;
    private _distinct = false;
    private _insertRows: Record<string, unknown>[] | null = null;
    private _updateValues: Record<string, unknown> | null = null;
    private _conflict: unknown = null;
    private _returning: string[] | null = null;
    private _shardKey: string | undefined;
    private _shardId: string | undefined;

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
        next._params = this._params;
        next._wheres = [...this._wheres];
        next._selects = [...this._selects];
        next._joins = [...this._joins];
        next._groups = [...this._groups];
        next._havings = [...this._havings];
        next._orders = [...this._orders];
        next._limit = this._limit;
        next._offset = this._offset;
        next._distinct = this._distinct;
        next._insertRows = this._insertRows;
        next._updateValues = this._updateValues;
        next._conflict = this._conflict;
        next._returning = this._returning;
        next._shardKey = this._shardKey;
        next._shardId = this._shardId;
        return next;
    }

    select(...columns: Array<string | string[] | SqlFragment>): this {
        this._mode = "select";
        let processedColumns: (string | SqlFragment)[];
        if (columns.length === 1 && Array.isArray(columns[0])) {
            processedColumns = columns[0] as (string | SqlFragment)[];
        } else {
            processedColumns = columns as (string | SqlFragment)[];
        }
        this._selects.push(...(processedColumns.length ? processedColumns : ["*"]));
        return this;
    }

    distinct(value = true): this {
        this._distinct = value;
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

    whereRaw(fragment: QueryFragment): this {
        this._wheres.push({ connector: "AND", text: fragment.text, values: fragment.values ?? [] });
        return this;
    }

    join(table: string, on: string | SqlFragment | string[]): this {
        this._joins.push({ type: "JOIN", table, on });
        return this;
    }

    leftJoin(table: string, on: string | SqlFragment | string[]): this {
        this._joins.push({ type: "LEFT JOIN", table, on });
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

    orderBy(column: string, direction = "ASC"): this {
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

    private _compileWhere(): string {
        if (this._wheres.length === 0) {
            return "";
        }

        const parts = this._wheres.map((condition, index) => {
            let text = "";

            if (condition.text !== undefined) {
                text = condition.text;
                const valueIndex = 0;
                text = text.replace(/\$(\d+)/g, (_, position) => {
                    const idx = Number(position) - 1;
                    if (idx < (condition.values ?? []).length) {
                        return this._params.add(condition.values![idx]);
                    }
                    return `\$${position}`;
                });
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

    private _compileReturning(): string {
        if (!this._returning) {
            return "";
        }
        const columns = Array.isArray(this._returning) ? this._returning : [this._returning];
        return ` RETURNING ${quoteIdentList(columns)}`;
    }

    private _compileSelect(): string {
        const distinct = this._distinct ? "DISTINCT " : "";
        const columns = this._selects.length ? this._selects : ["*"];
        const projection = columns
            .map((column) => {
                if (column && typeof column === "object" && "text" in column) {
                    return (column as QueryFragment).text;
                }
                if (column === "*") {
                    return "*";
                }
                return quoteIdent(column as string);
            })
            .join(", ");

        let text = `SELECT ${distinct}${projection} FROM ${quoteIdent(this._table)}`;

        for (const join of this._joins) {
            let on: SqlFragment;
            if (typeof join.on === "string") {
                on = raw(join.on);
            } else if (Array.isArray(join.on)) {
                on = sql`${join.on}`;
            } else {
                on = join.on;
            }
            text += ` ${join.type} ${quoteIdent(join.table)} ON ${on.text}`;
            for (const value of (on.values ?? [])) {
                text = text.replace(/\$\d+/, () => this._params.add(value));
            }
        }

        text += this._compileWhere();

        if (this._groups.length) {
            text += ` GROUP BY ${quoteIdentList(this._groups)}`;
        }

        for (const having of this._havings) {
            text += ` HAVING ${quoteIdent(having.column)} ${having.operator} ${this._params.add(having.value)}`;
        }

        if (this._orders.length) {
            text += ` ORDER BY ${this._orders
                .map((order) => `${quoteIdent(order.column)} ${order.direction}`)
                .join(", ")}`;
        }

        if (this._limit !== null) {
            text += ` LIMIT ${this._params.add(this._limit)}`;
        }

        if (this._offset !== null) {
            text += ` OFFSET ${this._params.add(this._offset)}`;
        }

        return text;
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

    toSQL(): { text: string; values: unknown[] } {
        const builder = this._buildCloneWithFreshParams();
        const text = builder._compile();
        return { text, values: builder._params.values };
    }

    private _buildCloneWithFreshParams(): QueryBuilder {
        const clone = this.clone();
        clone._params = new ParamCollector();
        return clone;
    }

    private _compile(): string {
        switch (this._mode) {
            case "insert":
                return this._compileInsert();
            case "update":
                return this._compileUpdate();
            case "delete":
                return this._compileDelete();
            default:
                return this._compileSelect();
        }
    }

    async execute(queryOptions: Record<string, unknown> = {}): Promise<QueryResult> {
        const { text, values } = this.toSQL();
        const runner = this._query;
        if (typeof runner !== "function") {
            throw new TypeError("QueryBuilder was not given a query function to execute with");
        }
        // Merge builder-level shard routing into execution options. Per-call
        // options take precedence over builder-level configuration.
        const mergedOptions: Record<string, unknown> = { ...queryOptions };
        if (this._shardKey && mergedOptions.shardKey === undefined) {
            mergedOptions.shardKey = this._shardKey;
        }
        if (this._shardId && mergedOptions.shardId === undefined) {
            mergedOptions.shardId = this._shardId;
        }
        const result = await runner(text, values, mergedOptions);
        return result ?? { rows: [], command: "", rowCount: 0, oid: 0, fields: [] };
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