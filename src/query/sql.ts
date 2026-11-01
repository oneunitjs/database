import type { QueryFragment, QueryLike, SqlFn, JoinFn } from "../types.js";

export const FRAGMENT = Symbol("sql.fragment");

function isFragment(value: unknown): value is QueryFragment {
    return value !== null && typeof value === "object" && (value as Record<symbol, boolean>)[FRAGMENT] === true;
}

function makeFragment(text: string, values: unknown[]): QueryFragment {
    return { [FRAGMENT]: true, text, values };
}

/**
 * Quote a PostgreSQL identifier. Dots are treated as schema/table separators
 * and `*` is passed through so `users.*` remains usable in SELECT lists.
 */
export function quoteIdent(name: string): string {
    if (typeof name !== "string" || name.length === 0) {
        throw new TypeError(`Invalid identifier: ${String(name)}`);
    }

    return name
        .split(".")
        .map((part) => {
            if (part === "*") {
                return "*";
            }
            return `"${part.replace(/"/g, '""')}"`;
        })
        .join(".");
}

export function quoteIdentList(names: string | string[]): string {
    return (Array.isArray(names) ? names : [names]).map(quoteIdent).join(", ");
}

/**
 * Mark a string as already-safe SQL. Never use this with untrusted input.
 */
export function raw(text: string): QueryFragment {
    return makeFragment(String(text), []);
}

/**
 * Create a safe identifier fragment. This properly quotes the identifier
 * to prevent SQL injection when using trusted input.
 *
 * @example
 * sql`SELECT * FROM ${identifier('users')}`
 * // => SELECT * FROM "users"
 */
export function identifier(name: string): QueryFragment {
    return raw(quoteIdent(name));
}

/**
 * Tagged template that produces a parameterized `{ text, values }` object.
 *
 *   sql`SELECT * FROM users WHERE id = ${id}`
 *   sql`WHERE status IN (${sql.join(statuses)})`
 *
 * Fragments produced by this helper nest; arrays expand into comma-separated
 * placeholders. Empty arrays produce `FALSE` for IN clauses.
 */
export function sql(strings: TemplateStringsArray, ...values: QueryLike[]): QueryFragment {
    let text = "";
    const params: unknown[] = [];

    strings.forEach((chunk, index) => {
        text += chunk;

        if (index >= values.length) {
            return;
        }

        const value = values[index];

        if (isFragment(value)) {
            const nested = shiftFragment(value, params.length);
            text += nested.text;
            params.push(...nested.values);
        } else if (Array.isArray(value)) {
            if (value.length === 0) {
                text += "FALSE";
            } else {
                const placeholders = value.map((item) => {
                    if (Array.isArray(item)) {
                        throw new TypeError("Nested arrays are not supported in sql template; use sql.join() for nested values");
                    }
                    if (item !== null && typeof item === "object" && !Buffer.isBuffer(item)) {
                        throw new TypeError("Objects and arrays are not supported as direct sql template values; use sql.join() or raw()");
                    }
                    params.push(item);
                    return `$${params.length}`;
                });
                text += placeholders.join(", ");
            }
        } else {
            params.push(value);
            text += `$${params.length}`;
        }
    });

    return makeFragment(text, params);
}

function shiftFragment(fragment: QueryFragment, offset: number): { text: string; values: unknown[] } {
    if (fragment.values.length === 0) {
        return fragment;
    }

    const text = fragment.text.replace(/\$(\d+)/g, (_, position) => {
        return `$${Number(position) + offset}`;
    });

    return { text, values: fragment.values };
}

sql.raw = raw;
sql.identifier = identifier;
sql.quoteIdent = quoteIdent;
sql.quoteIdentList = quoteIdentList;
sql.isFragment = isFragment;

sql.join = function join(values: QueryLike | QueryLike[], separator = ", "): QueryFragment {
    const items = Array.isArray(values) ? values : [values];
    const params: unknown[] = [];
    let text = "";

    if (items.length === 0) {
        // Empty arrays produce FALSE for IN clauses
        return makeFragment("FALSE", []);
    }

    items.forEach((item, index) => {
        if (index > 0) {
            text += separator;
        }

        if (isFragment(item)) {
            const shifted = shiftFragment(item, params.length);
            text += shifted.text;
            params.push(...shifted.values);
        } else if (Array.isArray(item)) {
            if (item.length === 0) {
                text += "FALSE";
            } else {
                const placeholders = item.map((subItem) => {
                    if (Array.isArray(subItem)) {
                        throw new TypeError("Nested arrays are not supported in sql.join; flatten the array first");
                    }
                    if (subItem !== null && typeof subItem === "object" && !Buffer.isBuffer(subItem)) {
                        throw new TypeError("Objects are not supported as direct sql.join values; use raw() for complex expressions");
                    }
                    params.push(subItem);
                    return `$${params.length}`;
                });
                text += placeholders.join(", ");
            }
        } else {
            params.push(item);
            text += `$${params.length}`;
        }
    });

    return makeFragment(text, params);
} as JoinFn;

// ==================== Window Function Helpers ====================

/**
 * Create a ROW_NUMBER() window function.
 *
 * @example
 * sql`ROW_NUMBER() OVER (PARTITION BY ${'dept'} ORDER BY ${'salary'} DESC)`
 */
export function rowNumber(partitionBy?: string[], orderBy?: string[]): QueryFragment {
    const parts: string[] = ["ROW_NUMBER() OVER ("];
    
    if (partitionBy && partitionBy.length > 0) {
        parts.push(`PARTITION BY ${partitionBy.map(p => quoteIdent(p)).join(", ")}`);
    }
    
    if (orderBy && orderBy.length > 0) {
        parts.push(`ORDER BY ${orderBy.map(o => quoteIdent(o)).join(", ")}`);
    }
    
    parts.push(")");
    return raw(parts.join(" "));
}

/**
 * Create a RANK() window function.
 */
export function rank(partitionBy?: string[], orderBy?: string[]): QueryFragment {
    const parts: string[] = ["RANK() OVER ("];
    
    if (partitionBy && partitionBy.length > 0) {
        parts.push(`PARTITION BY ${partitionBy.map(p => quoteIdent(p)).join(", ")}`);
    }
    
    if (orderBy && orderBy.length > 0) {
        parts.push(`ORDER BY ${orderBy.map(o => quoteIdent(o)).join(", ")}`);
    }
    
    parts.push(")");
    return raw(parts.join(" "));
}

/**
 * Create a DENSE_RANK() window function.
 */
export function denseRank(partitionBy?: string[], orderBy?: string[]): QueryFragment {
    const parts: string[] = ["DENSE_RANK() OVER ("];
    
    if (partitionBy && partitionBy.length > 0) {
        parts.push(`PARTITION BY ${partitionBy.map(p => quoteIdent(p)).join(", ")}`);
    }
    
    if (orderBy && orderBy.length > 0) {
        parts.push(`ORDER BY ${orderBy.map(o => quoteIdent(o)).join(", ")}`);
    }
    
    parts.push(")");
    return raw(parts.join(" "));
}

/**
 * Create a ROW_NUMBER(), RANK(), or DENSE_RANK() with a named window.
 *
 * @example
 * const w = sql.window('ranking', { partitionBy: ['dept'], orderBy: ['salary DESC'] });
 * sql`RANK() OVER ${w}`
 */
export function window(name: string, options: { partitionBy?: string[]; orderBy?: string[] }): QueryFragment {
    const parts: string[] = [quoteIdent(name)];
    return raw(parts.join(" "));
}

// ==================== JSON Operator Helpers ====================

/**
 * Get JSON field as text.
 *
 * @example
 * sql`${jsonGetText('metadata', 'color')} = 'blue'`
 */
export function jsonGetText(column: string, path: string): QueryFragment {
    return raw(`${quoteIdent(column)}->>${quoteString(path)}`);
}

/**
 * Get JSON field as JSON.
 *
 * @example
 * sql`${jsonGet('metadata', 'color')} = '"blue"'`
 */
export function jsonGet(column: string, path: string): QueryFragment {
    return raw(`${quoteIdent(column)}->${quoteString(path)}`);
}

/**
 * Check if JSON contains value (superset of).
 *
 * @example
 * sql`${jsonContains('metadata', { color: 'blue' })}`
 */
export function jsonContains(column: string, value: unknown): QueryFragment {
    return raw(`${quoteIdent(column)} @> $1`);
}

/**
 * Check if JSON is contained in value (subset of).
 *
 * @example
 * sql`${jsonContained('metadata', { color: 'blue', size: 'large' })}`
 */
export function jsonContained(column: string, value: unknown): QueryFragment {
    return raw(`${quoteIdent(column)} <@ $1`);
}

/**
 * Get JSON array length.
 *
 * @example
 * sql`${jsonArrayLength('items')} > 0`
 */
export function jsonArrayLength(column: string): QueryFragment {
    return raw(`jsonb_array_length(${quoteIdent(column)})`);
}

/**
 * Get JSON object keys.
 *
 * @example
 * sql`${jsonObjectKeys('metadata')}`
 */
export function jsonObjectKeys(column: string): QueryFragment {
    return raw(`jsonb_object_keys(${quoteIdent(column)})`);
}

/**
 * Get JSON field by path (PostgreSQL jsonb_path_query).
 *
 * @example
 * sql`${jsonPath('data', '$.users[*].name')} = 'John'`
 */
export function jsonPath(column: string, path: string): QueryFragment {
    return raw(`jsonb_path_query(${quoteIdent(column)}, ${quoteString(path)})`);
}

/**
 * Quote a string value for use in SQL (for dynamic SQL generation, not for parameters).
 * WARNING: Only use this with trusted input! For user input, use parameterized queries.
 */
export function quoteString(value: string): string {
    if (typeof value !== "string") {
        return String(value);
    }
    return `'${value.replace(/'/g, "''")}'`;
}

// ==================== CASE Expression Helper ====================

/**
 * Create a CASE expression with WHEN/THEN/ELSE.
 *
 * @example
 * sql`CASE WHEN ${status} = 'active' THEN 'Active' ELSE 'Inactive' END`
 */
export function caseWhen(
    conditions: Array<{ when: string | QueryFragment; then: unknown }>,
    elseValue?: unknown
): QueryFragment {
    const params: unknown[] = [];
    const parts: string[] = ["CASE"];

    for (const { when, then } of conditions) {
        if (isFragment(when)) {
            parts.push(`WHEN ${when.text}`);
            params.push(...when.values);
        } else {
            parts.push(`WHEN ${when}`);
        }
        
        if (isFragment(then)) {
            parts.push(`THEN ${then.text}`);
            params.push(...then.values);
        } else {
            parts.push(`THEN $${params.length + 1}`);
            params.push(then);
        }
    }

    if (elseValue !== undefined) {
        if (isFragment(elseValue)) {
            parts.push(`ELSE ${elseValue.text}`);
            params.push(...elseValue.values);
        } else {
            parts.push(`ELSE $${params.length + 1}`);
            params.push(elseValue);
        }
    }

    parts.push("END");
    return makeFragment(parts.join(" "), params);
}

// ==================== Aggregation Helpers ====================

/**
 * Create a COUNT DISTINCT expression.
 */
export function countDistinct(column: string): QueryFragment {
    return raw(`COUNT(DISTINCT ${quoteIdent(column)})`);
}

/**
 * Create a string aggregation with delimiter.
 */
export function stringAgg(column: string, delimiter: string): QueryFragment {
    return raw(`STRING_AGG(${quoteIdent(column)}, ${quoteString(delimiter)})`);
}

/**
 * Create an array aggregation.
 */
export function arrayAgg(column: string): QueryFragment {
    return raw(`ARRAY_AGG(${quoteIdent(column)})`);
}

/**
 * Create a JSON aggregation.
 */
export function jsonAgg(column: string): QueryFragment {
    return raw(`JSON_AGG(${quoteIdent(column)})`);
}

/**
 * Create a JSON object aggregation.
 */
export function jsonObjectAgg(key: string, value: string): QueryFragment {
    return raw(`JSON_OBJECT_AGG(${quoteIdent(key)}, ${quoteIdent(value)})`);
}

export type { QueryFragment } from "../types.js";
export type { SqlFn } from "../types.js";
export type { JoinFn } from "../types.js";
export { isFragment };
