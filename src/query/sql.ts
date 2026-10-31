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

export type { QueryFragment } from "../types.js";
export type { SqlFn } from "../types.js";
export type { JoinFn } from "../types.js";
export { isFragment };