import type { Pool, QueryResult } from "pg";
import type { SchemaCreate } from "../types.js";
import { quoteIdent } from "../query/sql.js";

export function createSchema(pool: Pool): SchemaCreate {
    async function table(name: string, columns: Record<string, string>): Promise<void> {
        const definition = Object.entries(columns)
            .map(([column, type]) => `${quoteIdent(column)} ${type}`)
            .join(",\n");

        const sql = `
            CREATE TABLE IF NOT EXISTS ${quoteIdent(name)} (
                ${definition}
            );
        `;

        await pool.query(sql);
    }

    async function index(name: string, table: string, columns: string | string[]): Promise<void> {
        const colList = Array.isArray(columns) ? columns.map(c => quoteIdent(c)).join(", ") : quoteIdent(columns);

        const sql = `
            CREATE INDEX IF NOT EXISTS ${quoteIdent(name)} 
            ON ${quoteIdent(table)} (${colList});
        `;

        await pool.query(sql);
    }

    return {
        table,
        index,
    };
}
