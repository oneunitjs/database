import type { Pool } from "pg";
import type { SchemaDrop } from "../types.js";
import { quoteIdent } from "../query/sql.js";

export function dropSchema(pool: Pool): SchemaDrop {
    async function table(name: string, options: { cascade?: boolean } = {}): Promise<void> {
        const { cascade = false } = options;

        const sql = `
            DROP TABLE IF EXISTS ${quoteIdent(name)}
            ${cascade ? "CASCADE" : ""};
        `;

        await pool.query(sql);
    }

    async function index(name: string, options: { cascade?: boolean } = {}): Promise<void> {
        const { cascade = false } = options;

        const sql = `
            DROP INDEX IF EXISTS ${quoteIdent(name)}
            ${cascade ? "CASCADE" : ""};
        `;

        await pool.query(sql);
    }

    return {
        table,
        index,
    };
}
