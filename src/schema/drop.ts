import type { Pool } from "pg";
import type { SchemaDrop } from "../types.js";

export function dropSchema(pool: Pool): SchemaDrop {
    async function table(name: string, options: { cascade?: boolean } = {}): Promise<void> {
        const { cascade = false } = options;

        const sql = `
            DROP TABLE IF EXISTS ${name}
            ${cascade ? "CASCADE" : ""};
        `;

        await pool.query(sql);
    }

    async function index(name: string, options: { cascade?: boolean } = {}): Promise<void> {
        const { cascade = false } = options;

        const sql = `
            DROP INDEX IF EXISTS ${name}
            ${cascade ? "CASCADE" : ""};
        `;

        await pool.query(sql);
    }

    return {
        table,
        index,
    };
}