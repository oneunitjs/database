import type { Pool } from "pg";
import type { SchemaAlter } from "../types.js";
import { quoteIdent } from "../query/sql.js";

export function alterSchema(pool: Pool): SchemaAlter {
    async function addColumns(table: string, columns: Record<string, string>): Promise<void> {
        const definition = Object.entries(columns)
            .map(([column, type]) => `ADD COLUMN ${quoteIdent(column)} ${type}`)
            .join(",\n");

        const sql = `
            ALTER TABLE ${quoteIdent(table)}
            ${definition};
        `;

        await pool.query(sql);
    }

    async function dropColumns(table: string, columns: string | string[]): Promise<void> {
        const colList = Array.isArray(columns) ? columns : [columns];
        const definition = colList
            .map((column) => `DROP COLUMN ${quoteIdent(column)}`)
            .join(",\n");

        const sql = `
            ALTER TABLE ${quoteIdent(table)}
            ${definition};
        `;

        await pool.query(sql);
    }

    async function renameColumn(table: string, oldName: string, newName: string): Promise<void> {
        const sql = `
            ALTER TABLE ${quoteIdent(table)}
            RENAME COLUMN ${quoteIdent(oldName)}
            TO ${quoteIdent(newName)};
        `;

        await pool.query(sql);
    }

    async function alterColumn(table: string, column: string, type: string): Promise<void> {
        const sql = `
            ALTER TABLE ${quoteIdent(table)}
            ALTER COLUMN ${quoteIdent(column)}
            TYPE ${type};
        `;

        await pool.query(sql);
    }

    async function renameTable(oldName: string, newName: string): Promise<void> {
        const sql = `
            ALTER TABLE ${quoteIdent(oldName)}
            RENAME TO ${quoteIdent(newName)};
        `;

        await pool.query(sql);
    }

    async function unique(table: string, constraint: string, columns: string | string[]): Promise<void> {
        const colList = Array.isArray(columns) ? columns.map(c => quoteIdent(c)).join(", ") : quoteIdent(columns);

        const sql = `
            ALTER TABLE ${quoteIdent(table)}
            ADD CONSTRAINT ${quoteIdent(constraint)} UNIQUE (${colList});
        `;

        await pool.query(sql);
    }

    return {
        addColumns,
        dropColumns,
        renameColumn,
        alterColumn,
        renameTable,
        unique,
    };
}
