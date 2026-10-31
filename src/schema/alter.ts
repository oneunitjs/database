import type { Pool } from "pg";
import type { SchemaAlter } from "../types.js";

export function alterSchema(pool: Pool): SchemaAlter {
    async function addColumns(table: string, columns: Record<string, string>): Promise<void> {
        const definition = Object.entries(columns)
            .map(([column, type]) => `ADD COLUMN ${column} ${type}`)
            .join(",\n");

        const sql = `
            ALTER TABLE ${table}
            ${definition};
        `;

        await pool.query(sql);
    }

    async function dropColumns(table: string, columns: string | string[]): Promise<void> {
        const colList = Array.isArray(columns) ? columns : [columns];
        const definition = colList
            .map((column) => `DROP COLUMN ${column}`)
            .join(",\n");

        const sql = `
            ALTER TABLE ${table}
            ${definition};
        `;

        await pool.query(sql);
    }

    async function renameColumn(table: string, oldName: string, newName: string): Promise<void> {
        const sql = `
            ALTER TABLE ${table}
            RENAME COLUMN ${oldName}
            TO ${newName};
        `;

        await pool.query(sql);
    }

    async function alterColumn(table: string, column: string, type: string): Promise<void> {
        const sql = `
            ALTER TABLE ${table}
            ALTER COLUMN ${column}
            TYPE ${type};
        `;

        await pool.query(sql);
    }

    async function renameTable(oldName: string, newName: string): Promise<void> {
        const sql = `
            ALTER TABLE ${oldName}
            RENAME TO ${newName};
        `;

        await pool.query(sql);
    }

    async function unique(table: string, constraint: string, columns: string | string[]): Promise<void> {
        const list = Array.isArray(columns) ? columns.join(", ") : columns;

        const sql = `
            ALTER TABLE ${table}
            ADD CONSTRAINT ${constraint}
            UNIQUE (${list});
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