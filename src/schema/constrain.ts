import type { Pool } from "pg";
import type { SchemaConstrain } from "../types.js";

function normalizeColumns(columns: string | string[]): string {
    return Array.isArray(columns) ? columns.join(", ") : columns;
}

function validateAction(type: string, action: string | undefined): void {
    const actions = [
        "CASCADE",
        "RESTRICT",
        "SET NULL",
        "SET DEFAULT",
        "NO ACTION",
    ];

    if (action && !actions.includes(action)) {
        throw new Error(`Invalid ${type} action: ${action}`);
    }
}

export function constrainSchema(pool: Pool): SchemaConstrain {
    async function unique(tableName: string, constraint: string, columns: string | string[]): Promise<void> {
        const list = normalizeColumns(columns);

        const sql = `
            ALTER TABLE ${tableName}
            ADD CONSTRAINT ${constraint}
            UNIQUE (${list});
        `;

        await pool.query(sql);
    }

    async function foreignKey(
        tableName: string,
        constraint: string,
        columns: string | string[],
        referenceTable: string,
        referenceColumns: string | string[],
        options: { onDelete?: string; onUpdate?: string } = {},
    ): Promise<void> {
        const { onDelete, onUpdate } = options;

        validateAction("ON DELETE", onDelete ?? "");
        validateAction("ON UPDATE", onUpdate ?? "");

        const localColumns = normalizeColumns(columns);
        const referencedColumns = normalizeColumns(referenceColumns);

        const sql = `
            ALTER TABLE ${tableName}
            ADD CONSTRAINT ${constraint}
            FOREIGN KEY (${localColumns})
            REFERENCES ${referenceTable} (${referencedColumns})
            ${onDelete ? `ON DELETE ${onDelete}` : ""}
            ${onUpdate ? `ON UPDATE ${onUpdate}` : ""};
        `;

        await pool.query(sql);
    }

    async function check(tableName: string, constraint: string, expression: string): Promise<void> {
        const sql = `
            ALTER TABLE ${tableName}
            ADD CONSTRAINT ${constraint}
            CHECK (${expression});
        `;

        await pool.query(sql);
    }

    async function dropConstraint(tableName: string, constraint: string): Promise<void> {
        const sql = `
            ALTER TABLE ${tableName}
            DROP CONSTRAINT IF EXISTS ${constraint};
        `;

        await pool.query(sql);
    }

    return {
        unique,
        foreignKey,
        check,
        dropConstraint,
    };
}