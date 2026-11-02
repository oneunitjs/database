import type { Pool } from "pg";
import type {
    ExtendedTableSchema,
    ExtendedColumnInfo,
    IndexInfo,
    ConstraintInfo,
    ForeignKeyInfo,
    SequenceInfo,
    EnumInfo,
    RollbackRisk,
    SchemaChange,
    SchemaDiffResult,
} from "../types.js";
import { quoteIdent } from "../query/sql.js";

function assessColumnChangeRisk(
    changeType: "create" | "drop" | "alter",
    currentType: string | undefined,
    newType: string | undefined,
    currentNullable: boolean | undefined,
    newNullable: boolean | undefined,
): RollbackRisk {
    if (changeType === "drop") {
        return "potentially-destructive";
    }
    if (changeType === "alter") {
        if (currentType && newType && currentType !== newType) {
            return "potentially-destructive";
        }
        if (currentNullable !== undefined && newNullable !== undefined && currentNullable && !newNullable) {
            return "potentially-destructive";
        }
    }
    return "safe";
}

function assessTableChangeRisk(changeType: "create" | "drop"): RollbackRisk {
    return changeType === "drop" ? "potentially-destructive" : "safe";
}

function assessConstraintChangeRisk(changeType: "create" | "drop"): RollbackRisk {
    return changeType === "drop" ? "potentially-destructive" : "safe";
}

/**
 * Introspect the current database schema across configurable schemas,
 * including tables, columns, indexes, constraints, foreign keys,
 * sequences, and enum types using PostgreSQL system catalogs.
 */
export async function inspectSchema(
    pool: Pool,
    options: { schemas?: string[] } = {},
): Promise<ExtendedTableSchema[]> {
    const schemas = options.schemas ?? ["public"];
    const client = await pool.connect();

    try {
        // Fetch tables across all requested schemas
        const { rows: tables } = await client.query(
            `SELECT table_schema, table_name
             FROM information_schema.tables
             WHERE table_schema = ANY($1)
               AND table_type = 'BASE TABLE'
             ORDER BY table_schema, table_name;`,
            [schemas],
        );

        const result: ExtendedTableSchema[] = [];

        for (const { table_schema, table_name } of tables) {
            const schema = String(table_schema);
            const name = String(table_name);

            // Skip internal migration/audit tables
            if (name.startsWith("_writer_")) {
                continue;
            }

            // Columns
            const { rows: columns } = await client.query(
                `SELECT column_name, data_type, is_nullable, column_default,
                        is_identity, generation_expression,
                        character_maximum_length, numeric_precision, numeric_scale
                 FROM information_schema.columns
                 WHERE table_schema = $1 AND table_name = $2
                 ORDER BY ordinal_position;`,
                [schema, name],
            );

            const columnMap: Record<string, ExtendedColumnInfo> = {};
            for (const c of columns as Array<Record<string, unknown>>) {
                const colName = String(c.column_name);
                columnMap[colName] = {
                    name: colName,
                    type: String(c.data_type),
                    nullable: c.is_nullable === "YES",
                    default: (c.column_default as string | null) ?? null,
                    isIdentity: c.is_identity === "YES",
                    isGenerated: c.generation_expression !== null && c.generation_expression !== undefined,
                    charMaxLength: c.character_maximum_length != null ? Number(c.character_maximum_length) : null,
                    numericPrecision: c.numeric_precision != null ? Number(c.numeric_precision) : null,
                    numericScale: c.numeric_scale != null ? Number(c.numeric_scale) : null,
                };
            }

            // Indexes
            const { rows: indexes } = await client.query(
                `SELECT indexname, indexdef
                 FROM pg_indexes
                 WHERE schemaname = $1 AND tablename = $2;`,
                [schema, name],
            );
            const indexList: IndexInfo[] = (indexes as Array<Record<string, unknown>>).map((i) => {
                const def = String(i.indexdef);
                const unique = def.toUpperCase().includes("UNIQUE");
                const colMatch = def.match(/\((.+)\)/);
                const columns = colMatch ? colMatch[1].split(",").map((c) => c.trim().replace(/"/g, "")) : [];
                return {
                    name: String(i.indexname),
                    table: name,
                    columns,
                    unique,
                    schema,
                };
            });

            // Constraints
            const { rows: constraints } = await client.query(
                `SELECT conname, contype, pg_get_constraintdef(pg_constraint.oid) as definition
                 FROM pg_constraint
                 JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
                 JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                 WHERE pg_namespace.nspname = $1 AND pg_class.relname = $2;`,
                [schema, name],
            );
            const constraintList: ConstraintInfo[] = (constraints as Array<Record<string, unknown>>).map((c) => {
                const contype = String(c.contype);
                let type: ConstraintInfo["type"];
                switch (contype) {
                    case "p": type = "PRIMARY KEY"; break;
                    case "u": type = "UNIQUE"; break;
                    case "f": type = "FOREIGN KEY"; break;
                    case "c": type = "CHECK"; break;
                    default: type = "CHECK";
                }
                const def = String(c.definition);
                const colMatch = def.match(/\((.+)\)/);
                const columns = colMatch ? colMatch[1].split(",").map((c) => c.trim().replace(/"/g, "")) : [];
                return {
                    name: String(c.conname),
                    table: name,
                    type,
                    columns,
                    schema,
                    definition: def,
                };
            });

            // Foreign keys
            const { rows: foreignKeys } = await client.query(
                `SELECT conname, conkey, confkey, confrelid, confupdtype, confdeltype
                 FROM pg_constraint
                 JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
                 JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                 WHERE pg_constraint.contype = 'f'
                   AND pg_namespace.nspname = $1
                   AND pg_class.relname = $2;`,
                [schema, name],
            );
            const fkList: ForeignKeyInfo[] = await Promise.all(
                (foreignKeys as Array<Record<string, unknown>>).map(async (fk) => {
                    const conkey = (fk.conkey as number[]).map((n) => n);
                    const columns: string[] = [];
                    for (const colIdx of conkey) {
                        const { rows: colRows } = await client.query(
                            `SELECT attname FROM pg_attribute WHERE attrelid = $1 AND attnum = $2;`,
                            [String(fk.conrelid ?? 0), colIdx],
                        );
                        if (colRows[0]) columns.push(String(colRows[0].attname));
                    }
                    return {
                        name: String(fk.conname),
                        table: name,
                        columns,
                        foreignTable: "",
                        foreignColumns: [],
                        onDelete: "NO ACTION",
                        onUpdate: "NO ACTION",
                        schema,
                    };
                }),
            );

            // Sequences owned by this table's identity columns
            const { rows: sequences } = await client.query(
                `SELECT seq.relname as sequence_name, ns.nspname as schema,
                        pg_sequence.seqstart as start_value, pg_sequence.seqincrement as increment_by
                 FROM pg_class seq
                 JOIN pg_namespace ns ON seq.relnamespace = ns.oid
                 JOIN pg_sequence ON pg_sequence.seqrelid = seq.oid
                 WHERE ns.nspname = $1 AND seq.relkind = 'S';`,
                [schema],
            );
            const sequenceList: SequenceInfo[] = (sequences as Array<Record<string, unknown>>).map((s) => ({
                name: String(s.sequence_name),
                schema: String(s.schema),
                dataType: "bigint",
                startValue: Number(s.start_value ?? 1),
                incrementBy: Number(s.increment_by ?? 1),
            }));

            // Enums used by columns in this table
            const { rows: enums } = await client.query(
                `SELECT t.typname as enum_name, n.nspname as schema,
                        array_agg(e.enumlabel ORDER BY e.enumsortorder) as values
                 FROM pg_type t
                 JOIN pg_enum e ON e.enumtypid = t.oid
                 JOIN pg_namespace n ON n.oid = t.typnamespace
                 JOIN pg_attribute a ON a.atttypid = t.oid
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace cn ON cn.oid = c.relnamespace
                 WHERE cn.nspname = $1 AND c.relname = $2
                 GROUP BY t.typname, n.nspname;`,
                [schema, name],
            );
            const enumList: EnumInfo[] = (enums as Array<Record<string, unknown>>).map((e) => ({
                name: String(e.enum_name),
                schema: String(e.schema),
                values: (e.values as unknown[]).map(String),
            }));

            result.push({
                name,
                schema,
                columns: columnMap,
                indexes: indexList,
                constraints: constraintList,
                foreignKeys: fkList,
                sequences: sequenceList,
                enums: enumList,
            });
        }

        return result;
    } finally {
        client.release();
    }
}

/**
 * Diff the current schema against the desired schema, producing a detailed
 * diff result with risk classification for every change.
 */
export function diffSchema(
    current: ExtendedTableSchema[],
    desired: Record<string, Record<string, string>>,
): SchemaDiffResult {
    const currentMap = new Map(current.map((t) => [`${t.schema}.${t.name}`, t]));
    const createTable: SchemaDiffResult["createTable"] = [];
    const dropTable: SchemaDiffResult["dropTable"] = [];
    const alterTable: SchemaDiffResult["alterTable"] = [];
    const changes: SchemaChange[] = [];

    for (const [key, columns] of Object.entries(desired)) {
        const [schema = "public", name] = key.includes(".") ? key.split(".") : ["public", key];
        const mapKey = `${schema}.${name}`;
        const currentTable = currentMap.get(mapKey);

        if (!currentTable) {
            createTable.push({ name, schema, columns });
            changes.push({
                object: name,
                objectType: "table",
                changeType: "create",
                risk: assessTableChangeRisk("create"),
                details: { schema, columns },
                requiresManualIntervention: false,
            });
            continue;
        }

        const addColumns: Record<string, string> = {};
        const dropColumns: string[] = [];

        for (const [column, type] of Object.entries(columns)) {
            if (!(column in currentTable.columns)) {
                addColumns[column] = type;
                changes.push({
                    object: column,
                    objectType: "column",
                    changeType: "create",
                    risk: assessColumnChangeRisk("create", undefined, type, undefined, undefined),
                    details: { table: name, schema, column, type },
                    requiresManualIntervention: false,
                });
            }
        }

        for (const column of Object.keys(currentTable.columns)) {
            if (!(column in columns)) {
                dropColumns.push(column);
                const currentCol = currentTable.columns[column];
                changes.push({
                    object: column,
                    objectType: "column",
                    changeType: "drop",
                    risk: assessColumnChangeRisk("drop", currentCol.type, undefined, currentCol.nullable, undefined),
                    details: { table: name, schema, column, type: currentCol.type },
                    sql: `ALTER TABLE ${quoteIdent(schema)}.${quoteIdent(name)} DROP COLUMN ${quoteIdent(column)};`,
                    requiresManualIntervention: true,
                });
            }
        }

        if (Object.keys(addColumns).length > 0 || dropColumns.length > 0) {
            alterTable.push({ name, schema, addColumns, dropColumns });
        }
    }

    for (const [key, currentTable] of currentMap.entries()) {
        const desiredKey = key in desired ? key : currentTable.name;
        if (!(desiredKey in desired)) {
            dropTable.push({ name: currentTable.name, schema: currentTable.schema });
            changes.push({
                object: currentTable.name,
                objectType: "table",
                changeType: "drop",
                risk: assessTableChangeRisk("drop"),
                details: { schema: currentTable.schema, name: currentTable.name },
                sql: `DROP TABLE IF EXISTS ${quoteIdent(currentTable.schema)}.${quoteIdent(currentTable.name)} CASCADE;`,
                rollbackSql: `CREATE TABLE ${quoteIdent(currentTable.schema)}.${quoteIdent(currentTable.name)} (...);`,
                requiresManualIntervention: true,
            });
        }
    }

    const hasDestructiveChanges = changes.some((c) => c.risk === "potentially-destructive" || c.risk === "irreversible");
    const hasSafeChanges = changes.some((c) => c.risk === "safe");

    return { createTable, dropTable, alterTable, changes, hasDestructiveChanges, hasSafeChanges };
}

export { assessColumnChangeRisk, assessTableChangeRisk, assessConstraintChangeRisk };
