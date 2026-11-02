// ---------------------------------------------------------------------------
// Schema Hash Calculator
// ---------------------------------------------------------------------------

import type { Pool, PoolClient } from "pg";
import { createHash as createHashFn } from "node:crypto";

export interface ColumnSchemaInfo {
    name: string;
    dataType: string;
    isNullable: boolean;
    defaultValue?: string | null;
    characterMaximumLength?: number | null;
    numericPrecision?: number | null;
    numericScale?: number | null;
}

export interface IndexSchemaInfo {
    name: string;
    columns: string[];
    unique: boolean;
}

export interface ConstraintSchemaInfo {
    name: string;
    type: "primary_key" | "foreign_key" | "unique" | "check";
    columns: string[];
    definition?: string;
}

export interface TableSchemaInfo {
    name: string;
    columns: ColumnSchemaInfo[];
    indexes: IndexSchemaInfo[];
    constraints: ConstraintSchemaInfo[];
}

export interface ShardSchemaInfo {
    shardId: string;
    schemaVersion: string;
    version?: string;
    checksum: string;
    tables: Record<string, TableSchemaInfo>;
    appliedAt: Date;
    isConsistent?: boolean;
}

export interface SchemaHashOptions {
    tableSchema?: string;
    schemaName?: string;
    includeFunctions?: boolean;
    includeTriggers?: boolean;
    includeSequences?: boolean;
    tables?: string[];
}

export class SchemaHashCalculator {
    private readonly pool: Pool;

    constructor(pool: Pool) {
        this.pool = pool;
    }

    async calculate(client?: Pool | PoolClient | SchemaHashOptions, options?: SchemaHashOptions): Promise<string> {
        let pool: Pool | PoolClient = this.pool;
        let opts: SchemaHashOptions = {};
        if (client && typeof (client as any).query === "function") {
            pool = client as Pool | PoolClient;
        } else if (client && typeof client === "object") {
            opts = client as SchemaHashOptions;
            if (options) opts = { ...opts, ...options };
        }
        if (options && (client as any)?.query) opts = options;

        const schemaName = opts.schemaName ?? opts.tableSchema ?? "public";
        const hash = (await import("node:crypto")).createHash("sha256");

        const conditions: string[] = [];
        const params: any[] = [schemaName];
        let tableSql = `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`;
        if (opts.tables?.length) {
            tableSql += ` AND table_name = ANY($2)`;
            params.push(opts.tables);
        }
        tableSql += ` ORDER BY table_name`;
        const tableResult = await (pool as any).query(tableSql, params);

        for (const row of tableResult.rows ?? []) {
            const tableName = row.table_name as string;
            const schema = await this.getTableSchema("", tableName, schemaName);
            hash.update(tableName);
            hash.update(this.hashTableSchemaInfo(schema));
        }

        if (opts.includeFunctions) {
            const fnResult = await (pool as any).query(
                `SELECT p.proname, pg_get_functiondef(p.oid) AS definition
                 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = $1 ORDER BY p.proname`,
                [schemaName],
            ).catch(() => ({ rows: [] }));
            for (const row of fnResult.rows ?? []) {
                hash.update(row.proname ?? "");
                hash.update(row.definition ?? "");
            }
        }
        if (opts.includeTriggers) {
            const trigResult = await (pool as any).query(
                `SELECT trigger_name, event_object_table, action_statement
                 FROM information_schema.triggers WHERE trigger_schema = $1 ORDER BY trigger_name`,
                [schemaName],
            ).catch(() => ({ rows: [] }));
            for (const row of trigResult.rows ?? []) {
                hash.update(row.trigger_name ?? "");
                hash.update(row.event_object_table ?? "");
                hash.update(row.action_statement ?? "");
            }
        }
        if (opts.includeSequences) {
            const seqResult = await (pool as any).query(
                `SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $1 ORDER BY sequence_name`,
                [schemaName],
            ).catch(() => ({ rows: [] }));
            for (const row of seqResult.rows ?? []) {
                hash.update(row.sequence_name ?? "");
            }
        }

        return hash.digest("hex");
    }

    calculateSchemaHash = this.calculate.bind(this);
    computeHash = this.calculate.bind(this);
    getSchemaHash = this.calculate.bind(this);
    calculateSchemaChecksum = this.calculate.bind(this);
    hashSchema = this.calculate.bind(this);

    calculateMigrationHash(sql: string): string {
        return this.hashMigrationSql(sql);
    }

    hashMigrationSql(sql: string): string {
        const hash = createHashFn("sha256");
        hash.update(sql.trim());
        return hash.digest("hex");
    }

    schemasEqual(a: any, b: any): boolean {
        return this.hashTableSchemaInfo(normalizeTable(a)) === this.hashTableSchemaInfo(normalizeTable(b));
    }

    async hashTableSchema(shardId: string, tableName: string, schemaName: string = "public"): Promise<string> {
        const schema = await this.getTableSchema(shardId, tableName, schemaName);
        return this.hashTableSchemaInfo(schema);
    }

    hashTableSchemaInfo(schema: TableSchemaInfo): string {
        const hash = createHashFn("sha256");

        const sortedColumns = [...(schema.columns ?? [])].sort((a, b) => a.name.localeCompare(b.name));
        for (const column of sortedColumns) {
            hash.update(String(column.name ?? ""));
            hash.update(String(column.dataType ?? ""));
            hash.update(String(column.isNullable));
            hash.update(column.defaultValue ?? "null");
            hash.update(String(column.characterMaximumLength ?? "null"));
            hash.update(String(column.numericPrecision ?? "null"));
            hash.update(String(column.numericScale ?? "null"));
        }

        const sortedIndexes = [...(schema.indexes ?? [])].sort((a, b) => a.name.localeCompare(b.name));
        for (const index of sortedIndexes) {
            hash.update(String(index.name ?? ""));
            const indexColumns = Array.isArray(index.columns)
                ? index.columns
                : (typeof index.columns === "string"
                    ? String(index.columns).replace(/^\{|\}$/g, "").split(",").filter(Boolean)
                    : []);
            hash.update(indexColumns.slice().sort().join(","));
            hash.update(String(index.unique));
        }

        const sortedConstraints = [...(schema.constraints ?? [])].sort((a, b) => a.name.localeCompare(b.name));
        for (const constraint of sortedConstraints) {
            hash.update(String(constraint.name ?? ""));
            hash.update(String(constraint.type ?? ""));
            const constraintColumns = Array.isArray(constraint.columns)
                ? constraint.columns
                : (typeof constraint.columns === "string"
                    ? String(constraint.columns).replace(/^\{|\}$/g, "").split(",").filter(Boolean)
                    : []);
            hash.update(constraintColumns.slice().sort().join(","));
            hash.update(constraint.definition ?? "null");
        }

        return hash.digest("hex");
    }

    async getTableSchema(shardId: string, tableName: string, schemaName: string = "public"): Promise<TableSchemaInfo> {
        const pool = this.pool;

        const columnResult = await pool.query(
            `SELECT column_name, data_type, is_nullable, column_default,
                    character_maximum_length, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position`,
            [schemaName, tableName],
        );

        const columns: ColumnSchemaInfo[] = (columnResult.rows ?? []).map((row) => ({
            name: row.column_name,
            dataType: row.data_type,
            isNullable: row.is_nullable === "YES",
            defaultValue: row.column_default ?? null,
            characterMaximumLength: row.character_maximum_length ? Number(row.character_maximum_length) : null,
            numericPrecision: row.numeric_precision ? Number(row.numeric_precision) : null,
            numericScale: row.numeric_scale ? Number(row.numeric_scale) : null,
        }));

        const indexResult = await pool.query(
            `SELECT indexname, array_agg(column_name ORDER BY column_name) as columns,
                    indisunique as unique
             FROM pg_indexes
             WHERE schemaname = $1 AND tablename = $2
             GROUP BY indexname, indisunique`,
            [schemaName, tableName],
        );

        const indexes: IndexSchemaInfo[] = (indexResult.rows ?? []).map((row) => ({
            name: row.indexname,
            columns: row.columns,
            unique: row.indisunique,
        }));

        const constraintResult = await pool.query(
            `SELECT con.conname as name, con.contype as type,
                    pg_get_constraintdef(con.oid) as definition,
                    array_agg(att.attname ORDER BY att.attnum) as columns
             FROM pg_constraint con
             JOIN pg_class c ON c.oid = con.conrelid
             JOIN pg_attribute att ON att.attrelid = con.conrelid
             WHERE c.relname = $2
             GROUP BY con.conname, con.contype, pg_get_constraintdef(con.oid)`,
            [schemaName, tableName],
        );

        const constraints: ConstraintSchemaInfo[] = (constraintResult.rows ?? []).map((row) => ({
            name: row.conname,
            type: row.contype as "primary_key" | "foreign_key" | "unique" | "check",
            columns: row.columns,
            definition: row.pg_get_constraintdef ?? undefined,
        }));

        return {
            name: tableName,
            columns,
            indexes,
            constraints,
        };
    }
}

function normalizeTable(schema: any): TableSchemaInfo {
    if (!schema) return { name: "", columns: [], indexes: [], constraints: [] };
    return {
        name: schema.name ?? "",
        columns: schema.columns ?? [],
        indexes: schema.indexes ?? [],
        constraints: schema.constraints ?? [],
    };
}
