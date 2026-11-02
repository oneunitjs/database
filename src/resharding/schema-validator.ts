export type SchemaIssueSeverity = "error" | "warning" | "info";

export interface ColumnSchema {
    name: string;
    dataType: string;
    nullable: boolean;
    defaultValue?: string | null;
    ordinalPosition: number;
}

export interface ConstraintSchema {
    name: string;
    type: "primary_key" | "foreign_key" | "unique" | "check";
    columns: string[];
    definition?: string;
}

export interface IndexSchema {
    name: string;
    columns: string[];
    unique: boolean;
}

export interface TableSchema {
    name: string;
    columns: ColumnSchema[];
    constraints: ConstraintSchema[];
    indexes: IndexSchema[];
}

export interface SchemaCompatibilityIssue {
    table: string;
    severity: SchemaIssueSeverity;
    code: string;
    message: string;
    suggestion?: string;
}

export interface SchemaCompatibilityReport {
    compatible: boolean;
    issues: SchemaCompatibilityIssue[];
    tablesCompared: number;
}

const COMPATIBLE_TYPE_WIDENING: Record<string, string[]> = {
    int2: ["int4", "int8", "numeric", "text"],
    int4: ["int8", "numeric", "text"],
    int8: ["numeric", "text"],
    float4: ["float8", "numeric", "text"],
    float8: ["numeric", "text"],
    numeric: ["text"],
    varchar: ["text"],
    char: ["varchar", "text"],
    date: ["timestamp", "timestamptz", "text"],
    timestamp: ["timestamptz", "text"],
};

function normalizeType(dataType: string): string {
    return dataType.trim().toLowerCase().replace(/\s+/g, "");
}

function typesCompatible(source: string, dest: string): boolean {
    const s = normalizeType(source);
    const d = normalizeType(dest);
    if (s === d) {
        return true;
    }
    const widened = COMPATIBLE_TYPE_WIDENING[s];
    return widened ? widened.includes(d) : false;
}

export class SchemaComparator {
    compare(source: TableSchema, destination: TableSchema, strict = false): SchemaCompatibilityIssue[] {
        const issues: SchemaCompatibilityIssue[] = [];
        const destCols = new Map(destination.columns.map((c) => [c.name, c]));
        const sourceCols = new Map(source.columns.map((c) => [c.name, c]));

        for (const col of source.columns) {
            const dest = destCols.get(col.name);
            if (!dest) {
                issues.push({
                    table: source.name,
                    severity: "error",
                    code: "MISSING_COLUMN",
                    message: `Destination table "${destination.name}" is missing column "${col.name}"`,
                    suggestion: `Add column "${col.name}" ${col.dataType} to the destination`,
                });
                continue;
            }
            if (!typesCompatible(col.dataType, dest.dataType)) {
                issues.push({
                    table: source.name,
                    severity: "error",
                    code: "INCOMPATIBLE_TYPE",
                    message: `Column "${col.name}" type ${col.dataType} is incompatible with destination ${dest.dataType}`,
                    suggestion: `Alter destination column "${col.name}" to a type compatible with ${col.dataType}`,
                });
            }
            if (col.nullable && !dest.nullable) {
                issues.push({
                    table: source.name,
                    severity: "error",
                    code: "NULLABILITY_MISMATCH",
                    message: `Column "${col.name}" is nullable on source but NOT NULL on destination`,
                    suggestion: `Make destination column "${col.name}" nullable or backfill NULLs before cutover`,
                });
            }
            if (strict && col.ordinalPosition !== dest.ordinalPosition) {
                issues.push({
                    table: source.name,
                    severity: "warning",
                    code: "COLUMN_ORDER",
                    message: `Column "${col.name}" ordinal differs (source ${col.ordinalPosition}, dest ${dest.ordinalPosition})`,
                });
            }
        }

        for (const col of destination.columns) {
            if (!sourceCols.has(col.name) && !col.nullable && col.defaultValue == null) {
                issues.push({
                    table: source.name,
                    severity: "error",
                    code: "EXTRA_NOT_NULL_COLUMN",
                    message: `Destination has extra NOT NULL column "${col.name}" without a default`,
                    suggestion: `Add a default or make "${col.name}" nullable`,
                });
            }
        }

        const destPk = destination.constraints.find((c) => c.type === "primary_key");
        const sourcePk = source.constraints.find((c) => c.type === "primary_key");
        if (sourcePk && destPk) {
            const src = [...sourcePk.columns].sort().join(",");
            const dst = [...destPk.columns].sort().join(",");
            if (src !== dst) {
                issues.push({
                    table: source.name,
                    severity: "error",
                    code: "PRIMARY_KEY_MISMATCH",
                    message: `Primary key columns differ (source [${sourcePk.columns.join(", ")}], dest [${destPk.columns.join(", ")}])`,
                });
            }
        } else if (sourcePk && !destPk) {
            issues.push({
                table: source.name,
                severity: "error",
                code: "MISSING_PRIMARY_KEY",
                message: `Destination table "${destination.name}" is missing a primary key`,
            });
        }

        const destUnique = new Set(
            destination.indexes.filter((i) => i.unique).map((i) => [...i.columns].sort().join(",")),
        );
        for (const index of source.indexes.filter((i) => i.unique)) {
            const key = [...index.columns].sort().join(",");
            if (!destUnique.has(key)) {
                issues.push({
                    table: source.name,
                    severity: "warning",
                    code: "MISSING_UNIQUE_INDEX",
                    message: `Destination is missing unique index on (${index.columns.join(", ")})`,
                    suggestion: `Create a unique index matching "${index.name}"`,
                });
            }
        }

        return issues;
    }
}

export class SchemaValidator {
    constructor(private readonly comparator = new SchemaComparator()) {}

    validate(
        sourceTables: TableSchema[],
        destinationTables: TableSchema[],
        options: { strict?: boolean } = {},
    ): SchemaCompatibilityReport {
        const destByName = new Map(destinationTables.map((t) => [t.name, t]));
        const issues: SchemaCompatibilityIssue[] = [];
        let tablesCompared = 0;
        for (const source of sourceTables) {
            const dest = destByName.get(source.name);
            if (!dest) {
                issues.push({
                    table: source.name,
                    severity: "error",
                    code: "MISSING_TABLE",
                    message: `Destination is missing table "${source.name}"`,
                    suggestion: `Create table "${source.name}" on the destination shard`,
                });
                continue;
            }
            tablesCompared += 1;
            issues.push(...this.comparator.compare(source, dest, options.strict === true));
        }
        return {
            compatible: issues.every((i) => i.severity !== "error"),
            issues,
            tablesCompared,
        };
    }
}
