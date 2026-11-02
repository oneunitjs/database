export type ColumnOptions = {
    columnName?: string;
    nullable?: boolean;
    primaryKey?: boolean;
    unique?: boolean;
    default?: string;
    references?: { table: string; column: string; onDelete?: string; onUpdate?: string };
};

export type NumericColumnOptions = ColumnOptions & {
    precision?: number;
    scale?: number;
};

export type CharacterColumnOptions = ColumnOptions & {
    length?: number;
};

export type ArrayColumnOptions = ColumnOptions & {
    of?: string;
};

export type GeneratedColumnOptions = ColumnOptions & {
    generated?: "ALWAYS" | "BY DEFAULT";
    identity?: boolean;
};

export function id(options: {
    columnName?: string;
    primaryKey?: boolean;
    generated?: boolean;
} = {}): Record<string, string> {
    const { columnName = "id", primaryKey = true, generated = true } = options;
    let type = "UUID";
    if (primaryKey) {
        type += " PRIMARY KEY";
    }
    if (generated) {
        type += " DEFAULT gen_random_uuid()";
    }
    return { [columnName]: type };
}

export function bigIntId(options: {
    columnName?: string;
    primaryKey?: boolean;
} = {}): Record<string, string> {
    const { columnName = "id", primaryKey = true } = options;
    return { [columnName]: primaryKey ? "BIGSERIAL PRIMARY KEY" : "BIGSERIAL" };
}

export function uuid(columnName = "id", options: { primaryKey?: boolean; generated?: boolean } = {}): Record<string, string> {
    const { primaryKey = false, generated = true } = options;
    const type = generated ? "UUID DEFAULT gen_random_uuid()" : "UUID";
    return { [columnName]: primaryKey ? `${type} PRIMARY KEY` : type };
}

export function createdAt(options: { columnName?: string; nullable?: boolean } = {}): Record<string, string> {
    const { columnName = "created_at", nullable = false } = options;
    return { [columnName]: `TIMESTAMPTZ${nullable ? "" : " NOT NULL"} DEFAULT CURRENT_TIMESTAMP` };
}

export function updatedAt(options: { columnName?: string; nullable?: boolean } = {}): Record<string, string> {
    const { columnName = "updated_at", nullable = false } = options;
    return { [columnName]: `TIMESTAMPTZ${nullable ? "" : " NOT NULL"} DEFAULT CURRENT_TIMESTAMP` };
}

/**
 * Text column with optional length constraint.
 */
export function text(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "text", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "TEXT";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Varchar column with optional length.
 */
export function varchar(options: CharacterColumnOptions = {}): Record<string, string> {
    const { columnName = "varchar", length = 255, nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = `VARCHAR(${length})`;
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Boolean column.
 */
export function boolean(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "boolean", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "BOOLEAN";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Integer column.
 */
export function integer(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "integer", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "INTEGER";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * BigInt column.
 */
export function bigint(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "bigint", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "BIGINT";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * SmallInt column.
 */
export function smallint(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "smallint", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "SMALLINT";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Numeric/Decimal column with optional precision and scale.
 */
export function numeric(options: NumericColumnOptions = {}): Record<string, string> {
    const { columnName = "numeric", nullable = false, primaryKey = false, unique = false, precision, scale, default: def } = options;
    let type = "NUMERIC";
    if (precision != null && scale != null) type += `(${precision},${scale})`;
    else if (precision != null) type += `(${precision})`;
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Decimal column (alias for numeric).
 */
export const decimal = numeric;

/**
 * Real column.
 */
export function real(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "real", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "REAL";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Double precision column.
 */
export function doublePrecision(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "double_precision", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "DOUBLE PRECISION";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Date column.
 */
export function date(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "date", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "DATE";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Timestamptz column.
 */
export function timestamptz(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "timestamptz", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "TIMESTAMPTZ";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Time column.
 */
export function time(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "time", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "TIME";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Timetz column.
 */
export function timetz(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "timetz", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "TIMETZ";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Interval column.
 */
export function interval(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "interval", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "INTERVAL";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * JSON column.
 */
export function json(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "json", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "JSON";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * JSONB column.
 */
export function jsonb(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "jsonb", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "JSONB";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Bytea column.
 */
export function bytea(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "bytea", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "BYTEA";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Array column.
 */
export function array(options: ArrayColumnOptions = {}): Record<string, string> {
    const { columnName = "array", of, nullable = false, primaryKey = false, unique = false, default: def } = options;
    if (!of) {
        throw new Error("Array column requires 'of' option specifying the element type");
    }
    let type = `${of.toUpperCase()}[]`;
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

export type EnumColumnOptions = ColumnOptions & {
    values?: string[];
};

/**
 * Enum column.
 */
export function enumColumn(options: EnumColumnOptions = {}): Record<string, string> {
    const { columnName = "enum", values, nullable = false, primaryKey = false, unique = false, default: def } = options;
    if (!values || values.length === 0) {
        throw new Error("Enum column requires 'values' option");
    }
    const enumValues = values.map(v => `'${String(v).replace(/'/g, "''")}'`).join(", ");
    let type = `ENUM (${enumValues})`;
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * Serial column (auto-incrementing integer).
 */
export function serial(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "serial", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "SERIAL";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}

/**
 * BigSerial column (auto-incrementing bigint).
 */
export function bigserial(options: ColumnOptions = {}): Record<string, string> {
    const { columnName = "bigserial", nullable = false, primaryKey = false, unique = false, default: def } = options;
    let type = "BIGSERIAL";
    if (primaryKey) type += " PRIMARY KEY";
    if (unique) type += " UNIQUE";
    if (def != null) type += ` DEFAULT ${def}`;
    if (!nullable) type += " NOT NULL";
    return { [columnName]: type };
}
