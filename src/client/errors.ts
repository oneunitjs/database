import type { NormalizedError } from "../types.js";

const TRANSIENT_ERROR_CODES = new Set([
    // Class 08 - Connection Exception
    "08000",
    "08001",
    "08003",
    "08004",
    "08006",
    "08007",
    "08P01",
    // Class 40 - Transaction Rollback
    "40001", // serialization_failure
    "40002", // transaction_integrity_constraint_violation
    "40P01", // deadlock_detected
    // Class 53 - Insufficient Resources
    "53000",
    "53100",
    "53200",
    "53300",
    "53400",
    // Class 55 - Object Not In Prerequisite State
    "55006",
    "55P03",
    // Class 57 - Operator Intervention
    "57P01",
    "57P02",
    "57P03",
    "57P05",
]);

const TRANSIENT_SYSTEM_CODES = new Set([
    "ECONNREFUSED",
    "ECONNRESET",
    "ETIMEDOUT",
    "EPIPE",
    "ENOTFOUND",
    "EAI_AGAIN",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "ENETDOWN",
    "ENOTCONN",
    "EPROTO",
    "CONNECTION_CLOSED",
    "CONNECTION_ENDED",
    "CONNECTION_DESTROYED",
]);

const UNIQUE_VIOLATION = "23505";
const FOREIGN_KEY_VIOLATION = "23503";
const NOT_NULL_VIOLATION = "23502";
const CHECK_VIOLATION = "23514";
const EXCLUSION_VIOLATION = "23P01";
const SERIALIZATION_FAILURE = "40001";
const DEADLOCK_DETECTED = "40P01";

/**
 * Normalized error type for every failure produced by the package. Wrapping
 * driver errors in a single, serializable shape keeps consumers decoupled from
 * `pg` internals while preserving the original error as `cause`.
 */
export class DatabaseError extends Error {
    code?: string;
    detail?: string;
    hint?: string;
    constraint?: string;
    table?: string;
    column?: string;
    schema?: string;
    dataType?: string;
    severity?: string;
    position?: string;
    where?: string;
    routine?: string;
    sqlState?: string;
    transient: boolean;
    cause?: Error;

    constructor(message: string, options: {
        code?: string;
        detail?: string;
        hint?: string;
        constraint?: string;
        table?: string;
        column?: string;
        schema?: string;
        dataType?: string;
        severity?: string;
        position?: string;
        where?: string;
        routine?: string;
        sqlState?: string;
        transient?: boolean;
        cause?: Error;
        stack?: string;
    } = {}) {
        super(message ?? "Database error");
        this.name = "DatabaseError";

        this.code = options.code;
        this.detail = options.detail;
        this.hint = options.hint;
        this.constraint = options.constraint;
        this.table = options.table;
        this.column = options.column;
        this.schema = options.schema;
        this.dataType = options.dataType;
        this.severity = options.severity;
        this.position = options.position;
        this.where = options.where;
        this.routine = options.routine;
        this.sqlState = options.sqlState ?? options.code;
        this.transient = options.transient ?? isTransientError(options.cause ?? options);
        this.cause = options.cause;

        if (options.stack) {
            this.stack = options.stack;
        }
    }

    toJSON(): Record<string, unknown> {
        return {
            name: this.name,
            message: this.message,
            code: this.code,
            constraint: this.constraint,
            table: this.table,
            column: this.column,
            detail: this.detail,
            hint: this.hint,
            transient: this.transient,
        };
    }
}

export class TimeoutError extends DatabaseError {
    timeout?: number;

    constructor(message = "Operation timed out", options: { code?: string; transient?: boolean; timeout?: number; cause?: Error } = {}) {
        super(message, {
            code: "TIMEOUT",
            transient: true,
            ...options,
        });
        this.name = "TimeoutError";
        this.timeout = options.timeout;
        this.transient = true;
    }
}

export function isDatabaseError(error: unknown): error is DatabaseError {
    return error instanceof DatabaseError;
}

export function normalizeError(error: unknown): DatabaseError {
    if (error instanceof DatabaseError) {
        return error;
    }

    if (!error || typeof error !== "object") {
        return new DatabaseError(String(error ?? "Unknown database error"));
    }

    const err = error as Record<string, unknown>;
    return new DatabaseError(String(err.message ?? "Database error"), {
        code: err.code as string,
        detail: err.detail as string,
        hint: err.hint as string,
        constraint: err.constraint as string,
        table: err.table as string,
        column: err.column as string,
        schema: err.schema as string,
        dataType: err.dataType as string,
        severity: err.severity as string,
        position: err.position as string,
        where: err.where as string,
        routine: err.routine as string,
        sqlState: err.sqlState as string,
        transient: isTransientError(error),
        cause: err instanceof Error ? err : undefined,
    });
}

export function isTransientError(error: unknown): boolean {
    if (!error) {
        return false;
    }

    if (error instanceof TimeoutError) {
        return true;
    }

    const code = (error as Record<string, unknown>).code ??
        (error as Record<string, unknown>).errno ??
        (error as Record<string, unknown>).sqlState;
    if (code && (TRANSIENT_ERROR_CODES.has(String(code)) || TRANSIENT_SYSTEM_CODES.has(String(code)))) {
        return true;
    }

    return false;
}

export function isUniqueViolation(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === UNIQUE_VIOLATION;
}

export function isForeignKeyViolation(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === FOREIGN_KEY_VIOLATION;
}

export function isNotNullViolation(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === NOT_NULL_VIOLATION;
}

export function isCheckViolation(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === CHECK_VIOLATION;
}

export function isExclusionViolation(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === EXCLUSION_VIOLATION;
}

export function isSerializationFailure(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === SERIALIZATION_FAILURE;
}

export function isDeadlock(error: unknown): boolean {
    return ((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState) === DEADLOCK_DETECTED;
}

export function isConnectionError(error: unknown): boolean {
    const code = (error as Record<string, unknown>)?.code ??
        (error as Record<string, unknown>)?.errno ??
        (error as Record<string, unknown>)?.sqlState;
    if (!code) {
        return false;
    }
    return (
        String(code).startsWith("08") ||
        TRANSIENT_SYSTEM_CODES.has(String(code))
    );
}

export function isConstraintViolation(error: unknown): boolean {
    const code = String((error as Record<string, unknown>)?.code ?? (error as Record<string, unknown>)?.sqlState ?? "");
    return code.startsWith("23");
}