import fs from "node:fs";
import type { PoolConfig } from "pg";
import type { DatabaseConfig, IdleTransactionTimeout } from "../types.js";

const DEFAULT_APPLICATION_NAME = "Unknown App";
const DEFAULT_POOL_MAX = 20;
const DEFAULT_IDLE_TIMEOUT = 30_000;
const DEFAULT_CONNECTION_TIMEOUT = 5_000;

function readEnv(env: NodeJS.ProcessEnv | undefined, ...names: string[]): string | undefined {
    for (const name of names) {
        const value = env?.[name];
        if (value !== undefined && value !== null && value !== "") {
            return value;
        }
    }
    return undefined;
}

function parseNumber(value: string | number | boolean | undefined, defaultValue?: number): number | undefined {
    if (value === undefined || value === null || value === "" || value === false || isNaN(Number(value))) {
        return defaultValue;
    }
    return Number(value);
}

function parseBoolean(value: string | boolean | number | undefined, defaultValue = false): boolean {
    if (value === undefined || value === null || value === "") {
        return defaultValue;
    }
    return value === "true" || value === true || value === "1" || value === 1;
}

export function parseSslConfig(env: NodeJS.ProcessEnv = process.env, logger?: { warn?: (meta: object, msg: string) => void }): PoolConfig["ssl"] {
    if (!parseBoolean(readEnv(env, "DB_SSL", "DATABASE_SSL"))) {
        return false;
    }

    const caPath = readEnv(env, "DB_SSL_CA", "DATABASE_SSL_CA");
    const rejectUnauthorized = parseBoolean(
        readEnv(env, "DB_SSL_REJECT_UNAUTHORIZED", "DATABASE_SSL_REJECT_UNAUTHORIZED"),
        undefined,
    );

    if (caPath) {
        if (fs.existsSync(caPath)) {
            return {
                rejectUnauthorized: rejectUnauthorized ?? true,
                ca: fs.readFileSync(caPath, "utf8"),
            };
        }
        logger?.warn?.({ caPath }, "SSL CA file not found, falling back to unverified SSL (rejectUnauthorized forced to false)");
        return { rejectUnauthorized: false };
    }

    return {
        rejectUnauthorized: rejectUnauthorized ?? false,
    };
}

function withOptional<T>(target: Record<string, unknown>, key: string, value: T): void {
    if (value !== undefined && value !== null) {
        target[key] = value;
    }
}

const IDLE_TRANSACTION_TIMEOUT_MAX = 2147483647;

function validateIdleTransactionTimeout(
    value: IdleTransactionTimeout | undefined,
): IdleTransactionTimeout | undefined {
    if (value === undefined) {
        return undefined;
    }

    if (!value.enabled) {
        return { enabled: false };
    }

    const { timeout } = value;

    if (timeout === undefined || timeout === null) {
        throw new TypeError(
            "idleTransactionTimeout requires a numeric timeout (milliseconds) when enabled",
        );
    }

    if (typeof timeout !== "number") {
        throw new TypeError(
            "idleTransactionTimeout timeout must be a number (milliseconds)",
        );
    }

    if (!Number.isFinite(timeout)) {
        throw new RangeError(
            "idleTransactionTimeout timeout must be a finite number",
        );
    }

    if (timeout <= 0) {
        throw new RangeError(
            "idleTransactionTimeout timeout must be greater than 0 when enabled",
        );
    }

    if (timeout > IDLE_TRANSACTION_TIMEOUT_MAX) {
        throw new RangeError(
            `idleTransactionTimeout timeout must not exceed ${IDLE_TRANSACTION_TIMEOUT_MAX}ms`,
        );
    }

    return { enabled: true, timeout: Math.floor(timeout) };
}

function mergeIdleTransactionOptions(
    existingOptions: string | undefined,
    idleTimeout: IdleTransactionTimeout | undefined,
): string | undefined {
    if (!idleTimeout?.enabled) {
        return existingOptions;
    }

    const setting = `-c idle_in_transaction_session_timeout=${idleTimeout.timeout}`;

    if (!existingOptions) {
        return setting;
    }

    const cleaned = existingOptions
        .replace(/-c\s+idle_in_transaction_session_timeout=\S*/gi, "")
        .replace(/\bidle_in_transaction_session_timeout=\S*/gi, "")
        .replace(/\s+/g, " ")
        .trim();

    return cleaned ? `${cleaned} ${setting}` : setting;
}

/**
 * Build a `pg` configuration object from environment variables and explicit
 * overrides. This is the single place in the package that reads configuration
 * from the environment.
 *
 * Explicit overrides always win over environment values. `undefined` overrides
 * are ignored so callers can spread partial configuration safely.
 */
export function loadDatabaseConfig(
    overrides: Partial<DatabaseConfig> & { logger?: { warn?: (meta: object, msg: string) => void } } = {},
    env: NodeJS.ProcessEnv = process.env,
): DatabaseConfig {
    const config: Record<string, unknown> = {};

    withOptional(
        config,
        "connectionString",
        overrides.connectionString ?? readEnv(env, "DATABASE_URL"),
    );
    withOptional(config, "host", overrides.host ?? readEnv(env, "DATABASE_HOST", "PGHOST"));
    (config as Record<string, unknown>).port = parseNumber(
        overrides.port ?? readEnv(env, "DATABASE_PORT", "PGPORT"),
        undefined,
    );
    withOptional(
        config,
        "database",
        overrides.database ?? readEnv(env, "DATABASE_NAME", "PGDATABASE"),
    );
    withOptional(config, "user", overrides.user ?? readEnv(env, "DATABASE_USER", "PGUSER"));
    withOptional(
        config,
        "password",
        overrides.password ?? readEnv(env, "DATABASE_PASSWORD", "PGPASSWORD"),
    );

    config.max = parseNumber(
        overrides.max ?? readEnv(env, "DATABASE_POOL_MAX", "DB_MAX_CONN", "DB_POOL_SIZE"),
        DEFAULT_POOL_MAX,
    ) as unknown;
    config.min = parseNumber(
        overrides.min ?? readEnv(env, "DATABASE_POOL_MIN", "DB_MIN_CONN"),
        0,
    ) as unknown;
    config.idleTimeoutMillis = parseNumber(
        overrides.idleTimeoutMillis ?? readEnv(env, "DB_IDLE_TIMEOUT", "DATABASE_IDLE_TIMEOUT"),
        DEFAULT_IDLE_TIMEOUT,
    ) as unknown;
    config.connectionTimeoutMillis = parseNumber(
        overrides.connectionTimeoutMillis ??
            readEnv(env, "DATABASE_CONNECTION_TIMEOUT", "DB_CONN_TIMEOUT"),
        DEFAULT_CONNECTION_TIMEOUT,
    ) as unknown;

    const statementTimeout = parseNumber(
        overrides.statement_timeout ??
            readEnv(env, "DATABASE_STATEMENT_TIMEOUT", "DB_STATEMENT_TIMEOUT"),
        undefined,
    );
    if (statementTimeout !== undefined) {
        config.statement_timeout = statementTimeout as unknown;
    }

    const queryTimeout = parseNumber(
        overrides.query_timeout ?? readEnv(env, "DATABASE_QUERY_TIMEOUT", "DB_QUERY_TIMEOUT"),
        undefined,
    );
    if (queryTimeout !== undefined) {
        config.query_timeout = queryTimeout as unknown;
    }

    config.keepAlive = overrides.keepAlive ?? parseBoolean(readEnv(env, "DB_KEEP_ALIVE"), true);
    config.application_name =
        overrides.application_name ??
        readEnv(env, "DB_APP_NAME", "DATABASE_APP_NAME") ??
        DEFAULT_APPLICATION_NAME;

    config.ssl = overrides.ssl ?? parseSslConfig(env, overrides.logger as { warn?: (meta: object, msg: string) => void } | undefined);

    const idleTimeout = validateIdleTransactionTimeout(
        overrides.idleTransactionTimeout,
    );

    const mergedOptions = mergeIdleTransactionOptions(
        overrides.options ?? readEnv(env, "PGOPTIONS"),
        idleTimeout,
    );
    if (mergedOptions) {
        config.options = mergedOptions;
    }

    for (const [key, value] of Object.entries(overrides)) {
        if (
            value === undefined ||
            key === "idleTransactionTimeout" ||
            key in config
        ) {
            continue;
        }
        config[key] = value;
    }

    return config as DatabaseConfig;
}

export const dbConfig = loadDatabaseConfig();