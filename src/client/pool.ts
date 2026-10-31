import { Pool } from "pg";
import type { DatabaseConfig, Logger } from "../types.js";
import { loadDatabaseConfig } from "./config.js";

/**
 * Create a `pg.Pool` from environment configuration merged with explicit
 * overrides. Only connection-safe fields are logged; credentials are never
 * included in log output.
 */
export function createPool({
    logger,
    config = {},
    onError,
}: {
    logger?: Logger;
    config?: Partial<DatabaseConfig>;
    onError?: (error: Error) => void;
} = {}): Pool {
    const mergedConfig = loadDatabaseConfig(config);
    const pool = new Pool(mergedConfig);

    const idleTimeout = mergedConfig.idleTransactionTimeout;
    if (idleTimeout?.enabled) {
        logger?.info?.(
            {
                database: mergedConfig.database,
                applicationName: mergedConfig.application_name,
                idleTransactionTimeoutMs: idleTimeout.timeout,
            },
            "Database idle-in-transaction timeout enabled",
        );
    }

    pool.on("connect", () => {
        logger?.info?.(
            {
                database: mergedConfig.database,
                applicationName: mergedConfig.application_name,
                maxConnections: mergedConfig.max,
            },
            "Database connection established",
        );
    });

    pool.on("acquire", () => {
        logger?.debug?.(
            { totalCount: pool.totalCount, idleCount: pool.idleCount },
            "Database client acquired",
        );
    });

    pool.on("remove", () => {
        logger?.debug?.(
            { totalCount: pool.totalCount, idleCount: pool.idleCount },
            "Database client removed",
        );
    });

    pool.on("error", (error) => {
        logger?.error?.(
            {
                err: error,
                applicationName: mergedConfig.application_name,
            },
            "Unexpected database error",
        );
        onError?.(error);
    });

    return pool;
}