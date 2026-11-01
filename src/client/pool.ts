import { Pool } from "pg";
import type { PoolClient } from "pg";
import type { DatabaseConfig, Logger } from "../types.js";
import { loadDatabaseConfig } from "./config.js";

// Track all active connections for leak detection
const activeConnections = new Set<PoolClient>();

// Connection validation timeout (5 minutes)
const CONNECTION_VALIDATION_TIMEOUT = 300000;

/**
 * Validate a connection by executing a simple query.
 * Returns true if connection is healthy, false otherwise.
 * Only used when validateConnections option is enabled.
 */
async function validateConnection(client: PoolClient): Promise<boolean> {
    try {
        await client.query('SELECT 1');
        return true;
    } catch {
        return false;
    }
}

/**
 * Wrap a client to track its lifecycle and optionally validate on first use.
 */
function wrapClient(client: PoolClient, pool: Pool, logger?: Logger, validateConnections?: boolean): PoolClient {
    // Mark as not yet validated
    (client as { _validated?: boolean })._validated = false;
    
    // Track connection for leak detection
    activeConnections.add(client);
    
    // Set timeout for leak detection
    const leakTimeout = setTimeout(() => {
        if (activeConnections.has(client)) {
            logger?.warn?.({
                message: 'Connection leak detected',
                stack: new Error().stack,
                totalConnections: pool.totalCount,
                idleConnections: pool.idleCount,
            }, 'Database connection not released within 5 minutes - potential leak');
        }
    }, CONNECTION_VALIDATION_TIMEOUT);
    
    // Clear timeout when connection is released
    const originalRelease = client.release.bind(client);
    client.release = function(this: PoolClient, error?: Error) {
        clearTimeout(leakTimeout);
        activeConnections.delete(client);
        return originalRelease(error);
    };
    
    // Only add validation proxy if enabled
    if (validateConnections) {
        return new Proxy(client, {
            get(target, prop) {
                // On first query, validate the connection
                if (prop === 'query' && !(target as { _validated?: boolean })._validated) {
                    return async function(this: PoolClient, ...args: Parameters<PoolClient['query']>) {
                        const isValid = await validateConnection(this);
                        if (!isValid) {
                            // Connection is stale, mark as invalid and release
                            (target as { _validated?: boolean })._validated = false;
                            target.release(new Error('Connection validation failed'));
                            // Re-throw the validation error instead of retrying
                            throw new Error('Connection validation failed - connection is stale');
                        }
                        (target as { _validated?: boolean })._validated = true;
                        return target.query(...args);
                    };
                }
                return target[prop as keyof PoolClient];
            },
        });
    }
    
    return client;
}

/**
 * Create a `pg.Pool` from environment configuration merged with explicit
 * overrides. Only connection-safe fields are logged; credentials are never
 * included in log output.
 */
export function createPool({
    logger,
    config = {},
    onError,
    validateConnections = false,
}: {
    logger?: Logger;
    config?: Partial<DatabaseConfig>;
    onError?: (error: Error) => void;
    validateConnections?: boolean;
} = {}): Pool {
    const mergedConfig = loadDatabaseConfig(config);
    const pool = new Pool(mergedConfig);
    
    // Override connect to wrap clients with validation and leak detection
    const originalConnect = pool.connect.bind(pool);
    pool.connect = async function() {
        const client = await originalConnect();
        return wrapClient(client, pool, logger, validateConnections);
    };

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

    pool.on("connect", (client) => {
        logger?.info?.(
            {
                database: mergedConfig.database,
                applicationName: mergedConfig.application_name,
                maxConnections: mergedConfig.max,
                totalConnections: pool.totalCount,
                idleConnections: pool.idleCount,
            },
            "Database connection established",
        );
    });

    pool.on("acquire", () => {
        logger?.debug?.(
            { 
                totalCount: pool.totalCount, 
                idleCount: pool.idleCount,
                waitingCount: pool.waitingCount,
                pendingAcquireCount: (pool as any).pendingAcquireCount ?? 0,
            },
            "Database client acquired",
        );
    });

    pool.on("remove", (client) => {
        logger?.debug?.(
            { 
                totalCount: pool.totalCount, 
                idleCount: pool.idleCount,
                waitingCount: pool.waitingCount,
                // pendingAcquireCount is an internal property
                pendingAcquire: (pool as any).pendingAcquireCount ?? 0,
            },
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