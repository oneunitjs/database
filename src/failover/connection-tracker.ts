import type { PoolClient } from "pg";

/**
 * A connection currently tracked by the connection tracker.
 */
export interface TrackedConnection {
    id: string;
    client: PoolClient;
    nodeId: string;
    shardId: string;
    createdAt: Date;
    lastUsedAt: Date;
    inTransaction: boolean;
    transactionId?: string;
}

/**
 * Result of invalidating connections for a node.
 */
export interface ConnectionInvalidationResult {
    nodeId: string;
    invalidatedCount: number;
    activeTransactionCount: number;
    errors: string[];
}

/**
 * Options for the ConnectionTracker.
 */
export interface ConnectionTrackerOptions {
    /** Interval in ms for idle connection cleanup sweeps. Default: 60_000 (1 minute). */
    cleanupIntervalMs?: number;
    /** Max idle age in ms before a connection is cleaned up. Default: 300_000 (5 minutes). */
    maxIdleMs?: number;
    /** Logger used for diagnostics. */
    logger?: RequiredFailoverLogger;
}

/**
 * Default options for ConnectionTracker.
 */
export const DEFAULT_CONNECTION_TRACKER_OPTIONS: Required<ConnectionTrackerOptions> = {
    cleanupIntervalMs: 60_000,
    maxIdleMs: 300_000,
    logger: {
        debug: () => {},
        info: () => {},
        warn: () => {},
        error: () => {},
    },
};

/**
 * Logger type compatible with FailoverManager's RequiredFailoverLogger.
 */
export type RequiredFailoverLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

/**
 * Tracks active connections per node so they can be invalidated after a
 * failover. Connections participating in active transactions are reported
 * but not forcibly closed, to avoid data loss.
 */
export class ConnectionTracker {
    private readonly connections = new Map<string, Set<TrackedConnection>>();
    private readonly clientToConnection = new Map<PoolClient, TrackedConnection>();
    private cleanupTimer?: ReturnType<typeof setInterval>;
    private nextConnectionId = 0;
    private readonly options!: Required<ConnectionTrackerOptions>;

    constructor(
        options: Partial<ConnectionTrackerOptions> = {},
    ) {
        this.options = {
            ...DEFAULT_CONNECTION_TRACKER_OPTIONS,
            ...options,
            logger: options.logger ?? DEFAULT_CONNECTION_TRACKER_OPTIONS.logger,
        };
        this.startCleanup();
    }

    /**
     * Register a connection for a node/shard. Returns the tracked connection.
     */
    registerConnection(nodeId: string, shardId: string, client: PoolClient): TrackedConnection {
        const conn: TrackedConnection = {
            id: `conn_${++this.nextConnectionId}_${Date.now()}`,
            client,
            nodeId,
            shardId,
            createdAt: new Date(),
            lastUsedAt: new Date(),
            inTransaction: false,
        };

        if (!this.connections.has(nodeId)) {
            this.connections.set(nodeId, new Set());
        }
        this.connections.get(nodeId)!.add(conn);
        this.clientToConnection.set(client, conn);
        return conn;
    }

    /**
     * Unregister a connection. Returns true if the connection was tracked.
     */
    unregisterConnection(client: PoolClient): boolean {
        const conn = this.clientToConnection.get(client);
        if (!conn) return false;

        const nodeConns = this.connections.get(conn.nodeId);
        if (nodeConns) {
            nodeConns.delete(conn);
            if (nodeConns.size === 0) {
                this.connections.delete(conn.nodeId);
            }
        }
        this.clientToConnection.delete(client);
        return true;
    }

    /**
     * Mark a tracked connection as being inside a transaction.
     */
    markInTransaction(client: PoolClient, transactionId: string): void {
        const conn = this.clientToConnection.get(client);
        if (conn) {
            conn.inTransaction = true;
            conn.transactionId = transactionId;
        }
    }

    /**
     * Mark a tracked connection as no longer in a transaction.
     */
    markOutOfTransaction(client: PoolClient): void {
        const conn = this.clientToConnection.get(client);
        if (conn) {
            conn.inTransaction = false;
            conn.transactionId = undefined;
        }
    }

    /**
     * Touch a connection's last-used timestamp.
     */
    touch(client: PoolClient): void {
        const conn = this.clientToConnection.get(client);
        if (conn) {
            conn.lastUsedAt = new Date();
        }
    }

    /**
     * Get all tracked connections for a node.
     */
    getConnectionsForNode(nodeId: string): TrackedConnection[] {
        const conns = this.connections.get(nodeId);
        return conns ? [...conns] : [];
    }

    /**
     * Get the total number of tracked connections across all nodes.
     */
    getConnectionCount(): number {
        let count = 0;
        for (const conns of this.connections.values()) {
            count += conns.size;
        }
        return count;
    }

    /**
     * Invalidate all connections for a given node. Connections in active
     * transactions are counted but not closed, to avoid aborting in-flight
     * work. Returns an invalidation result.
     */
    async invalidateConnections(nodeId: string): Promise<ConnectionInvalidationResult> {
        const result: ConnectionInvalidationResult = {
            nodeId,
            invalidatedCount: 0,
            activeTransactionCount: 0,
            errors: [],
        };

        const nodeConns = this.connections.get(nodeId);
        if (!nodeConns || nodeConns.size === 0) return result;

        for (const conn of [...nodeConns]) {
            if (conn.inTransaction) {
                result.activeTransactionCount++;
                continue;
            }

            try {
                await this.invalidateConnection(conn);
                result.invalidatedCount++;
            } catch (error) {
                result.errors.push(error instanceof Error ? error.message : String(error));
            }
        }

        return result;
    }

    /**
     * Invalidate connections for every node except the given one. Returns one
     * result per invalidated node.
     */
    async invalidateConnectionsExcept(excludeNodeId: string): Promise<ConnectionInvalidationResult[]> {
        const results: ConnectionInvalidationResult[] = [];
        for (const nodeId of this.connections.keys()) {
            if (nodeId !== excludeNodeId) {
                results.push(await this.invalidateConnections(nodeId));
            }
        }
        return results;
    }

    /**
     * Close and release all tracked connections for every node.
     */
    async invalidateAllConnections(): Promise<ConnectionInvalidationResult[]> {
        const results: ConnectionInvalidationResult[] = [];
        for (const nodeId of this.connections.keys()) {
            results.push(await this.invalidateConnections(nodeId));
        }
        return results;
    }

    /**
     * Stop the idle-cleanup timer. Call this when shutting down.
     */
    stop(): void {
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = undefined;
        }
    }

    private async invalidateConnection(conn: TrackedConnection): Promise<void> {
        const client = conn.client;
        try {
            // Terminate the backend to force-close any in-flight query on this connection.
            try {
                await client.query("SELECT pg_terminate_backend(pg_backend_pid())");
            } catch {
                // Ignore — the connection may already be dead.
            }
        } finally {
            try {
                client.release();
            } catch {
                // Ignore release errors.
            }
            this.unregisterConnection(client);
        }
    }

    private startCleanup(): void {
        this.cleanupTimer = setInterval(() => {
            void this.cleanupIdleConnections().catch(() => {
                // Swallow cleanup errors — they are best-effort.
            });
        }, this.options.cleanupIntervalMs);
        if (this.cleanupTimer.unref) {
            this.cleanupTimer.unref();
        }
    }

    private async cleanupIdleConnections(): Promise<void> {
        const cutoff = new Date(Date.now() - this.options.maxIdleMs);
        for (const [nodeId, conns] of this.connections.entries()) {
            for (const conn of [...conns]) {
                if (conn.inTransaction) continue;
                if (conn.lastUsedAt < cutoff) {
                    try {
                        await this.invalidateConnection(conn);
                    } catch {
                        // Best-effort cleanup; ignore failures.
                    }
                }
            }
        }
    }
}
