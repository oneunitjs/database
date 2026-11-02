import type { Pool } from "pg";

/**
 * Status of a fenced node.
 */
export interface FencingStatus {
    nodeId: string;
    fenced: boolean;
    reason?: string;
    activatedAt?: Date;
    priority?: number;
}

/**
 * Options for activating fencing on a node.
 */
export interface FencingOptions {
    /** TTL in ms after which the fence automatically expires. Default: no expiry. */
    ttlMs?: number;
    /** Priority of the fencing lock. Default: 1. */
    priority?: number;
    /** Additional metadata. */
    metadata?: Record<string, unknown>;
}

/**
 * Hooks for fencing lifecycle events.
 */
export interface FencingHooks {
    onFencingActivated?: (nodeId: string, reason: string) => void;
    onFencingDeactivated?: (nodeId: string) => void;
    onFencingCheckFailed?: (nodeId: string, error: Error) => void;
}

/**
 * Error thrown when a node is fenced.
 */
export class FencedError extends Error {
    readonly nodeId: string;

    constructor(nodeId: string, message: string) {
        super(message);
        this.nodeId = nodeId;
        this.name = "FencedError";
    }
}

/**
 * Result of a fencing activation.
 */
export interface FencingActivationResult {
    nodeId: string;
    success: boolean;
    error?: string;
}

/**
 * Provider interface for fencing nodes (preventing them from taking primary role).
 */
export interface FencingProvider {
    activate(nodeId: string, reason: string, options?: FencingOptions): Promise<void>;
    deactivate(nodeId: string): Promise<void>;
    isFenced(nodeId: string): Promise<boolean>;
    check(nodeId: string): Promise<void>;
    getFencedNodes(): Promise<string[]>;
    getStatus(nodeId: string): Promise<FencingStatus>;
    cleanup(nodeId: string): Promise<void>;
}

/**
 * SQL schema for fencing tables. Execute these against your PostgreSQL database
 * before using `AdvisoryLockFencingProvider`.
 */
export const FENCING_SCHEMA = `
CREATE TABLE IF NOT EXISTS _database_fencing_locks (
    node_id TEXT NOT NULL PRIMARY KEY,
    lock_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    priority INTEGER NOT NULL DEFAULT 1,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _database_fencing_history (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT,
    priority INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    performed_by TEXT NOT NULL,
    performed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

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
 * Fencing provider backed by PostgreSQL advisory locks. Uses pg_advisory_lock
 * to block a node from assuming the primary role, with a tracking table for
 * observability and history.
 */
export class AdvisoryLockFencingProvider implements FencingProvider {
    constructor(
        private readonly pool: Pool,
        private readonly logger: RequiredFailoverLogger,
        private readonly hooks: FencingHooks = {},
    ) {}

    async activate(nodeId: string, reason: string, options: FencingOptions = {}): Promise<void> {
        const { ttlMs, priority = 1, metadata = {} } = options;
        const lockId = this.generateLockId(nodeId);
        const client = await this.pool.connect();
        try {
            await client.query("SELECT pg_advisory_lock($1)", [lockId]);
            await client.query(
                `INSERT INTO _database_fencing_locks
                 (node_id, lock_id, reason, expires_at, priority, metadata, activated_by)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (node_id) DO UPDATE SET
                     lock_id = EXCLUDED.lock_id,
                     reason = EXCLUDED.reason,
                     expires_at = EXCLUDED.expires_at,
                     priority = EXCLUDED.priority,
                     metadata = EXCLUDED.metadata,
                     activated_at = NOW(),
                     activated_by = EXCLUDED.activated_by`,
                [
                    nodeId,
                    lockId,
                    reason,
                    ttlMs ? new Date(Date.now() + ttlMs) : null,
                    priority,
                    JSON.stringify(metadata),
                    "advisory-lock-provider",
                ]
            );
            await client.query(
                `INSERT INTO _database_fencing_history
                 (node_id, action, reason, priority, metadata, performed_by)
                 VALUES ($1, $2, $3, $4, $5, $6)`,
                [nodeId, "activate", reason, priority, JSON.stringify(metadata), "advisory-lock-provider"]
            );
            this.logger.info({ nodeId, reason, lockId }, "Advisory lock fencing activated");
            await this.hooks.onFencingActivated?.(nodeId, reason);
        } finally {
            client.release();
        }
    }

    async deactivate(nodeId: string): Promise<void> {
        const client = await this.pool.connect();
        try {
            const result = await client.query(
                "SELECT lock_id FROM _database_fencing_locks WHERE node_id = $1",
                [nodeId]
            );
            if (result.rows.length > 0) {
                const lockId = result.rows[0].lock_id;
                await client.query("SELECT pg_advisory_unlock($1)", [lockId]);
                await client.query("DELETE FROM _database_fencing_locks WHERE node_id = $1", [
                    nodeId,
                ]);
                await client.query(
                    `INSERT INTO _database_fencing_history
                     (node_id, action, performed_by)
                     VALUES ($1, $2, $3)`,
                    [nodeId, "deactivate", "advisory-lock-provider"]
                );
            }
            this.logger.info({ nodeId }, "Advisory lock fencing deactivated");
            await this.hooks.onFencingDeactivated?.(nodeId);
        } finally {
            client.release();
        }
    }

    async isFenced(nodeId: string): Promise<boolean> {
        const result = await this.pool.query(
            "SELECT 1 FROM _database_fencing_locks WHERE node_id = $1",
            [nodeId]
        );
        return result.rows.length > 0;
    }

    async check(nodeId: string): Promise<void> {
        const fenced = await this.isFenced(nodeId);
        if (fenced) {
            const error = new FencedError(nodeId, "Node is fenced");
            this.logger.warn({ nodeId }, "Fencing check failed");
            await this.hooks.onFencingCheckFailed?.(nodeId, error);
            throw error;
        }
    }

    async getFencedNodes(): Promise<string[]> {
        const result = await this.pool.query("SELECT node_id FROM _database_fencing_locks");
        return result.rows.map((row) => row.node_id);
    }

    async getStatus(nodeId: string): Promise<FencingStatus> {
        const result = await this.pool.query(
            "SELECT reason, expires_at, priority, activated_at FROM _database_fencing_locks WHERE node_id = $1",
            [nodeId]
        );
        if (result.rows.length === 0) {
            return { nodeId, fenced: false };
        }
        const row = result.rows[0];
        return {
            nodeId,
            fenced: true,
            reason: row.reason,
            priority: row.priority,
            activatedAt: new Date(row.activated_at),
        };
    }

    async cleanup(nodeId: string): Promise<void> {
        await this.deactivate(nodeId);
    }

    private generateLockId(nodeId: string): number {
        let hash = 0;
        for (let i = 0; i < nodeId.length; i++) {
            const char = nodeId.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash; // Convert to 32-bit integer
        }
        return Math.abs(hash);
    }
}

/**
 * Table-based fencing provider. Uses only the `_database_fencing_locks` table
 * (no PostgreSQL advisory locks). Suitable when advisory locks are unavailable
 * or undesirable. Fencing state is purely tracked in the tracking table.
 */
export class TableBasedFencingProvider implements FencingProvider {
    constructor(
        private readonly pool: Pool,
        private readonly logger: RequiredFailoverLogger,
        private readonly hooks: FencingHooks = {},
    ) {}

    async activate(nodeId: string, reason: string, options: FencingOptions = {}): Promise<void> {
        const { ttlMs, priority = 1, metadata = {} } = options;
        const expiresAt = ttlMs ? new Date(Date.now() + ttlMs) : null;
        const client = await this.pool.connect();
        try {
            await client.query(
                `INSERT INTO _database_fencing_locks
                 (node_id, lock_id, reason, expires_at, priority, metadata, activated_by)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (node_id) DO UPDATE SET
                     lock_id = EXCLUDED.lock_id,
                     reason = EXCLUDED.reason,
                     expires_at = EXCLUDED.expires_at,
                     priority = EXCLUDED.priority,
                     metadata = EXCLUDED.metadata,
                     activated_at = NOW(),
                     activated_by = EXCLUDED.activated_by`,
                [
                    nodeId,
                    this.generateLockId(nodeId),
                    reason,
                    expiresAt,
                    priority,
                    JSON.stringify(metadata),
                    "table-based-provider",
                ]
            );
            await client.query(
                `INSERT INTO _database_fencing_history
                 (node_id, action, reason, priority, metadata, performed_by)
                 VALUES ($1, $2, $3, $4, $5, $6)`,
                [nodeId, "activate", reason, priority, JSON.stringify(metadata), "table-based-provider"]
            );
            this.logger.info({ nodeId, reason, priority }, "Table-based fencing activated");
            await this.hooks.onFencingActivated?.(nodeId, reason);
        } finally {
            client.release();
        }
    }

    async deactivate(nodeId: string): Promise<void> {
        const client = await this.pool.connect();
        try {
            const result = await client.query(
                "SELECT 1 FROM _database_fencing_locks WHERE node_id = $1",
                [nodeId]
            );
            if (result.rows.length > 0) {
                await client.query("DELETE FROM _database_fencing_locks WHERE node_id = $1", [
                    nodeId,
                ]);
                await client.query(
                    `INSERT INTO _database_fencing_history
                     (node_id, action, performed_by)
                     VALUES ($1, $2, $3)`,
                    [nodeId, "deactivate", "table-based-provider"]
                );
            }
            this.logger.info({ nodeId }, "Table-based fencing deactivated");
            await this.hooks.onFencingDeactivated?.(nodeId);
        } finally {
            client.release();
        }
    }

    async isFenced(nodeId: string): Promise<boolean> {
        const result = await this.pool.query(
            "SELECT 1 FROM _database_fencing_locks WHERE node_id = $1",
            [nodeId]
        );
        return result.rows.length > 0;
    }

    async check(nodeId: string): Promise<void> {
        const fenced = await this.isFenced(nodeId);
        if (fenced) {
            const error = new FencedError(nodeId, "Node is fenced");
            this.logger.warn({ nodeId }, "Fencing check failed");
            await this.hooks.onFencingCheckFailed?.(nodeId, error);
            throw error;
        }
    }

    async getFencedNodes(): Promise<string[]> {
        const result = await this.pool.query("SELECT node_id FROM _database_fencing_locks");
        return result.rows.map((row) => row.node_id);
    }

    async getStatus(nodeId: string): Promise<FencingStatus> {
        const result = await this.pool.query(
            "SELECT reason, expires_at, priority, activated_at FROM _database_fencing_locks WHERE node_id = $1",
            [nodeId]
        );
        if (result.rows.length === 0) {
            return { nodeId, fenced: false };
        }
        const row = result.rows[0];
        return {
            nodeId,
            fenced: true,
            reason: row.reason,
            priority: row.priority,
            activatedAt: new Date(row.activated_at),
        };
    }

    async cleanup(nodeId: string): Promise<void> {
        await this.deactivate(nodeId);
    }

    private generateLockId(nodeId: string): number {
        let hash = 0;
        for (let i = 0; i < nodeId.length; i++) {
            const char = nodeId.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        return Math.abs(hash);
    }
}

/**
 * Default no-op hooks.
 */
export const defaultFencingHooks: FencingHooks = {};
