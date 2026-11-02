// ---------------------------------------------------------------------------
// Sync Lock Manager — distributed locking for schema sync operations.
//
// Uses PostgreSQL table-based locks (schema_sync_locks) to prevent concurrent
// sync operations. Locks expire automatically to avoid deadlocks on crash.
// ---------------------------------------------------------------------------

import type { Pool } from "pg";
import { SCHEMA_SYNC_SCHEMA } from "./schema-sync-schema.js";

export interface SyncLockOptions {
    /** Lock TTL in milliseconds (default: 300000) */
    ttlMs?: number;
    /** Maximum time to wait for lock acquisition in ms (default: 1000) */
    acquireTimeoutMs?: number;
    /** Retry interval in ms while waiting for the lock (default: 50) */
    retryIntervalMs?: number;
}

export interface SyncLockRow {
    lockName: string;
    holder: string;
    expiresAt: Date;
}

export class SyncLockManager {
    private readonly pool: Pool;
    private schemaReady?: Promise<void>;
    private async ensureSchema(): Promise<void> {
        if (!this.schemaReady) this.schemaReady = this.pool.query(SCHEMA_SYNC_SCHEMA).then(()=>undefined).catch(()=>undefined);
        await this.schemaReady;
    }
    private readonly ttlMs: number;
    private readonly acquireTimeoutMs: number;
    private readonly retryIntervalMs: number;

    constructor(pool: Pool, options: SyncLockOptions = {}) {
        this.pool = pool;
        this.ttlMs = options.ttlMs ?? 300_000;
        this.acquireTimeoutMs = options.acquireTimeoutMs ?? 0;
        this.retryIntervalMs = options.retryIntervalMs ?? 50;
    }

    /**
     * Acquire a named lock for `holder` with the given (or default) TTL.
     * Waits up to acquireTimeoutMs, retrying on conflicts.
     */
    async acquireLock(lockName: string, holder: string = "default", timeoutMs?: number): Promise<boolean> {
        await this.ensureSchema();
        const ttl = timeoutMs ?? this.ttlMs;
        const deadline = Date.now() + this.acquireTimeoutMs;
        while (true) {
            await this.pool.query(
                `DELETE FROM schema_sync_locks WHERE lock_name = $1 AND expires_at <= NOW()`,
                [lockName],
            ).catch(() => undefined);

            try {
                await this.pool.query(
                    `INSERT INTO schema_sync_locks (lock_name, lock_holder, expires_at, created_at)
                     VALUES ($1, $2, NOW() + ($3::bigint * INTERVAL '1 millisecond'), NOW())`,
                    [lockName, holder, ttl],
                );
                return true;
            } catch (error) {
                const code = (error as { code?: string })?.code;
                if (code !== "23505" && !String(error).includes("duplicate")) {
                    // Unknown error: treat as not acquired
                }
                // locked
            }

            if (Date.now() >= deadline) {
                return false;
            }
            await new Promise((resolve) => setTimeout(resolve, this.retryIntervalMs));
        }
    }

    /** Release a lock held by `holder`. */
    async releaseLock(lockName: string, holder?: string): Promise<void> {
        await this.ensureSchema();
        if (holder) {
            await this.pool.query(
                `DELETE FROM schema_sync_locks WHERE lock_name = $1 AND lock_holder = $2`,
                [lockName, holder],
            );
        } else {
            await this.pool.query(
                `DELETE FROM schema_sync_locks WHERE lock_name = $1`,
                [lockName],
            );
        }
    }

    /** Check if a lock is currently held (unexpired). */
    async isLocked(lockName: string): Promise<boolean> {
        await this.ensureSchema();
        const result = await this.pool.query(
            `SELECT 1 FROM schema_sync_locks WHERE lock_name = $1 AND expires_at > NOW() LIMIT 1`,
            [lockName],
        );
        return (result.rows?.length ?? 0) > 0;
    }

    /** Get lock info, if held. */
    async getLock(lockName: string): Promise<SyncLockRow | null> {
        await this.ensureSchema();
        const result = await this.pool.query(
            `SELECT lock_name, lock_holder, expires_at FROM schema_sync_locks WHERE lock_name = $1`,
            [lockName],
        );
        if (!result.rows?.length) return null;
        const row = result.rows[0];
        return {
            lockName: row.lock_name,
            holder: row.lock_holder,
            expiresAt: new Date(row.expires_at),
        };
    }

    /** Remove all expired locks. */
    async cleanupExpiredLocks(): Promise<void> {
        await this.ensureSchema();
        await this.pool.query(`DELETE FROM schema_sync_locks WHERE expires_at <= NOW()`);
    }
    async acquire(name: string, holder?: string, ttl?: number): Promise<boolean> { return this.acquireLock(name, holder, ttl); }
    async release(name: string, holder?: string): Promise<void> { return this.releaseLock(name, holder); }
    async locked(name: string): Promise<boolean> { return this.isLocked(name); }
    async info(name: string): Promise<any> { return this.getLock(name); }
    async cleanup(): Promise<void> { return this.cleanupExpiredLocks(); }
}
