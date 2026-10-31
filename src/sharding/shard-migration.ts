import type {
    MigrationRecord,
    MigrationStatus,
    MigrationStatusName,
} from "../types.js";
import { ShardManager } from "./shard-manager.js";

/**
 * Migration lock key namespace for advisory locks.
 * We use a fixed integer to avoid collisions with other advisory lock users.
 */
const MIGRATION_LOCK_KEY = 0x73686172; // "shar" in hex

/**
 * DDL for the migration tracking table. Created on each shard when
 * persistent tracking is first initialized.
 */
const MIGRATION_TABLE_DDL = `
CREATE TABLE IF NOT EXISTS shard_migrations (
    migration_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('pending', 'applied', 'failed')),
    applied_at TIMESTAMPTZ,
    error TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
)
`;

/**
 * Tracks schema migration state across all shards and provides
 * coordinated migration execution.
 *
 * Migration state is persisted to a `shard_migrations` table on each shard,
 * so it survives process restarts. An in-memory cache provides fast reads.
 *
 * Migrations are executed within a transaction (BEGIN/COMMIT) on each shard,
 * leveraging PostgreSQL's transactional DDL support. An advisory lock
 * (pg_advisory_lock) prevents concurrent execution of the same migration
 * on the same shard.
 */
export class ShardMigrationTracker {
    private migrations = new Map<string, Map<string, MigrationRecord>>();
    private initialized = new Set<string>();
    private initLocks = new Map<string, Promise<void>>();

    constructor(
        private readonly manager: ShardManager,
    ) {
        // Initialize migration records for all existing shards
        for (const shardId of manager.shardIds) {
            this.migrations.set(shardId, new Map());
        }
    }

    /**
     * Initialize durable migration tracking on all known shards.
     * Ensures the `shard_migrations` table exists on each shard and loads
     * persisted migration state into the in-memory cache. Idempotent and
     * safe for concurrent calls. Throws if any shard's tracking table
     * cannot be created or read.
     */
    async initialize(): Promise<void> {
        const errors: Error[] = [];
        await Promise.all(
            this.manager.shardIds.map(async (shardId) => {
                try {
                    await this.initShard(shardId);
                    await this.loadShardState(shardId);
                } catch (error) {
                    errors.push(error instanceof Error ? error : new Error(String(error)));
                }
            }),
        );
        if (errors.length > 0) {
            throw new Error(
                `Failed to initialize migration tracking on ${errors.length} shard(s): ${errors
                    .map((e) => e.message)
                    .join("; ")}`,
            );
        }
    }

    /**
     * Ensure the migration tracking table exists on a shard.
     * Idempotent and safe for concurrent calls via per-shard advisory lock.
     * After creating the table, loads persisted state into the cache.
     */
    async initShard(shardId: string): Promise<void> {
        // Fast path: already initialized in this process
        if (this.initialized.has(shardId)) {
            return;
        }

        // Per-shard lock to prevent concurrent initialization races
        const existingLock = this.initLocks.get(shardId);
        if (existingLock) {
            await existingLock;
            return;
        }

        const lockPromise = (async () => {
            try {
                const shard = this.manager.getShard(shardId);
                if (!shard || !shard.pool) {
                    throw new Error(`Shard "${shardId}" pool not available`);
                }

                // Use an advisory lock to make DDL idempotent across processes
                const lockKey = 0x73686173; // "shas" — migration schema namespace
                await shard.pool.query(`SELECT pg_advisory_lock($1)`, [lockKey]);
                try {
                    // Check if already initialized by another process while we waited
                    if (this.initialized.has(shardId)) {
                        return;
                    }

                    await shard.pool.query(MIGRATION_TABLE_DDL);
                    this.initialized.add(shardId);
                } finally {
                    await shard.pool.query(`SELECT pg_advisory_unlock($1)`, [lockKey]).catch(() => {});
                }
            } finally {
                this.initLocks.delete(shardId);
            }
        })();

        this.initLocks.set(shardId, lockPromise);
        await lockPromise;
    }

    /**
     * Load migration state from a shard's tracking table into the in-memory cache.
     * Called during initialize() and can be called explicitly to refresh state.
     */
    async loadShardState(shardId: string): Promise<void> {
        const shard = this.manager.getShard(shardId);
        if (!shard || !shard.pool) {
            return;
        }
        try {
            const result = await shard.pool.query(
                `SELECT migration_id, status, applied_at, error FROM shard_migrations`
            );
            const records = new Map<string, MigrationRecord>();
            for (const row of result.rows) {
                records.set(row.migration_id, {
                    migrationId: row.migration_id,
                    shardId,
                    status: row.status as MigrationStatusName,
                    appliedAt: row.applied_at,
                    error: row.error,
                });
            }
            this.migrations.set(shardId, records);
            this.initialized.add(shardId);
        } catch (_error) {
            // Table may not exist yet; that's OK, it will be created on first migration
        }
    }

    /**
     * Register a new shard for migration tracking. Called automatically
     * when a shard is added via the ShardingApi.
     */
    registerShard(shardId: string): void {
        if (!this.migrations.has(shardId)) {
            this.migrations.set(shardId, new Map());
        }
    }

    /**
     * Unregister a shard from migration tracking. Called automatically
     * when a shard is removed via the ShardingApi.
     */
    unregisterShard(shardId: string): void {
        this.migrations.delete(shardId);
        this.initialized.delete(shardId);
    }

    /**
     * Record the result of a migration on a specific shard.
     * Updates both the in-memory cache and the persistent tracking table.
     */
    async recordMigration(
        shardId: string,
        migrationId: string,
        status: MigrationStatusName,
        error?: string,
    ): Promise<void> {
        // Update in-memory cache
        let shardMigrations = this.migrations.get(shardId);
        if (!shardMigrations) {
            shardMigrations = new Map();
            this.migrations.set(shardId, shardMigrations);
        }

        const existing = shardMigrations.get(migrationId);
        shardMigrations.set(migrationId, {
            migrationId,
            shardId,
            status,
            appliedAt: status === "applied" ? new Date().toISOString() : undefined,
            error: status === "failed" ? error : existing?.error,
        });

        // Persist to the shard's tracking table
        const shard = this.manager.getShard(shardId);
        if (!shard || !shard.pool) {
            return;
        }
        try {
            await shard.pool.query(
                `INSERT INTO shard_migrations (migration_id, status, applied_at, error, updated_at)
                 VALUES ($1, $2, $3, $4, NOW())
                 ON CONFLICT (migration_id)
                 DO UPDATE SET status = $2, applied_at = $3, error = $4, updated_at = NOW()`,
                [migrationId, status, status === "applied" ? new Date().toISOString() : null, error]
            );
        } catch (_err) {
            // If the table doesn't exist yet, try to create it and retry once
            try {
                await this.initShard(shardId);
                await shard.pool.query(
                    `INSERT INTO shard_migrations (migration_id, status, applied_at, error, updated_at)
                     VALUES ($1, $2, $3, $4, NOW())
                     ON CONFLICT (migration_id)
                     DO UPDATE SET status = $2, applied_at = $3, error = $4, updated_at = NOW()`,
                    [migrationId, status, status === "applied" ? new Date().toISOString() : null, error]
                );
            } catch (_retryErr) {
                // Logging failure to persist should not fail the migration itself
            }
        }
    }

    /**
     * Get the status of a specific migration across all shards.
     * Reads from the in-memory cache (populated by runMigrationOnAllShards
     * or loadShardState).
     */
    getMigrationStatus(migrationId: string): MigrationStatus | undefined {
        const shardStatuses: Record<string, MigrationStatusName> = {};
        let allApplied = true;
        let anyFailed = false;

        for (const [shardId, records] of this.migrations) {
            const record = records.get(migrationId);
            if (!record) {
                shardStatuses[shardId] = "pending";
                allApplied = false;
            } else {
                shardStatuses[shardId] = record.status;
                if (record.status !== "applied") {
                    allApplied = false;
                }
                if (record.status === "failed") {
                    anyFailed = true;
                }
            }
        }

        if (Object.keys(shardStatuses).length === 0) {
            return undefined;
        }

        return {
            migrationId,
            shards: shardStatuses,
            allApplied,
            anyFailed,
        };
    }

    /**
     * Get all migration records for a specific shard.
     */
    getShardMigrationStatus(shardId: string): MigrationRecord[] {
        const records = this.migrations.get(shardId);
        if (!records) {
            return [];
        }
        return Array.from(records.values());
    }

    /**
     * Run a migration (arbitrary SQL) against all shards.
     * Each migration is executed within a transaction (BEGIN/COMMIT) on its
     * shard, leveraging PostgreSQL's transactional DDL support.
     *
     * An advisory lock (pg_advisory_lock) is acquired per migration per shard
     * to prevent concurrent execution of the same migration.
     *
     * Continues on individual shard failures and reports per-shard results.
     */
    async runMigrationOnAllShards(migrationId: string, sql: string): Promise<MigrationStatus> {
        const shardStatuses: Record<string, MigrationStatusName> = {};

        // Run migration on each shard sequentially (to avoid connection storms)
        for (const shardId of this.manager.shardIds) {
            const shard = this.manager.getShard(shardId);
            if (!shard || !shard.pool) {
                shardStatuses[shardId] = "failed";
                await this.recordMigration(shardId, migrationId, "failed", "Shard pool not available");
                continue;
            }

            // Ensure tracking table exists
            await this.initShard(shardId);

            // Acquire advisory lock to prevent concurrent execution of same migration
            const lockKey = (MIGRATION_LOCK_KEY << 16) | (hashString(migrationId) & 0xFFFF);
            try {
                await shard.pool.query(`SELECT pg_advisory_lock($1)`, [lockKey]);
        } catch (_error) {
            const errMsg = _error instanceof Error ? _error.message : String(_error);
            shardStatuses[shardId] = "failed";
            await this.recordMigration(shardId, migrationId, "failed", `Lock acquisition failed: ${errMsg}`);
            continue;
        }

            try {
                // Execute migration within a transaction for atomicity
                await shard.pool.query("BEGIN");
                try {
                    await shard.pool.query(sql);
                    await shard.pool.query("COMMIT");
                    shardStatuses[shardId] = "applied";
                    await this.recordMigration(shardId, migrationId, "applied");
                } catch (error) {
                    try {
                        await shard.pool.query("ROLLBACK");
                    } catch {
                        // Ignore rollback errors
                    }
                    const errMsg = error instanceof Error ? error.message : String(error);
                    shardStatuses[shardId] = "failed";
                    await this.recordMigration(shardId, migrationId, "failed", errMsg);
                }
            } finally {
                // Always release the advisory lock
                try {
                    await shard.pool.query(`SELECT pg_advisory_unlock($1)`, [lockKey]);
                } catch {
                    // Ignore unlock errors
                }
            }
        }

        const allApplied = Object.values(shardStatuses).every((s) => s === "applied");
        const anyFailed = Object.values(shardStatuses).some((s) => s === "failed");

        return {
            migrationId,
            shards: shardStatuses,
            allApplied,
            anyFailed,
        };
    }

    /**
     * Get migration statistics across all shards.
     */
    getMigrationStats(): {
        totalMigrations: number;
        totalShards: number;
        fullyApplied: number;
        partiallyApplied: number;
        failedMigrations: number;
    } {
        const allMigrationIds = new Set<string>();
        for (const records of this.migrations.values()) {
            for (const migrationId of records.keys()) {
                allMigrationIds.add(migrationId);
            }
        }

        let fullyApplied = 0;
        let partiallyApplied = 0;
        let failedMigrations = 0;

        for (const migrationId of allMigrationIds) {
            const status = this.getMigrationStatus(migrationId);
            if (!status) continue;
            if (status.allApplied) {
                fullyApplied += 1;
            } else if (status.anyFailed) {
                failedMigrations += 1;
            } else {
                partiallyApplied += 1;
            }
        }

        return {
            totalMigrations: allMigrationIds.size,
            totalShards: this.migrations.size,
            fullyApplied,
            partiallyApplied,
            failedMigrations,
        };
    }
}

/**
 * Simple hash function to convert a string to a 16-bit integer
 * for use as part of an advisory lock key.
 */
function hashString(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash);
}
