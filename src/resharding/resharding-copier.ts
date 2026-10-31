// ---------------------------------------------------------------------------
// ReshardingCopier — bounded, resumable batch copying with keyset pagination.
//
// Reads rows from source shards in bounded batches (keyset cursor, never
// offset pagination), writes them idempotently to destination shards, and
// persists checkpoints only after the corresponding destination writes commit.
// Change events arriving during the copy are drained during catching_up.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient } from "pg";
import type {
    MigrationDescriptor,
    MigrationTableAdapter,
    MigrationUnit,
    ProgressEvent,
    ValidationReport,
    ValidationOptions,
    CheckpointCursor,
    ChangeEvent,
} from "./resharding-types.js";
import type { Logger } from "../types.js";
import { ReshardingChangeCapture } from "./resharding-change-capture.js";
import { ReshardingStore } from "./resharding-store.js";

const DEFAULT_BATCH_SIZE = 1000;

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTransaction<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        const result = await fn(client);
        await client.query("COMMIT");
        return result;
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Ignore rollback errors
        }
        throw error;
    } finally {
        client.release();
    }
}

export interface BulkCopyOptions {
    batchSize: number;
    maxConcurrency: number;
    maxRetries: number;
    baseRetryDelayMs: number;
    maxRetryDelayMs: number;
    throttleMs: number;
    batchTimeoutMs: number;
    idleTimeoutMs: number;
}

export class ReshardingCopier {
    private readonly coordination: Pool;
    private readonly manager: { getShard(id: string): { pool?: Pool } | undefined } | undefined;
    private readonly changeCapture: ReshardingChangeCapture;
    private readonly store: ReshardingStore;
    private readonly options: BulkCopyOptions;
    private readonly logger?: Logger;
    private readonly hooks?: { onProgress?: (e: ProgressEvent) => void };
    private readonly initShard?: (shardId: string, pool: Pool) => Promise<void>;
    private readonly initializedShards = new Set<string>();
    private currentDescriptor: MigrationDescriptor | null = null;
    private snapshotIds: Map<string, string> | undefined;

    constructor(
        coordination: Pool,
        manager: { getShard(id: string): { pool?: Pool } | undefined } | undefined,
        changeCapture: ReshardingChangeCapture,
        options: BulkCopyOptions,
        logger?: Logger,
        hooks?: { onProgress?: (e: ProgressEvent) => void },
        initShard?: (shardId: string, pool: Pool) => Promise<void>,
    ) {
        this.coordination = coordination;
        this.manager = manager;
        this.changeCapture = changeCapture;
        this.store = new ReshardingStore(coordination, logger);
        this.options = options;
        this.logger = logger;
        this.hooks = hooks;
        this.initShard = initShard;
    }

    private async ensureShardInitialized(shardId: string, pool: Pool): Promise<void> {
        if (this.initializedShards.has(shardId)) return;

        // If the schema tables already exist (common case: initShard() was
        // called during database bootstrap), avoid re-running DDL here. This
        // keeps an in-flight source transaction from being blocked by shard
        // schema initialization (which takes a session advisory lock).
        try {
            const client = await pool.connect();
            try {
                const res = await client.query(
                    `SELECT to_regclass('public.resharding_changes') AS present`,
                );
                if (res.rows[0]?.present) {
                    this.initializedShards.add(shardId);
                    return;
                }
            } finally {
                client.release();
            }
        } catch {
            // Fall through to initShard; it is idempotent.
        }

        if (this.initShard) {
            await this.initShard(shardId, pool);
        }
        this.initializedShards.add(shardId);
    }

    /** Copy all units for a migration, bounded by concurrency. */
    async copyAll(
        migrationId: string,
        descriptor: MigrationDescriptor,
        snapshotIds?: Map<string, string>,
    ): Promise<void> {
        this.currentDescriptor = descriptor;
        this.snapshotIds = snapshotIds;
        const units = await this.planUnitsForMigration(migrationId);
        if (units.length === 0) return;

        const sem = new Semaphore(this.options.maxConcurrency);
        const errors: Error[] = [];

        await Promise.all(units.map((unit) =>
            sem.run(() => this.copyUnit(migrationId, descriptor, unit).catch((e) => {
                errors.push(e);
            }))
        ));

        if (errors.length > 0) throw errors[0];
    }

    private async planUnitsForMigration(migrationId: string): Promise<MigrationUnit[]> {
        // Read units from the coordination store.
        const client = await this.coordination.connect();
        try {
            const result = await client.query(
                `SELECT unit_id, unit_type, source_shard_id, destination_shard_id, shard_key,
                        hash_bucket, range_start, range_end, estimated_rows, status, rows_copied,
                        last_checkpoint, updated_at
                 FROM resharding_units WHERE migration_id = $1 ORDER BY unit_id`,
                [migrationId],
            );
            return result.rows.map((r) => ({
                unitId: r.unit_id,
                unitType: r.unit_type,
                sourceShardId: r.source_shard_id,
                destinationShardId: r.destination_shard_id,
                shardKey: r.shard_key,
                hashBucket: r.hash_bucket,
                rangeStart: r.range_start,
                rangeEnd: r.range_end,
                estimatedRows: r.estimated_rows,
                status: r.status,
                rowsCopied: Number(r.rows_copied ?? 0),
                lastCheckpoint: (r.last_checkpoint as CheckpointCursor | undefined) ?? undefined,
                updatedAt: r.updated_at,
            }));
        } finally {
            client.release();
        }
    }

    private async copyUnit(
        migrationId: string,
        descriptor: MigrationDescriptor,
        unit: MigrationUnit,
    ): Promise<void> {
        const table = this.findTable(descriptor, unit);
        if (!table) {
            // No adapter for this unit's table; skip with a warning.
            this.logger?.warn?.({ migrationId, unitId: unit.unitId }, "No table adapter for unit; skipping copy");
            return;
        }

        const sourcePool = this.getPool(unit.sourceShardId);
        const destPool = this.getPool(unit.destinationShardId);
        if (!sourcePool || !destPool) {
            throw new Error(`Pools unavailable for unit ${unit.unitId}: source=${unit.sourceShardId} dest=${unit.destinationShardId}`);
        }
        await this.ensureShardInitialized(unit.sourceShardId, sourcePool);
        await this.ensureShardInitialized(unit.destinationShardId, destPool);

        const snapshotId = this.snapshotIds?.get(unit.sourceShardId);

        let cursor: CheckpointCursor | null = unit.lastCheckpoint ?? null;
        let done = false;

        while (!done) {
        const batch = await this.readBatchWithRetry(sourcePool, table, unit, cursor, snapshotId);
            if (batch.rows.length === 0 || batch.done) {
                done = true;
                if (batch.rows.length === 0) break;
            }

            // Write to destination idempotently within a transaction, with retries.
            let lastWriteError: Error | undefined;
            for (let attempt = 0; attempt <= this.options.maxRetries; attempt++) {
                try {
                    await withTransaction(destPool, async (client) => {
                        const result = await table.upsertRows({
                            client: client as unknown as PoolClient,
                            destinationShardId: unit.destinationShardId,
                            unit,
                            rows: batch.rows,
                        });
                        return result;
                    });
                    lastWriteError = undefined;
                    break;
                } catch (error) {
                    lastWriteError = error instanceof Error ? error : new Error(String(error));
                    if (attempt === this.options.maxRetries) throw lastWriteError;
                    await sleep(this.backoff(attempt));
                }
            }

            // Advance checkpoint only after destination writes committed.
            cursor = batch.nextCursor;
            await this.store.updateUnitCheckpoint(migrationId, unit.unitId, {
                status: "copying",
                rowsCopied: unit.rowsCopied + batch.rows.length,
                lastCheckpoint: cursor,
            });

            this.emitProgress({
                type: "batch-copied",
                migrationId,
                unitId: unit.unitId,
                timestamp: new Date().toISOString(),
                rowsCopied: batch.rows.length,
            });

            if (this.options.throttleMs > 0) await sleep(this.options.throttleMs);
        }

        await this.store.updateUnitCheckpoint(migrationId, unit.unitId, { status: "caught_up" });
    }

    private async readBatchWithRetry(
        pool: Pool,
        table: MigrationTableAdapter,
        unit: MigrationUnit,
        cursor: CheckpointCursor | null,
        _snapshotId?: string,
    ) {
        let lastError: Error | undefined;
        for (let attempt = 0; attempt <= this.options.maxRetries; attempt++) {
            try {
                return await withTransaction(pool, async (client) => {
                    return table.readBatch({
                        client: client as unknown as PoolClient,
                        sourceShardId: unit.sourceShardId,
                        unit,
                        batchSize: this.options.batchSize,
                        cursor,
                    });
                });
            } catch (error) {
                lastError = error instanceof Error ? error : new Error(String(error));
                if (attempt === this.options.maxRetries) throw lastError;
                await sleep(this.backoff(attempt));
            }
        }
        throw lastError ?? new Error("read batch failed");
    }

    private backoff(attempt: number): number {
        const ms = Math.min(
            this.options.baseRetryDelayMs * Math.pow(2, attempt),
            this.options.maxRetryDelayMs,
        );
        return ms + Math.floor(Math.random() * 100);
    }

    private findTable(descriptor: MigrationDescriptor, unit: MigrationUnit): MigrationTableAdapter | undefined {
        // Units map to tables via tableName; fall back to the first table for
        // single-table migrations or when the planner did not set tableName.
        if (unit.tableName) {
            return descriptor.tables.find((t) => t.table === unit.tableName);
        }
        return descriptor.tables[0];
    }

    /**
     * Apply a batch of change events idempotently at the destination.
     * Events are grouped by source transaction (sourceTxId) and each group
     * is applied in a single destination transaction to preserve atomicity.
     *
     * @param changeEvents - Ordered by commit_ts ASC, txid ASC, seq ASC.
     * Events from the same source transaction are contiguous.
     */
    async applyChangeEvents(
        migrationId: string,
        unit: MigrationUnit,
        changeEvents: ChangeEvent[],
    ): Promise<void> {
        const descriptor = this.currentDescriptor;
        if (!descriptor) {
            throw new Error(`No active migration descriptor for applying change events ${migrationId}`);
        }
        const table = this.findTable(descriptor, unit);
        if (!table) return;

        const destPool = this.getPool(unit.destinationShardId);
        if (!destPool) {
            throw new Error(`Destination pool unavailable for unit ${unit.unitId}: ${unit.destinationShardId}`);
        }

        await this.ensureShardInitialized(unit.destinationShardId, destPool);

        // Group consecutive events by sourceTxId for atomic destination application
        const groups: ChangeEvent[][] = [];
        let currentGroup: ChangeEvent[] = [];
        let currentTxId: number | null = null;

        for (const event of changeEvents) {
            if (currentTxId === null || event.sourceTxId !== currentTxId) {
                if (currentGroup.length > 0) {
                    groups.push(currentGroup);
                }
                currentGroup = [event];
                currentTxId = event.sourceTxId;
            } else {
                currentGroup.push(event);
            }
        }
        if (currentGroup.length > 0) {
            groups.push(currentGroup);
        }

        for (const group of groups) {
            await withTransaction(destPool, async (client) => {
                for (const event of group) {
                    if (event.operation === "delete") {
                        if (table.deleteRows && event.shardKey) {
                            await table.deleteRows!({
                                client: client as unknown as import("pg").PoolClient,
                                destinationShardId: unit.destinationShardId,
                                unit,
                                shardKeys: [event.shardKey],
                            });
                        }
                    } else if (event.newRow) {
                        await table.upsertRows({
                            client: client as unknown as import("pg").PoolClient,
                            destinationShardId: unit.destinationShardId,
                            unit,
                            rows: [event.newRow],
                            changes: [event],
                        });
                    }
                }
            });
        }
    }

    private getPool(shardId: string): Pool | undefined {
        return this.manager?.getShard(shardId)?.pool;
    }

    private emitProgress(event: ProgressEvent): void {
        try {
            this.hooks?.onProgress?.(event);
        } catch {
            // Advisory hooks must not compromise consistency.
        }
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    async validateAll(
        migrationId: string,
        descriptor: MigrationDescriptor,
        row: { units: MigrationUnit[]; status: string; sourceRoutingVersion: number; targetRoutingVersion: number },
    ): Promise<{ report: ValidationReport[]; passed: boolean }> {
        const report: ValidationReport[] = [];
        let passed = true;

        for (const table of descriptor.tables) {
            for (const unit of row.units) {
                const rep = await this.validateUnit(migrationId, table, unit);
                report.push(rep);
                if (!rep.passed) passed = false;
            }
        }

        return { report, passed };
    }

    private async validateUnit(
        _migrationId: string,
        table: MigrationTableAdapter,
        unit: MigrationUnit,
    ): Promise<ValidationReport> {
        const failures: { code: string; message: string; table: string; unitId?: string; severity: "warning" | "error" }[] = [];

        let sourceCount = 0;
        let destinationCount = 0;
        let countMatch = true;
        let duplicateKeys: string[] = [];
        let missingKeys: string[] = [];
        let extraKeys: string[] = [];
        let checksumMatch: boolean | null = null;
        let sourceChecksum: string | null = null;
        let destinationChecksum: string | null = null;

        const sourcePool = this.getPool(unit.sourceShardId);
        const destPool = this.getPool(unit.destinationShardId);

        if (sourcePool && table.countRows) {
            sourceCount = await withTransaction(sourcePool, async (client) =>
                table.countRows!({ client: client as unknown as PoolClient, shardId: unit.sourceShardId, unit }),
            );
        }
        if (destPool && table.countRows) {
            destinationCount = await withTransaction(destPool, async (client) =>
                table.countRows!({ client: client as unknown as PoolClient, shardId: unit.destinationShardId, unit }),
            );
        }

        countMatch = sourceCount === destinationCount;
        if (!countMatch) {
            failures.push({
                code: "COUNT_MISMATCH",
                message: `source=${sourceCount} destination=${destinationCount}`,
                table: table.table,
                unitId: unit.unitId,
                severity: "error",
            });
        }

        if (table.computeChecksum && sourcePool && destPool) {
            const src = await withTransaction(sourcePool, async (client) =>
                table.computeChecksum!({ client: client as unknown as PoolClient, shardId: unit.sourceShardId, unit }),
            );
            const dst = await withTransaction(destPool, async (client) =>
                table.computeChecksum!({ client: client as unknown as PoolClient, shardId: unit.destinationShardId, unit }),
            );
            sourceChecksum = src.hash;
            destinationChecksum = dst.hash;
            checksumMatch = src.hash === dst.hash && src.rowCount === dst.rowCount;
            if (!checksumMatch) {
                failures.push({
                    code: "CHECKSUM_MISMATCH",
                    message: `source=${src.hash}(${src.rowCount}) destination=${dst.hash}(${dst.rowCount})`,
                    table: table.table,
                    unitId: unit.unitId,
                    severity: "error",
                });
            }
        }

        const passed = failures.length === 0;

        return {
            table: table.table,
            unitId: unit.unitId,
            sourceCount,
            destinationCount,
            countMatch,
            duplicateKeys,
            missingKeys,
            extraKeys,
            checksumMatch,
            sourceChecksum,
            destinationChecksum,
            unappliedChanges: 0,
            mismatchedDestination: 0,
            passed,
            failures,
        };
    }
}

class Semaphore {
    private readonly limit: number;
    private readonly queue: (() => void)[] = [];
    private active = 0;

    constructor(limit: number) {
        this.limit = Math.max(1, limit);
    }

    async run<T>(fn: () => Promise<T>): Promise<T> {
        if (this.active < this.limit) {
            this.active++;
            try {
                return await fn();
            } finally {
                this.active--;
                this.next();
            }
        }
        return new Promise<T>((resolve, reject) => {
            this.queue.push(() => {
                this.active++;
                fn().then(resolve, reject).finally(() => {
                    this.active--;
                    this.next();
                });
            });
        });
    }

    private next(): void {
        const next = this.queue.shift();
        if (next) next();
    }
}
