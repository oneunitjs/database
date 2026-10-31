// ---------------------------------------------------------------------------
// ReshardingCutover — coordinated, recoverable cutover protocol.
//
// Cutover is a recoverable, durable operation:
//   1. Verify destination copy complete.
//   2. Drain the change log to a consistency boundary.
//   3. Fence writes for moved keys.
//   4. Publish the new routing version atomically in the coordination DB.
//   5. Record cutover_committed so recovery can determine the outcome.
//
// Writes are briefly paused during the drain+fence window. Stale routing
// configurations cannot write to the old shard after the routing version is
// published, because the router resolves the current version from the
// coordination authority.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient } from "pg";
import type { MigrationDescriptor, ProgressEvent } from "./resharding-types.js";
import type { MigrationRow } from "./resharding-store.js";
import type { ChangeEvent, MigrationUnit } from "./resharding-types.js";
import { ReshardingChangeCapture } from "./resharding-change-capture.js";
import { ReshardingStore } from "./resharding-store.js";
import { ReshardingCopier } from "./resharding-copier.js";
import type { Logger } from "../types.js";
import { fencingLockKey } from "./resharding-schema.js";

export class ReshardingCutover {
    private readonly coordination: Pool;
    private readonly manager: { getShard(id: string): { pool?: Pool } | undefined } | undefined;
    private readonly store: ReshardingStore;
    private readonly changeCapture: ReshardingChangeCapture;
    private readonly copier: ReshardingCopier;
    private readonly drainTimeoutMs: number;
    private readonly writeFenceTimeoutMs: number;
    private readonly requireValidation: boolean;
    private readonly logger?: Logger;
    private readonly hooks?: { onProgress?: (event: ProgressEvent) => void };

    constructor(
        coordination: Pool,
        manager: { getShard(id: string): { pool?: Pool } | undefined } | undefined,
        store: ReshardingStore,
        changeCapture: ReshardingChangeCapture,
        copier: ReshardingCopier,
        opts: { drainTimeoutMs: number; writeFenceTimeoutMs: number; requireValidation: boolean },
        logger?: Logger,
        hooks?: { onProgress?: (event: ProgressEvent) => void },
    ) {
        this.coordination = coordination;
        this.manager = manager;
        this.store = store;
        this.changeCapture = changeCapture;
        this.copier = copier;
        this.drainTimeoutMs = opts.drainTimeoutMs;
        this.writeFenceTimeoutMs = opts.writeFenceTimeoutMs;
        this.requireValidation = opts.requireValidation;
        this.logger = logger;
        this.hooks = hooks;
    }

    async commit(migrationId: string, descriptor: MigrationDescriptor, row: MigrationRow): Promise<void> {
        const start = Date.now();

        // 1. Verify destination copy complete: all units caught_up/completed.
        const units = row.units;
        const incomplete = units.filter((u) => u.status !== "caught_up" && u.status !== "completed" && u.status !== "validated");
        if (incomplete.length > 0) {
            throw new Error(`Cannot cut over: ${incomplete.length} unit(s) not caught up: ${incomplete.map((u) => u.unitId).join(",")}`);
        }

        // 2. Persist cutover intent on the coordination authority.
        await this.store.setCutoverIntent(migrationId);

        // 3. Acquire the exclusive write barrier on each source shard.
        //    This acquires transaction-level advisory locks and persists
        //    fenced = TRUE within each shard's transaction, but does NOT
        //    commit yet. The locks are held until we explicitly commit.
        const lockedClients = await this.acquireWriteBarrier(migrationId, units);

        try {
            // 4. Commit the write barrier FIRST. This persists fenced = TRUE
            //    on each source shard and releases the advisory locks. Writers
            //    are now blocked from committing to the old shard. We do this
            //    BEFORE publishing the routing version so that a crash between
            //    gate publication and routing publication leaves writes blocked
            //    (safe) rather than allowing stale commits (unsafe).
            await this.releaseWriteBarrier(lockedClients);

            // 5. Drain change log to consistency boundary.
            //    Only changes with txid >= capture_txid need replay; changes
            //    with txid < capture_txid are covered by the initial snapshot.
            await this.drainToBoundary(migrationId, units);

            // 6. Publish the new routing version atomically (commit protocol).
            //    At this point, the gate is already fenced, so no stale writer
            //    can commit after the new routing version is visible.
            await this.store.publishRoutingVersion(row.targetRoutingVersion, migrationId);

            this.logger?.info?.({ migrationId, targetRoutingVersion: row.targetRoutingVersion }, "Cutover committed");
        } catch (error) {
            // If we fail after committing the barrier but before publishing
            // the routing version, the gate remains fenced. Writers are
            // blocked. Recovery can retry the cutover or abort with the gate
            // still up (safe direction).
            throw error;
        }
        // No finally block releasing the barrier here — it was already
        // committed above. The barrier is released exactly once.
    }

    /**
     * Acquire an exclusive transaction-level advisory lock on each source
     * shard and persist the write gate (fenced = TRUE) within the same
     * transaction. The lock is held until releaseWriteBarrier() commits
     * each barrier transaction, preventing any writer admitted under the
     * old ownership from committing after the barrier is established.
     */
    private async acquireWriteBarrier(migrationId: string, units: { unitId: string; sourceShardId: string }[]): Promise<{ client: PoolClient; shardId: string }[]> {
        const sourceShardIds = [...new Set(units.map((u) => u.sourceShardId))];
        const lockedClients: { client: PoolClient; shardId: string }[] = [];

        try {
            for (const shardId of sourceShardIds) {
                const shard = this.manager?.getShard(shardId);
                if (!shard?.pool) continue;

                const client = await shard.pool.connect();
                try {
                    await client.query("BEGIN");

                    const [key1, key2] = fencingLockKey(shardId);
                    await client.query(`SELECT pg_advisory_xact_lock($1, $2)`, [key1, key2]);

                    await client.query(
                        `INSERT INTO resharding_write_gate (id, fenced, migration_id, fenced_at, updated_at)
                         VALUES (1, TRUE, $1, NOW(), NOW())
                         ON CONFLICT (id)
                         DO UPDATE SET fenced = TRUE, migration_id = $1,
                                      fenced_at = NOW(), updated_at = NOW()`,
                        [migrationId],
                    );

                    const result = await client.query(
                        `SELECT fenced FROM resharding_write_gate WHERE id = 1`
                    );
                    if (!result.rows[0]?.fenced) {
                        throw new Error(`Write gate not set for shard "${shardId}"`);
                    }
                } catch (error) {
                    try {
                        await client.query("ROLLBACK").catch(() => {});
                    } finally {
                        client.release();
                    }
                    throw error;
                }
                lockedClients.push({ client, shardId });
            }
        } catch (error) {
            // Release all previously acquired barrier clients so advisory locks
            // are not leaked and gates roll back.
            for (const { client } of [...lockedClients]) {
                try { await client.query("ROLLBACK").catch(() => {}); } finally { client.release(); }
            }
            lockedClients.length = 0;
            throw error;
        }

        return lockedClients;
    }

    /**
     * Release the write barrier by committing all barrier transactions.
     * This releases the exclusive advisory locks, allowing writers to
     * resume (they will observe fenced=TRUE and be rejected until the
     * gate is cleared by finalize() or a new migration).
     */
    private async releaseWriteBarrier(lockedClients: { client: PoolClient; shardId: string }[]): Promise<void> {
        const errors: Error[] = [];
        for (const { client, shardId } of lockedClients) {
            try {
                await client.query("COMMIT");
            } catch (error) {
                errors.push(new Error(`Failed to release write barrier for shard "${shardId}": ${error instanceof Error ? error.message : String(error)}`));
            } finally {
                client.release();
            }
        }
        if (errors.length > 0) {
            throw new Error(`Write barrier release failures: ${errors.map((e) => e.message).join("; ")}`);
        }
    }

    private async applyPendingChanges(migrationId: string, unitId: string, afterSeq: number): Promise<void> {
        // Read the unit to find its destination shard.
        const client = await this.coordination.connect();
        try {
            const result = await client.query(
                `SELECT unit_id, source_shard_id, destination_shard_id, unit_type, status, rows_copied, updated_at
                 FROM resharding_units WHERE migration_id = $1 AND unit_id = $2`,
                [migrationId, unitId],
            );
            if (result.rowCount === 0) return;
            const row = result.rows[0];
            const unit: MigrationUnit = {
                unitId: row.unit_id,
                unitType: row.unit_type as MigrationUnit["unitType"],
                sourceShardId: row.source_shard_id,
                destinationShardId: row.destination_shard_id,
                shardKey: row.shard_key ?? undefined,
                hashBucket: row.hash_bucket ?? undefined,
                rangeStart: row.range_start ?? undefined,
                rangeEnd: row.range_end ?? undefined,
                tableName: undefined,
                estimatedRows: row.estimated_rows ?? null,
                status: row.status as MigrationUnit["status"],
                rowsCopied: Number(row.rows_copied ?? 0),
                updatedAt: row.updated_at,
            };

            await this.changeCapture.drainChanges(
                migrationId,
                unitId,
                Number.MAX_SAFE_INTEGER,
                async (events: ChangeEvent[]) => {
                    await this.copier.applyChangeEvents(migrationId, unit, events);
                },
                afterSeq,
            );
        } finally {
            client.release();
        }
    }

    private async drainToBoundary(migrationId: string, units: { unitId: string; captureTxid?: number }[]): Promise<void> {
        const deadline = Date.now() + this.drainTimeoutMs;
        for (const unit of units) {
            while (Date.now() < deadline) {
                const unapplied = await this.changeCapture.unappliedCountAfterTxid(
                    migrationId,
                    unit.unitId,
                    unit.captureTxid ?? 0,
                );
                if (unapplied === 0) break;
                await this.applyPendingChanges(migrationId, unit.unitId, unit.captureTxid ?? 0);
                await new Promise((r) => setTimeout(r, 100));
            }
            const remaining = await this.changeCapture.unappliedCountAfterTxid(
                migrationId,
                unit.unitId,
                unit.captureTxid ?? 0,
            );
            if (remaining > 0) {
                throw new Error(`Change log not caught up for unit ${unit.unitId}: ${remaining} unapplied changes`);
            }
        }
    }
}
