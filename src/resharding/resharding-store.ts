// ---------------------------------------------------------------------------
// ReshardingStore — coordination-database persistence for migration lifecycle.
//
// Single coordination authority owns: migration rows, units, routing versions.
// Cross-shard transactions are NOT atomic; this store never spans shards.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient } from "pg";
import type {
    CheckpointCursor,
    MigrationDescriptor,
    ProgressEvent,
    ReshardingPlan,
    TopologySnapshot,
} from "./resharding-types.js";
import type { MigrationUnit } from "./resharding-types.js";
import type { Logger } from "../types.js";
import { InvalidStateTransitionError, MigrationLockedError, MigrationNotFoundError } from "./resharding-errors.js";

const LEGAL_TRANSITIONS: Record<string, Set<string>> = {
    planned: new Set(["copying", "paused", "cancelled", "failed"]),
    copying: new Set(["catching_up", "paused", "failed", "cancelled"]),
    catching_up: new Set(["validating", "paused", "failed", "cancelled"]),
    validating: new Set(["ready_for_cutover", "paused", "failed", "cancelled", "rollback_required"]),
    ready_for_cutover: new Set(["cutting_over", "paused", "failed", "cancelled", "rollback_required"]),
    cutting_over: new Set(["completed", "failed", "rollback_required"]),
    completed: new Set(["finalized"]),
    paused: new Set(["copying", "catching_up", "validating", "cancelled", "failed"]),
    failed: new Set(["cancelled", "rollback_required", "planned"]),
    rollback_required: new Set(["cancelled", "planned"]),
    cancelled: new Set([]),
    finalized: new Set([]),
};

export interface MigrationRow {
    migrationId: string;
    status: string;
    version: number;
    sourceRoutingVersion: number;
    targetRoutingVersion: number;
    sourceTopology: TopologySnapshot;
    targetTopology: TopologySnapshot;
    strategy: string;
    strategyConfig: unknown;
    plan: ReshardingPlan;
    unitsTotal: number;
    unitsCompleted: number;
    unitsFailed: number;
    rowsCopied: number;
    rowsValidated: number;
    changeLogLag: number;
    createdAt: string;
    updatedAt: string;
    startedAt?: string;
    completedAt?: string;
    finalizedAt?: string;
    pausedAt?: string;
    errorCode?: string;
    errorMessage?: string;
    cutoverIntent: boolean;
    cutoverCommitted: boolean;
    workerId?: string;
    workerLockedAt?: string;
    captureSeq: number;
    fencedAt?: string;
    units: MigrationUnit[];
}

export class ReshardingStore {
    private readonly coordination: Pool;
    private readonly logger?: Logger;

    constructor(coordination: Pool, logger?: Logger, _hooks?: { onProgress?: (event: ProgressEvent) => void }) {
        this.coordination = coordination;
        this.logger = logger;
    }

    async getMigration(migrationId: string): Promise<MigrationRow | undefined> {
        const client = await this.coordination.connect();
        try {
            const result = await client.query(
                `SELECT migration_id, status, version, source_routing_version, target_routing_version,
                        source_topology, target_topology, strategy, strategy_config, plan,
                        units_total, units_completed, units_failed, rows_copied, rows_validated,
                        change_log_lag, created_at, updated_at, started_at, completed_at,
                        finalized_at, paused_at, error_code, error_message,
                        cutover_intent, cutover_committed, worker_id, worker_locked_at
                 FROM resharding_migrations WHERE migration_id = $1`,
                [migrationId],
            );
            if (result.rowCount === 0) return undefined;
            const row = this.rowToMigration(result.rows[0]);
            // Refresh unit statuses from the live units table (the plan snapshot
            // is only the initial intent; units are updated as copy progresses).
             const unitsResult = await client.query(
                `SELECT unit_id, unit_type, source_shard_id, destination_shard_id, shard_key,
                        hash_bucket, range_start, range_end, estimated_rows, status, rows_copied,
                        last_checkpoint, capture_txid, capture_xmax, capture_xip, updated_at
                 FROM resharding_units WHERE migration_id = $1 ORDER BY unit_id`,
                [migrationId],
             );
            if ((unitsResult.rowCount ?? 0) > 0) {
                row.units = unitsResult.rows.map((r) => ({
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
                    captureTxid: r.capture_txid != null ? Number(r.capture_txid) : undefined,
                    captureXmax: r.capture_xmax != null ? Number(r.capture_xmax) : undefined,
                    captureXip: Array.isArray(r.capture_xip) ? r.capture_xip.map(Number) : undefined,
                    updatedAt: r.updated_at,
                }));
            }
            return row;
        } finally {
            client.release();
        }
    }

    async requireMigration(migrationId: string): Promise<MigrationRow> {
        const row = await this.getMigration(migrationId);
        if (!row) throw new MigrationNotFoundError(migrationId);
        return row;
    }

    async insertMigration(plan: ReshardingPlan, descriptor: MigrationDescriptor): Promise<void> {
        const client = await this.coordination.connect();
        try {
            await client.query("BEGIN");
            try {
                await client.query(
                    `INSERT INTO resharding_migrations (
                        migration_id, status, version, source_routing_version, target_routing_version,
                        source_topology, target_topology, strategy, strategy_config, plan,
                        units_total, units_completed, units_failed, rows_copied, rows_validated,
                        change_log_lag, created_at, updated_at, cutover_intent, cutover_committed
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
                    [
                        plan.migrationId,
                        "planned",
                        1,
                        plan.sourceRoutingVersion,
                        plan.targetRoutingVersion,
                        JSON.stringify(plan.sourceTopology),
                        JSON.stringify(plan.targetTopology),
                        plan.strategy,
                        plan.strategyConfig ? JSON.stringify(plan.strategyConfig) : null,
                        JSON.stringify(plan),
                        plan.units.length,
                        0,
                        0,
                        0,
                        0,
                        0,
                        plan.createdAt,
                        plan.createdAt,
                        false,
                        false,
                    ],
                );
                for (const unit of plan.units) {
                    await client.query(
                        `INSERT INTO resharding_units (migration_id, unit_id, unit_type, source_shard_id, destination_shard_id,
                                                        shard_key, hash_bucket, range_start, range_end, estimated_rows, status,
                                                        rows_copied, last_checkpoint, updated_at)
                         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
                        [
                            plan.migrationId,
                            unit.unitId,
                            unit.unitType,
                            unit.sourceShardId,
                            unit.destinationShardId,
                            unit.shardKey ?? null,
                            unit.hashBucket ?? null,
                            unit.rangeStart ?? null,
                            unit.rangeEnd ?? null,
                            unit.estimatedRows ?? null,
                            "planned",
                            0,
                            unit.lastCheckpoint ? JSON.stringify(unit.lastCheckpoint) : null,
                            unit.updatedAt,
                        ],
                    );
                }
                await client.query("COMMIT");
            } catch (_err) {
                await client.query("ROLLBACK");
                throw _err;
            }
        } finally {
            client.release();
        }
    }

    async claimWorkerLock(migrationId: string): Promise<void> {
        const client = await this.coordination.connect();
        try {
            const lockKey = 0x72657368 + (migrationId.length % 1000);
            await client.query(`SELECT pg_advisory_lock($1)`, [lockKey]);
            await client.query(
                `UPDATE resharding_migrations SET worker_id = $1, worker_locked_at = NOW() WHERE migration_id = $2`,
                [`worker-${lockKey}`, migrationId],
            );
        } finally {
            client.release();
        }
    }

    async releaseWorkerLock(migrationId: string): Promise<void> {
        const client = await this.coordination.connect();
        try {
            const lockKey = 0x72657368 + (migrationId.length % 1000);
            await client.query(`SELECT pg_advisory_unlock($1)`, [lockKey]).catch(() => {});
            await client.query(
                `UPDATE resharding_migrations SET worker_id = NULL, worker_locked_at = NULL WHERE migration_id = $1`,
                [migrationId],
            ).catch(() => {});
        } finally {
            client.release();
        }
    }

    async transition(
        migrationId: string,
        from: string,
        to: string,
        expectedVersion: number,
    ): Promise<void> {
        if (!LEGAL_TRANSITIONS[from]?.has(to)) {
            throw new InvalidStateTransitionError(migrationId, from, to);
        }

        const client = await this.coordination.connect();
        try {
            await client.query("BEGIN");
            try {
                const result = await client.query(
                    `UPDATE resharding_migrations
                     SET status = $2, version = version + 1, updated_at = NOW()
                     WHERE migration_id = $1 AND status = $3 AND version = $4
                     RETURNING version`,
                    [migrationId, to, from, expectedVersion],
                );
                if (result.rowCount === 0) {
                    const current = await this.getMigration(migrationId);
                    if (!current) throw new MigrationNotFoundError(migrationId);
                    throw new MigrationLockedError(migrationId, current.workerId);
                }
                await client.query("COMMIT");
            } catch (_err) {
                await client.query("ROLLBACK");
                throw _err;
            }
        } finally {
            client.release();
        }
    }

    async updateProgress(migrationId: string, patch: {
        rowsCopied?: number;
        rowsValidated?: number;
        changeLogLag?: number;
        unitsCompleted?: number;
        unitsFailed?: number;
    }): Promise<void> {
        const client = await this.coordination.connect();
        try {
            const sets: string[] = ["updated_at = NOW()"];
            const params: unknown[] = [migrationId];
            let p = 2;
            if (patch.rowsCopied !== undefined) { sets.push(`rows_copied = $${p}`); params.push(patch.rowsCopied); p++; }
            if (patch.rowsValidated !== undefined) { sets.push(`rows_validated = $${p}`); params.push(patch.rowsValidated); p++; }
            if (patch.changeLogLag !== undefined) { sets.push(`change_log_lag = $${p}`); params.push(patch.changeLogLag); p++; }
            if (patch.unitsCompleted !== undefined) { sets.push(`units_completed = $${p}`); params.push(patch.unitsCompleted); p++; }
            if (patch.unitsFailed !== undefined) { sets.push(`units_failed = $${p}`); params.push(patch.unitsFailed); p++; }
            await client.query(
                `UPDATE resharding_migrations SET ${sets.join(", ")} WHERE migration_id = $1`,
                params,
            );
        } finally {
            client.release();
        }
    }

    async updateCaptureSeq(migrationId: string, captureSeq: number): Promise<void> {
        const client = await this.coordination.connect();
        try {
            await client.query(
                `UPDATE resharding_migrations SET capture_seq = $2, updated_at = NOW() WHERE migration_id = $1`,
                [migrationId, captureSeq],
            );
        } finally {
            client.release();
        }
    }

    /**
     * Record the per-unit capture transaction snapshot boundary (xmin from
     * txid_current_snapshot()). All changes with txid >= capture_txid must be
     * replayed during catching_up; changes with txid < capture_txid are
     * covered by the initial snapshot copy.
     */
    async updateUnitCaptureTxid(migrationId: string, unitId: string, captureTxid: number): Promise<void> {
        await this.updateUnitCaptureSnapshot(migrationId, unitId, captureTxid);
    }

    async updateUnitCaptureSnapshot(
        migrationId: string,
        unitId: string,
        captureTxid: number,
        captureXmax?: number,
        captureXip?: number[],
    ): Promise<void> {
        const client = await this.coordination.connect();
        try {
            await client.query(
                `UPDATE resharding_units SET capture_txid = $3, capture_xmax = COALESCE($4, capture_xmax), capture_xip = COALESCE($5, capture_xip), updated_at = NOW() WHERE migration_id = $1 AND unit_id = $2`,
                [migrationId, unitId, captureTxid, captureXmax ?? null, captureXip ? JSON.stringify(captureXip) : null],
            );
        } finally {
            client.release();
        }
    }

    async setCutoverIntent(migrationId: string): Promise<void> {
        const client = await this.coordination.connect();
        try {
            await client.query(
                `UPDATE resharding_migrations SET cutover_intent = TRUE, fenced_at = NOW(), updated_at = NOW() WHERE migration_id = $1`,
                [migrationId],
            );
        } finally {
            client.release();
        }
    }

    /**
     * Set the write fencing gate on a shard database. When fenced = TRUE,
     * the shard's write path must reject writes for migrated units.
     * This is enforced shard-locally; it does not require coordination-DB
     * participation at write time.
     */
    async setWriteGate(shardPool: Pool, migrationId: string, fenced: boolean): Promise<void> {
        const client = await shardPool.connect();
        try {
            await client.query("BEGIN");
            try {
                await client.query(
                    `INSERT INTO resharding_write_gate (id, fenced, migration_id, fenced_at, updated_at)
                     VALUES (1, $1, $2, $3, NOW())
                     ON CONFLICT (id)
                     DO UPDATE SET fenced = $1, migration_id = $2,
                                  fenced_at = $3, updated_at = NOW()`,
                    [fenced, migrationId, fenced ? new Date().toISOString() : null],
                );
                await client.query("COMMIT");
            } catch (_err) {
                await client.query("ROLLBACK");
                throw _err;
            }
        } finally {
            client.release();
        }
    }

    /**
     * Check whether the write gate is fenced on a shard database.
     * Returns true if writes should be rejected.
     *
     * All errors are propagated to the caller — the caller decides whether
     * to fail closed or fail open based on whether a migration is active.
     */
    async isWriteGateFenced(shardPool: Pool): Promise<{ fenced: boolean; migrationId?: string }> {
        const client = await shardPool.connect();
        try {
            const result = await client.query(
                `SELECT fenced, migration_id FROM resharding_write_gate WHERE id = 1`,
            );
            if (result.rowCount === 0) return { fenced: false };
            return {
                fenced: Boolean(result.rows[0].fenced),
                migrationId: result.rows[0].migration_id ?? undefined,
            };
        } catch (error) {
            throw error;
        } finally {
            client.release();
        }
    }

    async updateUnitCheckpoint(
        migrationId: string,
        unitId: string,
        patch: { status?: string; rowsCopied?: number; lastCheckpoint?: unknown },
    ): Promise<void> {
        const client = await this.coordination.connect();
        try {
            const sets: string[] = ["updated_at = NOW()"];
            const params: unknown[] = [migrationId, unitId];
            let p = 3;
            if (patch.status !== undefined) { sets.push(`status = $${p}`); params.push(patch.status); p++; }
            if (patch.rowsCopied !== undefined) { sets.push(`rows_copied = $${p}`); params.push(patch.rowsCopied); p++; }
            if (patch.lastCheckpoint !== undefined) { sets.push(`last_checkpoint = $${p}`); params.push(JSON.stringify(patch.lastCheckpoint)); p++; }
            await client.query(
                `UPDATE resharding_units SET ${sets.join(", ")} WHERE migration_id = $1 AND unit_id = $2`,
                params,
            );
        } finally {
            client.release();
        }
    }

    async currentRoutingVersion(): Promise<number> {
        const client = await this.coordination.connect();
        try {
            const row = await client.query(
                `SELECT routing_version FROM resharding_routing_versions WHERE is_current = TRUE LIMIT 1`,
            );
            if (row.rowCount === 0) return 0;
            return Number(row.rows[0].routing_version);
        } finally {
            client.release();
        }
    }

    async publishRoutingVersion(routingVersion: number, migrationId: string): Promise<void> {
        const client = await this.coordination.connect();
        try {
            await client.query("BEGIN");
            try {
                // Retire previous current version.
                await client.query(`UPDATE resharding_routing_versions SET is_current = FALSE WHERE is_current = TRUE`);
                await client.query(
                    `INSERT INTO resharding_routing_versions (routing_version, migration_id, committed_at, is_current)
                     VALUES ($1, $2, NOW(), TRUE)
                     ON CONFLICT (routing_version)
                     DO UPDATE SET migration_id = $2, committed_at = NOW(), is_current = TRUE`,
                    [routingVersion, migrationId],
                );
                await client.query("COMMIT");
            } catch (_err) {
                await client.query("ROLLBACK");
                throw _err;
            }
        } finally {
            client.release();
        }
    }

    private rowToMigration(row: any): MigrationRow {
        return {
            migrationId: row.migration_id,
            status: row.status,
            version: Number(row.version),
            sourceRoutingVersion: Number(row.source_routing_version),
            targetRoutingVersion: Number(row.target_routing_version),
            sourceTopology: row.source_topology,
            targetTopology: row.target_topology,
            strategy: row.strategy,
            strategyConfig: row.strategy_config,
            plan: row.plan,
            unitsTotal: Number(row.units_total ?? 0),
            unitsCompleted: Number(row.units_completed ?? 0),
            unitsFailed: Number(row.units_failed ?? 0),
            rowsCopied: Number(row.rows_copied ?? 0),
            rowsValidated: Number(row.rows_validated ?? 0),
            changeLogLag: Number(row.change_log_lag ?? 0),
            createdAt: row.created_at,
            updatedAt: row.updated_at,
            startedAt: row.started_at,
            completedAt: row.completed_at,
            finalizedAt: row.finalized_at,
            pausedAt: row.paused_at,
            errorCode: row.error_code,
            errorMessage: row.error_message,
            cutoverIntent: Boolean(row.cutover_intent),
            cutoverCommitted: Boolean(row.cutover_committed),
            workerId: row.worker_id,
            workerLockedAt: row.worker_locked_at,
            captureSeq: Number(row.capture_seq ?? 0),
            fencedAt: row.fenced_at,
            units: row.plan?.units ?? [],
        };
    }
}
