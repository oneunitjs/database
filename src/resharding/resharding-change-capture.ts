// ---------------------------------------------------------------------------
// ReshardingChangeCapture — transactional change log (outbox) on each shard.
//
// Source mutations and their change records commit atomically in the same
// transaction on the source shard database. `seq` provides stable total
// ordering for change application, preventing stale updates from winning.
// Delivery is at-least-once; consumers must apply idempotently.
//
// Change records are stored SHARD-LOCALLY: each shard database has its own
// `resharding_changes` table. The `getShardPool` callback resolves the
// correct Pool for reading change records from a given source shard.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient } from "pg";
import type { ChangeEvent } from "./resharding-types.js";

export interface ChangeCaptureOptions {
    /** Coordination database pool (for migration metadata, not change records). */
    coordination: Pool;
    /** Resolve a shard's Pool from its shard id. Change records live on shard databases. */
    getShardPool: (shardId: string) => Pool | undefined;
}

export class ReshardingChangeCapture {
    private readonly coordination: Pool;
    private readonly getShardPool: (shardId: string) => Pool | undefined;

    constructor(options: ChangeCaptureOptions) {
        this.coordination = options.coordination;
        this.getShardPool = options.getShardPool;
    }

    /**
     * Record a change event inside a source transaction, atomically with the
     * source mutation it describes. Call within db.transaction()/sharding.transaction().
     * The `client` must belong to the source shard's transaction.
     */
    async recordChange(
        client: PoolClient,
        event: Omit<ChangeEvent, "id" | "sequence" | "createdAt">,
    ): Promise<ChangeEvent> {
        const result = await client.query(
            `INSERT INTO resharding_changes (migration_id, unit_id, table_name, operation, shard_key, old_row, new_row, txid, commit_ts, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, txid_current(), now(), NOW())
             RETURNING id, seq, txid, commit_ts, created_at`,
            [
                event.migrationId,
                event.unitId ?? null,
                event.table,
                event.operation,
                event.shardKey ?? null,
                event.oldRow ? JSON.stringify(event.oldRow) : null,
                event.newRow ? JSON.stringify(event.newRow) : null,
            ],
        );
        const row = result.rows[0];

        // Acquire advisory lock and get commit_order while holding it.
        // Since the lock is held until transaction commit, the order in which
        // transactions acquire this lock IS commit order. nextval() called
        // while holding the lock therefore reflects commit order.
        // This guarantees that replay ordered by commit_order ASC produces
        // the same result as applying changes in source commit order.
        const commitOrderResult = await client.query(
            `SELECT pg_advisory_xact_lock($1), nextval('resharding_changes_commit_order_seq') AS commit_order`,
            [0x72657363], // "resc" — resharding commit order namespace
        );
        const commitOrder = Number(commitOrderResult.rows[0].commit_order);

        // Update the change record with its commit order.
        await client.query(
            `UPDATE resharding_changes SET commit_order = $1 WHERE id = $2`,
            [commitOrder, row.id],
        );

        return {
            id: String(row.id),
            sequence: Number(row.seq),
            migrationId: event.migrationId,
            unitId: event.unitId ?? null,
            table: event.table,
            operation: event.operation,
            shardKey: event.shardKey ?? null,
            oldRow: event.oldRow,
            newRow: event.newRow,
            createdAt: row.created_at,
            appliedAt: undefined,
            commitTs: row.commit_ts ? new Date(row.commit_ts) : undefined,
            sourceTxId: Number(row.txid),
            commitOrder,
        };
    }

    /**
     * Resolve the source shard pool for a migration unit by looking up the
     * unit's source_shard_id from the coordination database's resharding_units table.
     */
    private async resolveUnitShardPool(
        migrationId: string,
        unitId: string,
    ): Promise<{ pool: Pool; sourceShardId: string } | undefined> {
        const client = await this.coordination.connect();
        try {
            const result = await client.query(
                `SELECT source_shard_id FROM resharding_units WHERE migration_id = $1 AND unit_id = $2`,
                [migrationId, unitId],
            );
            if (result.rowCount === 0) return undefined;
            const sourceShardId = result.rows[0].source_shard_id;
            const pool = this.getShardPool(sourceShardId);
            if (!pool) return undefined;
            return { pool, sourceShardId };
        } finally {
            client.release();
        }
    }

    /**
     * Drain and apply pending change events for a unit with txid >= afterTxid.
     * Applies in txid ASC (commit order), seq ASC as tiebreaker.
     * Idempotent via applied_at marking.
     * Returns the number of changes applied in this call.
     *
     * txid is used instead of seq because seq is allocated at INSERT time,
     * not commit time. txid is allocated at transaction START and is
     * commit-approximate: a transaction with a lower txid may commit after
     * one with a higher txid, but txid ordering is strictly better than seq
     * ordering for preventing stale updates from overwriting newer data.
     *
     * Change records are read from the source shard's database (where
     * recordChange() wrote them), not from the coordination database.
     */
    /**
     * Drain and apply pending change events for a unit with txid >= afterTxid.
     * Applies in txid ASC (commit order), seq ASC as tiebreaker.
     * Idempotent via applied_at marking.
     * Returns the number of changes applied in this call.
     *
     * txid is used instead of seq because seq is allocated at INSERT time,
     * not commit time. txid is allocated at transaction START and is
     * commit-approximate: a transaction with a lower txid may commit after
     * one with a higher txid, but txid ordering is strictly better than seq
     * ordering for preventing stale updates from overwriting newer data.
     *
     * Change records are read from the source shard's database (where
     * recordChange() wrote them), not from the coordination database.
     */
    /**
     * Drain and apply pending change events for a unit with txid >= afterTxid.
     * Applies in txid ASC (commit order), seq ASC as tiebreaker.
     * Idempotent via applied_at marking.
     * Returns the number of changes applied in this call.
     *
     * txid is used instead of seq because seq is allocated at INSERT time,
     * not commit time. txid is allocated at transaction START and is
     * commit-approximate: a transaction with a lower txid may commit after
     * one with a higher txid, but txid ordering is strictly better than seq
     * ordering for preventing stale updates from overwriting newer data.
     *
     * Change records are read from the source shard's database (where
     * recordChange() wrote them), not from the coordination database.
     *
     * @param shardId - Optional source shard ID. If provided, skips the
     *   coordination DB lookup for the unit's source shard. Useful for tests
     *   or when the caller already knows the shard.
     */
    async drainChanges(
        migrationId: string,
        unitId: string,
        uptoSequence: number,
        apply: (events: ChangeEvent[]) => Promise<void>,
        afterTxid: number = 0,
        shardId?: string,
    ): Promise<{ applied: number }> {
        let pool: Pool | undefined;

        if (shardId) {
            pool = this.getShardPool(shardId);
        } else {
            const info = await this.resolveUnitShardPool(migrationId, unitId);
            if (!info) return { applied: 0 };
            pool = info.pool;
        }

        if (!pool) return { applied: 0 };

        const client = await pool.connect();
        let applied = 0;
        try {
            const rows = await client.query(
                `SELECT id, seq, txid, commit_ts, commit_order, migration_id, unit_id, table_name, operation, shard_key, old_row, new_row, created_at
                 FROM resharding_changes
                 WHERE migration_id = $1 AND unit_id = $2 AND txid >= $4 AND seq <= $3 AND applied_at IS NULL
                 ORDER BY commit_order ASC NULLS LAST, seq ASC`,
                [migrationId, unitId, uptoSequence, afterTxid],
            );
            // Collect all events first, then apply as a batch for transaction grouping
            const events: ChangeEvent[] = [];
            for (const row of rows.rows) {
                events.push({
                    id: String(row.id),
                    sequence: Number(row.seq),
                    migrationId: row.migration_id,
                    unitId: row.unit_id,
                    table: row.table_name,
                    operation: row.operation,
                    shardKey: row.shard_key,
                    oldRow: row.old_row ?? null,
                    newRow: row.new_row ?? null,
                    createdAt: row.created_at,
                    appliedAt: undefined,
                    commitTs: row.commit_ts ? new Date(row.commit_ts) : undefined,
                    sourceTxId: Number(row.txid),
                    commitOrder: row.commit_order ? Number(row.commit_order) : 0,
                });
            }
            if (events.length > 0) {
                await apply(events);
                // Mark all as applied in a single update
                const ids = events.map((e) => Number(e.id));
                await client.query(
                    `UPDATE resharding_changes SET applied_at = NOW() WHERE id = ANY($1)`,
                    [ids],
                );
                applied = events.length;
            }
        } finally {
            client.release();
        }
        return { applied };
    }

    async unappliedCount(migrationId: string, unitId?: string): Promise<number> {
        // Without a unitId, we cannot know which shard to query. Return 0
        // and let callers use the unit-specific variant.
        if (!unitId) return 0;

        const shardInfo = await this.resolveUnitShardPool(migrationId, unitId);
        if (!shardInfo) return 0;

        const client = await shardInfo.pool.connect();
        try {
            const row = await client.query(
                `SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1 AND unit_id = $2 AND applied_at IS NULL`,
                [migrationId, unitId],
            );
            return Number(row.rows[0].c);
        } finally {
            client.release();
        }
    }

    /**
     * Count unapplied changes with txid >= afterTxid for a unit. Used during
     * catching_up to replay all changes that occurred at or after the
     * snapshot capture boundary.
     *
     * @param shardId - Optional source shard ID. If provided, skips the
     *   coordination DB lookup for the unit's source shard.
     */
    async unappliedCountAfterTxid(
        migrationId: string,
        unitId: string,
        afterTxid: number,
        shardId?: string,
    ): Promise<number> {
        let pool: Pool | undefined;

        if (shardId) {
            pool = this.getShardPool(shardId);
        } else {
            const info = await this.resolveUnitShardPool(migrationId, unitId);
            if (!info) return 0;
            pool = info.pool;
        }

        if (!pool) return 0;

        const client = await pool.connect();
        try {
            const row = await client.query(
                `SELECT COUNT(*) AS c FROM resharding_changes
                 WHERE migration_id = $1 AND unit_id = $2 AND txid >= $3 AND applied_at IS NULL`,
                [migrationId, unitId, afterTxid],
            );
            return Number(row.rows[0].c);
        } finally {
            client.release();
        }
    }
}
