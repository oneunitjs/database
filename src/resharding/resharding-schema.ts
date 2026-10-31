// ---------------------------------------------------------------------------
// Persistent resharding schema and versioned initialization
//
// Cross-shard transactions are NOT atomic. The migration control plane
// (lifecycle state, routing versions, units) lives on a designated
// coordination database. Per-shard databases own their change log
// (transactional outbox) and batch checkpoints, written in the same
// transaction as the source mutations they describe.
// ---------------------------------------------------------------------------

import type { Pool, PoolClient, PoolConfig } from "pg";

export const RESHARDING_SCHEMA_VERSION = 3;

const COORDINATION_DDL = [
    // Schema version tracking (idempotent initialization)
    `
    CREATE TABLE IF NOT EXISTS resharding_schema_version (
        id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        version INTEGER NOT NULL,
        initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    // Migration lifecycle records (single coordination authority)
    `
    CREATE TABLE IF NOT EXISTS resharding_migrations (
        migration_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        source_routing_version INTEGER NOT NULL,
        target_routing_version INTEGER NOT NULL,
        source_topology JSONB NOT NULL,
        target_topology JSONB NOT NULL,
        strategy TEXT NOT NULL,
        strategy_config JSONB,
        plan JSONB,
        units_total INTEGER NOT NULL DEFAULT 0,
        units_completed INTEGER NOT NULL DEFAULT 0,
        units_failed INTEGER NOT NULL DEFAULT 0,
        rows_copied BIGINT NOT NULL DEFAULT 0,
        rows_validated BIGINT NOT NULL DEFAULT 0,
        change_log_lag BIGINT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        finalized_at TIMESTAMPTZ,
        paused_at TIMESTAMPTZ,
        error_code TEXT,
        error_message TEXT,
        cutover_intent BOOLEAN NOT NULL DEFAULT FALSE,
        cutover_committed BOOLEAN NOT NULL DEFAULT FALSE,
        worker_id TEXT,
        worker_locked_at TIMESTAMPTZ,
        capture_seq BIGINT,
        fenced_at TIMESTAMPTZ,
        CONSTRAINT resharding_migrations_status_check
            CHECK (status IN (
                'planned','copying','catching_up','validating',
                'ready_for_cutover','cutting_over','completed',
                'paused','failed','cancelled','rollback_required','finalized'
            ))
    )
    `,
    // Per-unit progress within a migration
    `
    CREATE TABLE IF NOT EXISTS resharding_units (
        migration_id TEXT NOT NULL REFERENCES resharding_migrations(migration_id) ON DELETE CASCADE,
        unit_id TEXT NOT NULL,
        unit_type TEXT NOT NULL,
        source_shard_id TEXT NOT NULL,
        destination_shard_id TEXT NOT NULL,
        shard_key TEXT,
        hash_bucket INTEGER,
        range_start TEXT,
        range_end TEXT,
        estimated_rows INTEGER,
        status TEXT NOT NULL DEFAULT 'planned',
        rows_copied BIGINT NOT NULL DEFAULT 0,
        last_checkpoint JSONB,
        capture_txid BIGINT,
        capture_xmax BIGINT,
        capture_xip JSONB,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (migration_id, unit_id),
        CONSTRAINT resharding_units_status_check
            CHECK (status IN ('planned','copying','caught_up','validated','cut_over','completed','failed'))
    )
    `,
    // Published routing versions — cutover commit protocol.
    // The row with is_current = true is the active routing version.
    // Application logic publishes one current version at a time.
    `
    CREATE TABLE IF NOT EXISTS resharding_routing_versions (
        routing_version INTEGER NOT NULL,
        migration_id TEXT REFERENCES resharding_migrations(migration_id) ON DELETE SET NULL,
        committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        is_current BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (routing_version)
    )
    `,
    // Indexes
    `CREATE INDEX IF NOT EXISTS idx_resharding_migrations_status ON resharding_migrations(status)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_migrations_updated_at ON resharding_migrations(updated_at)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_units_migration ON resharding_units(migration_id, status)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_routing_current ON resharding_routing_versions(is_current) WHERE is_current`,
];

const SHARD_DDL = [
    // Per-shard batch checkpoints (advanced only after destination commit)
    `
    CREATE TABLE IF NOT EXISTS resharding_checkpoints (
        migration_id TEXT NOT NULL,
        unit_id TEXT NOT NULL,
        shard_id TEXT NOT NULL,
        batch_no BIGINT NOT NULL,
        cursor JSONB NOT NULL,
        rows_copied BIGINT NOT NULL DEFAULT 0,
        destination_committed BOOLEAN NOT NULL DEFAULT FALSE,
        committed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (migration_id, unit_id, shard_id)
    )
    `,
    // Transactional change log (outbox) on each shard.
    // Source mutations and change records commit in the same transaction.
    // `seq` provides a unique insert-time identifier but does NOT guarantee
    // commit order (PostgreSQL sequences are allocated at statement time).
    // `txid` records the transaction ID at insert time (transaction start,
    // not commit). `commit_order` is assigned at COMMIT time using an
    // advisory lock, guaranteeing that commit_order reflects actual commit
    // order: replaying events ordered by commit_order ASC produces the same
    // result as applying them in commit order on the source.
    //
    // Replay ORDER BY commit_order ASC, seq ASC guarantees that:
    //   1. Transactions are applied in commit order
    //   2. Within a single transaction, events are applied in insert order
    //   3. Stale updates cannot overwrite newer data (conflict resolution
    //      using sourceTxId/commit_order in the adapter)
    `
    CREATE SEQUENCE IF NOT EXISTS resharding_changes_seq;
    CREATE SEQUENCE IF NOT EXISTS resharding_changes_commit_order_seq;
    CREATE TABLE IF NOT EXISTS resharding_changes (
        id BIGSERIAL PRIMARY KEY,
        seq BIGINT NOT NULL UNIQUE DEFAULT nextval('resharding_changes_seq'),
        commit_order BIGINT,
        txid BIGINT NOT NULL,
        commit_ts TIMESTAMPTZ,
        migration_id TEXT NOT NULL,
        unit_id TEXT,
        table_name TEXT NOT NULL,
        operation TEXT NOT NULL,
        shard_key TEXT,
        old_row JSONB,
        new_row JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        applied_at TIMESTAMPTZ,
        CONSTRAINT resharding_changes_operation_check
            CHECK (operation IN ('insert','update','delete'))
    )
    `,
    `CREATE INDEX IF NOT EXISTS idx_resharding_changes_seq ON resharding_changes(seq)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_changes_txid ON resharding_changes(txid)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_changes_commit_order ON resharding_changes(commit_order)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_changes_migration_unit ON resharding_changes(migration_id, unit_id, seq)`,
    `CREATE INDEX IF NOT EXISTS idx_resharding_changes_unapplied ON resharding_changes(migration_id, unit_id, seq) WHERE applied_at IS NULL`,
    // Write fencing gate — enforced at each shard database. Cutover sets
    // fenced = TRUE on source shards to block stale writers after ownership
    // transfers. The write path checks this gate before executing any write.
    `
    CREATE TABLE IF NOT EXISTS resharding_write_gate (
        id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        fenced BOOLEAN NOT NULL DEFAULT FALSE,
        migration_id TEXT,
        fenced_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS idx_resharding_write_gate_fenced ON resharding_write_gate(fenced) WHERE fenced`,
    // Distributed shard drain state — persisted across application instances.
    // removeShard() writes draining state here so other instances observe it
    // and stop routing new operations to the shard.
    `
    CREATE TABLE IF NOT EXISTS shard_drain_state (
        shard_id TEXT PRIMARY KEY,
        draining BOOLEAN NOT NULL DEFAULT FALSE,
        drain_version INTEGER NOT NULL DEFAULT 0,
        drain_started_at TIMESTAMPTZ,
        instance_id TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    // Distributed active operation registry — tracks in-flight operations
    // per shard across all application instances. removeShard() waits for
    // this table to be empty for the target shard before closing the pool.
    // Rows are heartbeat-refreshed; stale rows (older than the TTL) are
    // treated as abandoned and ignored.
    `
    CREATE TABLE IF NOT EXISTS shard_active_ops (
        shard_id TEXT NOT NULL,
        instance_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (shard_id, operation_id)
    )
    `,
    `CREATE INDEX IF NOT EXISTS idx_shard_active_ops_shard ON shard_active_ops(shard_id)`,
    `CREATE INDEX IF NOT EXISTS idx_shard_active_ops_heartbeat ON shard_active_ops(heartbeat_at)`,
];

export interface ReshardingSchema {
    initCoordination(client: PoolClient): Promise<void>;
    initShard(client: PoolClient, shardId: string): Promise<void>;
    schemaVersion(): number;
}

export function createReshardingSchema(): ReshardingSchema {
    return {
        schemaVersion: () => RESHARDING_SCHEMA_VERSION,
        async initCoordination(client: PoolClient): Promise<void> {
            // Advisory lock makes initialization idempotent across processes.
            await client.query(`SELECT pg_advisory_lock($1)`, [0x72657368]); // "resh"
            try {
                // Create all tables first (idempotent) so the version table exists.
                for (const ddl of COORDINATION_DDL) {
                    await client.query(ddl);
                }
                // Add new columns for existing databases (idempotent).
                await client.query(
                    `ALTER TABLE resharding_changes ADD COLUMN IF NOT EXISTS txid BIGINT`,
                );
                await client.query(
                    `ALTER TABLE resharding_changes ADD COLUMN IF NOT EXISTS commit_ts TIMESTAMPTZ`,
                );
                await client.query(
                    `ALTER TABLE resharding_changes ADD COLUMN IF NOT EXISTS commit_order BIGINT`,
                );
                await client.query(
                    `CREATE SEQUENCE IF NOT EXISTS resharding_changes_commit_order_seq`,
                );
                await client.query(
                    `ALTER TABLE resharding_migrations ADD COLUMN IF NOT EXISTS capture_seq BIGINT`,
                );
                await client.query(
                    `ALTER TABLE resharding_migrations ADD COLUMN IF NOT EXISTS fenced_at TIMESTAMPTZ`,
                );
        await client.query(
            `ALTER TABLE resharding_units ADD COLUMN IF NOT EXISTS capture_txid BIGINT`,
        );
        await client.query(
            `ALTER TABLE resharding_units ADD COLUMN IF NOT EXISTS capture_xmax BIGINT`,
        );
        await client.query(
            `ALTER TABLE resharding_units ADD COLUMN IF NOT EXISTS capture_xip JSONB`,
        );
                await client.query(
                    `CREATE INDEX IF NOT EXISTS idx_resharding_changes_txid ON resharding_changes(txid)`,
                );
                // Record/validate schema version.
                const existing = await client.query(
                    `SELECT version FROM resharding_schema_version WHERE id = 1`,
                );
                if (existing.rowCount === 0) {
                    await client.query(
                        `INSERT INTO resharding_schema_version (id, version) VALUES (1, $1)`,
                        [RESHARDING_SCHEMA_VERSION],
                    );
    } else if (Number(existing.rows[0].version) < RESHARDING_SCHEMA_VERSION) {
        await client.query(
            `UPDATE resharding_schema_version SET version = $1, initialized_at = NOW() WHERE id = 1`,
            [RESHARDING_SCHEMA_VERSION],
        );
    } else if (Number(existing.rows[0].version) !== RESHARDING_SCHEMA_VERSION) {
        throw new Error(
            `Resharding schema version mismatch: found ${existing.rows[0].version}, expected ${RESHARDING_SCHEMA_VERSION}`,
        );
    }
            } finally {
                await client.query(`SELECT pg_advisory_unlock($1)`, [0x72657368]);
            }
        },
        async initShard(client: PoolClient, _shardId: string): Promise<void> {
            await client.query(`SELECT pg_advisory_lock($1)`, [0x73687264]); // "shard"
            try {
                for (const ddl of SHARD_DDL) {
                    await client.query(ddl);
                }
            } finally {
                await client.query(`SELECT pg_advisory_unlock($1)`, [0x73687264]);
            }
        },
    };
}

export function isPool(value: Pool | PoolConfig): value is Pool {
    return (value as Pool).connect !== undefined && (value as Pool).end !== undefined;
}

/**
 * Construct a PostgreSQL advisory lock key for shard write fencing.
 *
 * Returns [key1, key2] for use with pg_advisory_xact_lock(int4, int4).
 * The first int32 is a fixed namespace (0x7772696e = "wrin" for write fencing).
 * The second int32 is a Jenkins-style hash of the shard id.
 *
 * Using transaction-level advisory locks (pg_advisory_xact_lock_*) ensures the
 * lock is automatically released when the transaction commits or rolls back,
 * even if the application crashes or the connection drops. This is critical
 * for write fencing: a stale writer cannot hold the fence past its transaction.
 *
 * Lock scope is per-shard: writers on the same shard contend (shared vs
 * exclusive), writers on independent shards do not block each other.
 */
export function fencingLockKey(shardId: string): [number, number] {
    const namespace = 0x7772696e; // "wrin" — write fencing namespace
    let hash = 0;
    for (let i = 0; i < shardId.length; i++) {
        hash = ((hash << 5) - hash + shardId.charCodeAt(i)) | 0;
    }
    // Ensure hash is a valid int32 (it already is due to | 0 above)
    return [namespace, hash];
}
