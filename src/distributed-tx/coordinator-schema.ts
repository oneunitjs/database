// ---------------------------------------------------------------------------
// Distributed Transaction Coordinator Schema
// ---------------------------------------------------------------------------

export const DISTRIBUTED_TX_SCHEMA = [
    `
    CREATE TABLE IF NOT EXISTS distributed_transactions (
        transaction_id TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        coordinator_id TEXT NOT NULL,
        participants TEXT[] NOT NULL DEFAULT '{}',
        global_decision TEXT,
        decision_persisted_at TIMESTAMPTZ,
        recovery_metadata JSONB NOT NULL DEFAULT '{}'
    )
    `,
    `
    CREATE TABLE IF NOT EXISTS distributed_transaction_participants (
        transaction_id TEXT NOT NULL,
        shard_id TEXT NOT NULL,
        prepared_transaction_id TEXT,
        participant_status TEXT NOT NULL DEFAULT 'active',
        prepared_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        error_details TEXT,
        PRIMARY KEY (transaction_id, shard_id),
        CONSTRAINT fk_distributed_tx_participants_tx
            FOREIGN KEY (transaction_id) REFERENCES distributed_transactions(transaction_id)
    )
    `,
    `CREATE INDEX IF NOT EXISTS idx_distributed_tx_status ON distributed_transactions(status)`,
    `CREATE INDEX IF NOT EXISTS idx_distributed_tx_global_decision ON distributed_transactions(global_decision)`,
    `CREATE INDEX IF NOT EXISTS idx_distributed_tx_updated_at ON distributed_transactions(updated_at)`,
    `CREATE INDEX IF NOT EXISTS idx_distributed_tx_participants_tx ON distributed_transaction_participants(transaction_id)`,
    `CREATE INDEX IF NOT EXISTS idx_distributed_tx_participants_status ON distributed_transaction_participants(participant_status)`,
    `CREATE INDEX IF NOT EXISTS idx_distributed_tx_participants_prepared ON distributed_transaction_participants(prepared_transaction_id) WHERE prepared_transaction_id IS NOT NULL`,
];

export const DISTRIBUTED_TX_TABLES = ["distributed_transactions", "distributed_transaction_participants"];
