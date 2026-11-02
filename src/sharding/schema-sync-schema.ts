// ---------------------------------------------------------------------------
// Schema Auto-Sync — PostgreSQL Schema
//
// Defines the tables used by the SchemaSyncService to track schema versions,
// synchronization state, and migration history across all shards.
//
// Execute these statements against your coordination database before using
// SchemaSyncService.
// ---------------------------------------------------------------------------

export const SCHEMA_SYNC_SCHEMA = `
CREATE TABLE IF NOT EXISTS schema_sync_state (
    id BIGSERIAL PRIMARY KEY,
    operation_id UUID NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'applying', 'validating', 'completed', 'failed', 'rolled_back', 'cancelling', 'cancelled')),
    migration_version VARCHAR(255) NOT NULL,
    migration_name VARCHAR(255) NOT NULL,
    migration_sql TEXT NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    error_stack TEXT,
    retries INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schema_sync_state_operation_id ON schema_sync_state(operation_id);
CREATE INDEX IF NOT EXISTS idx_schema_sync_state_status ON schema_sync_state(status);
CREATE INDEX IF NOT EXISTS idx_schema_sync_state_version ON schema_sync_state(migration_version);

CREATE TABLE IF NOT EXISTS shard_schema_versions (
    id BIGSERIAL PRIMARY KEY,
    shard_id TEXT NOT NULL,
    migration_version VARCHAR(255) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum VARCHAR(64) NOT NULL,
    is_consistent BOOLEAN NOT NULL DEFAULT TRUE,
    last_verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(shard_id, migration_version)
);

CREATE INDEX IF NOT EXISTS idx_shard_schema_versions_shard_id ON shard_schema_versions(shard_id);
CREATE INDEX IF NOT EXISTS idx_shard_schema_versions_version ON shard_schema_versions(migration_version);
CREATE INDEX IF NOT EXISTS idx_shard_schema_versions_consistency ON shard_schema_versions(is_consistent) WHERE is_consistent = FALSE;

CREATE TABLE IF NOT EXISTS schema_sync_locks (
    id BIGSERIAL PRIMARY KEY,
    lock_name TEXT NOT NULL UNIQUE,
    lock_holder TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schema_sync_locks_name ON schema_sync_locks(lock_name);
CREATE INDEX IF NOT EXISTS idx_schema_sync_locks_expires ON schema_sync_locks(expires_at);

CREATE TABLE IF NOT EXISTS migration_checksums (
    id BIGSERIAL PRIMARY KEY,
    version VARCHAR(255) NOT NULL UNIQUE,
    checksum VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_migration_checksums_version ON migration_checksums(version);
`;
