import type { Pool } from "pg";
import type {
    FailoverState,
    ShardFailoverState,
} from "./failover-types.js";

/**
 * Context recorded for each state store mutation, capturing who or what
 * initiated the change and when.
 */
export interface FailoverContext {
    timestamp: Date;
    initiatedBy: string;
    metadata?: Record<string, unknown>;
}

/**
 * A single versioned entry in the failover state history.
 */
export interface FailoverStateVersion {
    version: number;
    shardId: string;
    state: ShardFailoverState;
    previousVersion: number;
    changedBy: string;
    changedAt: Date;
    reason?: string;
}

/**
 * Shard failover state augmented with persistence metadata. The `_version`
 * and `updatedAt` fields are managed by the store and are not part of the
 * public `ShardFailoverState` interface, so existing consumers are unaffected.
 */
export interface PersistedShardState extends ShardFailoverState {
    _version: number;
    updatedAt: string;
}

/**
 * SQL schema for the failover state store. Execute these statements against
 * your PostgreSQL database before using `PgFailoverStateStore`.
 */
export const FAILOVER_STATE_SCHEMA = `
CREATE TABLE IF NOT EXISTS _database_ha_state (
    shard_id TEXT NOT NULL PRIMARY KEY,
    state_json JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _database_ha_state_versions (
    id BIGSERIAL PRIMARY KEY,
    shard_id TEXT NOT NULL,
    state_json JSONB NOT NULL,
    version BIGINT NOT NULL,
    previous_version BIGINT NOT NULL,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT
);

CREATE TABLE IF NOT EXISTS _database_ha_locks (
    lock_name TEXT NOT NULL PRIMARY KEY,
    locked_by TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lock_expiry TIMESTAMPTZ NOT NULL
);
`;

/**
 * Contract for persisting and retrieving failover state. Implementations are
 * responsible for concurrency control so that multiple failover manager
 * instances cannot corrupt shared state.
 */
export interface FailoverStateStore {
    loadState(shardId: string): Promise<ShardFailoverState | null>;
    saveState(shardId: string, state: ShardFailoverState, expectedVersion: number): Promise<boolean>;
    transitionState(shardId: string, fromState: FailoverState, toState: FailoverState, reason: string, context: FailoverContext): Promise<boolean>;
    listStates(): Promise<Map<string, ShardFailoverState>>;
    acquireLock(shardId: string, ttlMs: number): Promise<boolean>;
    releaseLock(shardId: string): Promise<void>;
    getStateHistory(shardId: string, limit?: number): Promise<FailoverStateVersion[]>;
}

type FailoverLogger = {
    debug?: (payload: unknown, message?: string) => void;
    info?: (payload: unknown, message?: string) => void;
    warn?: (payload: unknown, message?: string) => void;
    error?: (payload: unknown, message?: string) => void;
};

/**
 * PostgreSQL-backed failover state store. Uses optimistic concurrency on
 * the `version` column for saves and state-value checks for transitions.
 * Requires the tables defined in `FAILOVER_STATE_SCHEMA` to exist.
 */
export class PgFailoverStateStore implements FailoverStateStore {
    constructor(
        private readonly pool: Pool,
        private readonly logger: FailoverLogger = {},
    ) {}

    async loadState(shardId: string): Promise<ShardFailoverState | null> {
        const result = await this.pool.query(
            `SELECT state_json, version, updated_at FROM _database_ha_state WHERE shard_id = $1`,
            [shardId]
        );
        if (result.rows.length === 0) return null;
        const row = result.rows[0];
        return {
            ...row.state_json,
            _version: row.version,
            updatedAt: row.updated_at,
        } as unknown as ShardFailoverState;
    }

    async saveState(shardId: string, state: ShardFailoverState, expectedVersion: number): Promise<boolean> {
        const result = await this.pool.query(
            `UPDATE _database_ha_state
             SET state_json = $1::jsonb, version = version + 1, updated_at = NOW(), updated_by = $2
             WHERE shard_id = $3 AND version = $4
             RETURNING version`,
            [JSON.stringify(state), "failover-manager", shardId, expectedVersion]
        );
        if (result.rowCount === 0) return false;
        const newVersion = Number(result.rows[0].version);
        await this.logVersionChange(shardId, state, expectedVersion, newVersion, undefined);
        return true;
    }

    async transitionState(
        shardId: string,
        fromState: FailoverState,
        toState: FailoverState,
        reason: string,
        context: FailoverContext,
    ): Promise<boolean> {
        const result = await this.pool.query(
            `UPDATE _database_ha_state
             SET state_json = state_json || $1::jsonb, version = version + 1, updated_at = NOW(), updated_by = $2
             WHERE shard_id = $3 AND (state_json->>'state') = $4
             RETURNING version`,
            [
                JSON.stringify({ state: toState, updatedAt: context.timestamp.toISOString() }),
                context.initiatedBy,
                shardId,
                fromState,
            ]
        );
        if (result.rowCount === 0) return false;
        const newVersion = Number(result.rows[0].version);
        const loaded = await this.loadState(shardId);
        const stateForHistory = loaded ?? minimalState(shardId, toState);
        await this.logVersionChange(shardId, stateForHistory, newVersion - 1, newVersion, reason, context.initiatedBy);
        return true;
    }

    async listStates(): Promise<Map<string, ShardFailoverState>> {
        const result = await this.pool.query(
            `SELECT shard_id, state_json, version, updated_at FROM _database_ha_state`
        );
        const states = new Map<string, ShardFailoverState>();
        for (const row of result.rows) {
            states.set(row.shard_id, {
                ...row.state_json,
                _version: row.version,
                updatedAt: row.updated_at,
            } as unknown as ShardFailoverState);
        }
        return states;
    }

    async acquireLock(shardId: string, ttlMs: number): Promise<boolean> {
        const lockName = `ha_lock_${shardId}`;
        const expiry = new Date(Date.now() + ttlMs);
        const result = await this.pool.query(
            `INSERT INTO _database_ha_locks (lock_name, locked_by, locked_at, lock_expiry)
             VALUES ($1, $2, NOW(), $3)
             ON CONFLICT (lock_name) DO UPDATE SET
                 locked_by = EXCLUDED.locked_by,
                 locked_at = EXCLUDED.locked_at,
                 lock_expiry = EXCLUDED.lock_expiry
             WHERE _database_ha_locks.lock_expiry < NOW()
             RETURNING *`,
            [lockName, `worker_${process.pid}`, expiry]
        );
        return (result.rowCount ?? 0) > 0;
    }

    async releaseLock(shardId: string): Promise<void> {
        const lockName = `ha_lock_${shardId}`;
        await this.pool.query('DELETE FROM _database_ha_locks WHERE lock_name = $1', [lockName]);
    }

    async getStateHistory(shardId: string, limit?: number): Promise<FailoverStateVersion[]> {
        const limitClause = limit !== undefined ? ` LIMIT ${Math.floor(limit)}` : "";
        const result = await this.pool.query(
            `SELECT id, shard_id, state_json, version, previous_version, changed_by, changed_at, reason
             FROM _database_ha_state_versions
             WHERE shard_id = $1
             ORDER BY version DESC${limitClause}`,
            [shardId]
        );
        return result.rows.map((row) => ({
            version: Number(row.version),
            shardId: row.shard_id,
            state: row.state_json as ShardFailoverState,
            previousVersion: Number(row.previous_version),
            changedBy: row.changed_by,
            changedAt: new Date(row.changed_at),
            reason: row.reason ?? undefined,
        }));
    }

    private async logVersionChange(
        shardId: string,
        state: ShardFailoverState,
        prevVersion: number,
        newVersion: number,
        reason?: string,
        changedBy?: string,
    ): Promise<void> {
        await this.pool.query(
            `INSERT INTO _database_ha_state_versions (shard_id, state_json, version, previous_version, changed_by, reason)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [shardId, state, newVersion, prevVersion, changedBy ?? "system", reason ?? null]
        );
    }
}

function minimalState(shardId: string, state: FailoverState): ShardFailoverState {
    return {
        shardId,
        state,
        currentGeneration: 1,
        failoverEvents: [],
        lastStateChange: new Date().toISOString(),
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
    };
}

/**
 * In-memory failover state store. Intended for testing and single-process
 * deployments where persistence across restarts is not required.
 */
export class MemoryFailoverStateStore implements FailoverStateStore {
    private readonly states = new Map<string, PersistedShardState>();
    private readonly history = new Map<string, FailoverStateVersion[]>();
    private readonly locks = new Map<string, { lockedBy: string; expiry: Date }>();

    async loadState(shardId: string): Promise<ShardFailoverState | null> {
        const state = this.states.get(shardId);
        if (!state) return null;
        return { ...state };
    }

    async saveState(shardId: string, state: ShardFailoverState, expectedVersion: number): Promise<boolean> {
        const existing = this.states.get(shardId);
        if (!existing) {
            this.states.set(shardId, {
                ...state,
                _version: expectedVersion + 1,
                updatedAt: new Date().toISOString(),
            } as PersistedShardState);
            return true;
        }
        if (existing._version !== expectedVersion) return false;
        this.states.set(shardId, {
            ...state,
            _version: expectedVersion + 1,
            updatedAt: new Date().toISOString(),
        } as PersistedShardState);
        return true;
    }

    async transitionState(
        shardId: string,
        fromState: FailoverState,
        toState: FailoverState,
        reason: string,
        context: FailoverContext,
    ): Promise<boolean> {
        const existing = this.states.get(shardId);
        if (!existing || existing.state !== fromState) return false;
        const next: PersistedShardState = {
            ...existing,
            state: toState,
            _version: existing._version + 1,
            updatedAt: context.timestamp.toISOString(),
        };
        this.states.set(shardId, next);
        const entry: FailoverStateVersion = {
            version: next._version,
            shardId,
            state: { ...next },
            previousVersion: existing._version,
            changedBy: context.initiatedBy,
            changedAt: context.timestamp,
            reason,
        };
        const h = this.history.get(shardId) ?? [];
        h.push(entry);
        this.history.set(shardId, h);
        return true;
    }

    async listStates(): Promise<Map<string, ShardFailoverState>> {
        const result = new Map<string, ShardFailoverState>();
        for (const [shardId, state] of this.states) {
            result.set(shardId, { ...state });
        }
        return result;
    }

    async acquireLock(shardId: string, ttlMs: number): Promise<boolean> {
        const existing = this.locks.get(shardId);
        if (existing && existing.expiry > new Date()) return false;
        this.locks.set(shardId, {
            lockedBy: `worker_${process.pid}`,
            expiry: new Date(Date.now() + ttlMs),
        });
        return true;
    }

    async releaseLock(shardId: string): Promise<void> {
        this.locks.delete(shardId);
    }

    async getStateHistory(shardId: string, limit?: number): Promise<FailoverStateVersion[]> {
        const h = this.history.get(shardId) ?? [];
        const sorted = [...h].sort((a, b) => b.version - a.version);
        return limit !== undefined ? sorted.slice(0, Math.floor(limit)) : sorted;
    }
}
