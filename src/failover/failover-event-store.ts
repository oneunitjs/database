import type { Pool } from "pg";
import type { FailoverState } from "./failover-types.js";

/**
 * Type of failover event recorded in the event history store.
 */
export type FailoverEventType =
    | "state_change"
    | "health_check_started"
    | "health_check_failed"
    | "health_check_succeeded"
    | "failover_started"
    | "failover_completed"
    | "failover_failed"
    | "promotion_started"
    | "promotion_completed"
    | "connection_invalidated"
    | "lock_acquired"
    | "lock_released"
    | "replica_added"
    | "replica_removed";

/**
 * A single failover event persisted in the event history store.
 */
export interface FailoverEvent {
    id: string;
    timestamp: Date;
    type: FailoverEventType;
    shardId: string;
    nodeId?: string;
    details: Record<string, unknown>;
    severity: "debug" | "info" | "warning" | "error";
    previousState?: FailoverState;
    relatedState?: FailoverState;
}

/**
 * Filter for querying failover events.
 */
export interface FailoverEventFilter {
    shardId?: string;
    type?: FailoverEventType;
    severity?: "debug" | "info" | "warning" | "error";
    startTime?: Date;
    endTime?: Date;
    limit?: number;
    offset?: number;
}

/**
 * Aggregated statistics about failover events.
 */
export interface FailoverEventStatistics {
    totalEvents: number;
    eventsByType: Record<string, number>;
    eventsBySeverity: Record<string, number>;
    eventsByShard: Record<string, number>;
    firstEvent?: Date;
    lastEvent?: Date;
}

/**
 * Result of querying events with pagination.
 */
export interface FailoverEventQueryResult {
    events: FailoverEvent[];
    total: number;
}

/**
 * Contract for persisting and querying failover event history.
 */
export interface FailoverEventStore {
    recordEvent(event: Omit<FailoverEvent, "id" | "timestamp">): Promise<FailoverEvent>;
    queryEvents(filter: FailoverEventFilter): Promise<FailoverEventQueryResult>;
    getEventsForShard(shardId: string, limit?: number): Promise<FailoverEvent[]>;
    getLatestEvents(limit?: number): Promise<FailoverEvent[]>;
    cleanupEvents(retentionDays: number): Promise<number>;
    getStatistics(filter?: FailoverEventFilter): Promise<FailoverEventStatistics>;
}

type FailoverLogger = {
    debug?: (payload: unknown, message?: string) => void;
    info?: (payload: unknown, message?: string) => void;
    warn?: (payload: unknown, message?: string) => void;
    error?: (payload: unknown, message?: string) => void;
};

/**
 * SQL schema for the failover event store. Execute these statements against
 * your PostgreSQL database before using `PgFailoverEventStore`.
 */
export const FAILOVER_EVENT_SCHEMA = `
CREATE TABLE IF NOT EXISTS _database_ha_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    type TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    node_id TEXT,
    details JSONB NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    previous_state TEXT,
    new_state TEXT
);

CREATE INDEX IF NOT EXISTS idx_ha_events_shard_id ON _database_ha_events(shard_id);
CREATE INDEX IF NOT EXISTS idx_ha_events_type ON _database_ha_events(type);
CREATE INDEX IF NOT EXISTS idx_ha_events_timestamp ON _database_ha_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_ha_events_severity ON _database_ha_events(severity);
`;

/**
 * PostgreSQL-backed failover event store. Requires the table defined in
 * `FAILOVER_EVENT_SCHEMA` to exist.
 */
export class PgFailoverEventStore implements FailoverEventStore {
    constructor(private readonly pool: Pool, private readonly logger: FailoverLogger = {}) {}

    async recordEvent(event: Omit<FailoverEvent, "id" | "timestamp">): Promise<FailoverEvent> {
        const id = crypto.randomUUID();
        const timestamp = new Date();
        const fullEvent: FailoverEvent = { ...event, id, timestamp };

        await this.pool.query(
            `INSERT INTO _database_ha_events
             (event_id, timestamp, type, shard_id, node_id, details, severity, previous_state, new_state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
            [
                fullEvent.id,
                fullEvent.timestamp,
                fullEvent.type,
                fullEvent.shardId,
                fullEvent.nodeId ?? null,
                JSON.stringify(fullEvent.details),
                fullEvent.severity,
                fullEvent.previousState ?? null,
                fullEvent.relatedState ?? null,
            ]
        );

        return fullEvent;
    }

    async queryEvents(filter: FailoverEventFilter): Promise<FailoverEventQueryResult> {
        const { shardId, type, severity, startTime, endTime, limit = 100, offset = 0 } = filter;

        const conditions: string[] = [];
        const params: unknown[] = [];

        if (shardId) {
            conditions.push(`shard_id = $${params.length + 1}`);
            params.push(shardId);
        }
        if (type) {
            conditions.push(`type = $${params.length + 1}`);
            params.push(type);
        }
        if (severity) {
            conditions.push(`severity = $${params.length + 1}`);
            params.push(severity);
        }
        if (startTime) {
            conditions.push(`timestamp >= $${params.length + 1}`);
            params.push(startTime);
        }
        if (endTime) {
            conditions.push(`timestamp <= $${params.length + 1}`);
            params.push(endTime);
        }

        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

        const countQuery = `SELECT COUNT(*) FROM _database_ha_events ${whereClause}`;
        const dataQuery = `
            SELECT id, event_id as "eventId", timestamp, type, shard_id as "shardId",
                   node_id as "nodeId", details, severity, previous_state as "previousState",
                   new_state as "relatedState"
            FROM _database_ha_events ${whereClause}
            ORDER BY timestamp DESC
            LIMIT $${params.length + 1} OFFSET $${params.length + 2}
        `;

        const dataParams = [...params, limit, offset];

        const [countResult, eventsResult] = await Promise.all([
            this.pool.query(countQuery, params),
            this.pool.query(dataQuery, dataParams),
        ]);

        const total = Number(countResult.rows[0]?.count ?? 0);
        const events = eventsResult.rows.map((row) => this.mapRowToEvent(row));

        return { events, total };
    }

    async getEventsForShard(shardId: string, limit?: number): Promise<FailoverEvent[]> {
        const result = await this.queryEvents({ shardId, limit: limit ?? 1000 });
        return result.events;
    }

    async getLatestEvents(limit?: number): Promise<FailoverEvent[]> {
        const result = await this.queryEvents({ limit: limit ?? 100 });
        return result.events;
    }

    async cleanupEvents(retentionDays: number): Promise<number> {
        const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
        const result = await this.pool.query(
            "DELETE FROM _database_ha_events WHERE timestamp < $1 RETURNING id",
            [cutoff]
        );
        return result.rowCount ?? 0;
    }

    async getStatistics(filter?: FailoverEventFilter): Promise<FailoverEventStatistics> {
        const baseFilter = filter ?? {};
        const result = await this.queryEvents({ ...baseFilter, limit: 10000, offset: 0 });

        const stats: FailoverEventStatistics = {
            totalEvents: result.total,
            eventsByType: {},
            eventsBySeverity: {},
            eventsByShard: {},
        };

        let firstEvent: Date | undefined;
        let lastEvent: Date | undefined;

        for (const event of result.events) {
            stats.eventsByType[event.type] = (stats.eventsByType[event.type] ?? 0) + 1;
            stats.eventsBySeverity[event.severity] =
                (stats.eventsBySeverity[event.severity] ?? 0) + 1;
            stats.eventsByShard[event.shardId] = (stats.eventsByShard[event.shardId] ?? 0) + 1;

            const ts = event.timestamp;
            if (!firstEvent || ts < firstEvent) firstEvent = ts;
            if (!lastEvent || ts > lastEvent) lastEvent = ts;
        }

        stats.firstEvent = firstEvent;
        stats.lastEvent = lastEvent;

        return stats;
    }

    private mapRowToEvent(row: unknown): FailoverEvent {
        const r = row as Record<string, unknown>;
        return {
            id: String(r.eventId ?? r.id),
            timestamp: new Date(String(r.timestamp)),
            type: String(r.type) as FailoverEventType,
            shardId: String(r.shardId),
            nodeId: r.nodeId ? String(r.nodeId) : undefined,
            details: typeof r.details === "string" ? JSON.parse(String(r.details)) : (r.details ?? {}),
            severity: String(r.severity ?? "info") as "debug" | "info" | "warning" | "error",
            previousState: r.previousState ? (String(r.previousState) as FailoverState) : undefined,
            relatedState: r.new_state ? (String(r.new_state) as FailoverState) : undefined,
        };
    }
}

/**
 * In-memory failover event store. Intended for testing and single-process
 * deployments where persistence across restarts is not required.
 */
export class MemoryFailoverEventStore implements FailoverEventStore {
    private events: FailoverEvent[] = [];

    async recordEvent(event: Omit<FailoverEvent, "id" | "timestamp">): Promise<FailoverEvent> {
        const fullEvent: FailoverEvent = {
            ...event,
            id: crypto.randomUUID(),
            timestamp: new Date(),
        };
        this.events.push(fullEvent);
        return fullEvent;
    }

    async queryEvents(filter: FailoverEventFilter): Promise<FailoverEventQueryResult> {
        let filtered = this.events.filter((e) => {
            if (filter.shardId && e.shardId !== filter.shardId) return false;
            if (filter.type && e.type !== filter.type) return false;
            if (filter.severity && e.severity !== filter.severity) return false;
            if (filter.startTime && e.timestamp < filter.startTime) return false;
            if (filter.endTime && e.timestamp > filter.endTime) return false;
            return true;
        });

        const total = filtered.length;
        filtered = filtered.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime());
        const offset = filter.offset ?? 0;
        const limit = filter.limit ?? 100;
        const page = filtered.slice(offset, offset + limit);

        return { events: page, total };
    }

    async getEventsForShard(shardId: string, limit?: number): Promise<FailoverEvent[]> {
        const result = await this.queryEvents({ shardId, limit: limit ?? 1000 });
        return result.events;
    }

    async getLatestEvents(limit?: number): Promise<FailoverEvent[]> {
        const result = await this.queryEvents({ limit: limit ?? 100 });
        return result.events;
    }

    async cleanupEvents(retentionDays: number): Promise<number> {
        const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
        const before = this.events.length;
        this.events = this.events.filter((e) => e.timestamp >= cutoff);
        return before - this.events.length;
    }

    async getStatistics(filter?: FailoverEventFilter): Promise<FailoverEventStatistics> {
        const result = await this.queryEvents({ ...(filter ?? {}), limit: 10000, offset: 0 });

        const stats: FailoverEventStatistics = {
            totalEvents: result.total,
            eventsByType: {},
            eventsBySeverity: {},
            eventsByShard: {},
        };

        let firstEvent: Date | undefined;
        let lastEvent: Date | undefined;

        for (const event of result.events) {
            stats.eventsByType[event.type] = (stats.eventsByType[event.type] ?? 0) + 1;
            stats.eventsBySeverity[event.severity] =
                (stats.eventsBySeverity[event.severity] ?? 0) + 1;
            stats.eventsByShard[event.shardId] = (stats.eventsByShard[event.shardId] ?? 0) + 1;

            const ts = event.timestamp;
            if (!firstEvent || ts < firstEvent) firstEvent = ts;
            if (!lastEvent || ts > lastEvent) lastEvent = ts;
        }

        stats.firstEvent = firstEvent;
        stats.lastEvent = lastEvent;

        return stats;
    }
}
