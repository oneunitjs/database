import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    MemoryFailoverEventStore,
    PgFailoverEventStore,
    FAILOVER_EVENT_SCHEMA,
} from "../../src/failover/failover-event-store.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

describe("MemoryFailoverEventStore", () => {
    let store: MemoryFailoverEventStore;

    beforeEach(() => {
        store = new MemoryFailoverEventStore();
    });

    it("recordEvent stores event with id and timestamp", async () => {
        const event = await store.recordEvent({
            type: "state_change",
            shardId: "shard-1",
            nodeId: "node-1",
            details: { reason: "failover" },
            severity: "info",
        });
        expect(event.id).toBeDefined();
        expect(event.timestamp).toBeInstanceOf(Date);
        expect(event.type).toBe("state_change");
        expect(event.shardId).toBe("shard-1");
    });

    it("queryEvents returns all events", async () => {
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        await store.recordEvent({ type: "failover_started", shardId: "shard-1", details: {}, severity: "warning" });

        const result = await store.queryEvents({});
        expect(result.total).toBe(2);
        expect(result.events.length).toBe(2);
    });

    it("queryEvents filters by shardId and type", async () => {
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        await store.recordEvent({ type: "state_change", shardId: "shard-2", details: {}, severity: "info" });

        const result = await store.queryEvents({ shardId: "shard-1", type: "state_change" });
        expect(result.total).toBe(1);
        expect(result.events[0].shardId).toBe("shard-1");
    });

    it("queryEvents filters by severity", async () => {
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        await store.recordEvent({ type: "failover_failed", shardId: "shard-1", details: {}, severity: "error" });

        const result = await store.queryEvents({ severity: "error" });
        expect(result.total).toBe(1);
        expect(result.events[0].severity).toBe("error");
    });

    it("queryEvents supports limit and offset", async () => {
        for (let i = 0; i < 5; i++) {
            await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        }
        const page1 = await store.queryEvents({ limit: 2, offset: 0 });
        expect(page1.events.length).toBe(2);
        expect(page1.total).toBe(5);

        const page2 = await store.queryEvents({ limit: 2, offset: 2 });
        expect(page2.events.length).toBe(2);
    });

    it("getEventsForShard returns events for a shard", async () => {
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        await store.recordEvent({ type: "state_change", shardId: "shard-2", details: {}, severity: "info" });

        const events = await store.getEventsForShard("shard-1");
        expect(events.length).toBe(1);
        expect(events[0].shardId).toBe("shard-1");
    });

    it("getLatestEvents returns most recent first", async () => {
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        // Ensure timestamp ordering by sleeping
        await new Promise((r) => setTimeout(r, 5));
        await store.recordEvent({ type: "failover_started", shardId: "shard-1", details: {}, severity: "warning" });

        const latest = await store.getLatestEvents(1);
        expect(latest.length).toBe(1);
        expect(latest[0].type).toBe("failover_started");
    });

    it("cleanupEvents removes old events", async () => {
        const old = await store.recordEvent({
            type: "state_change",
            shardId: "shard-1",
            details: {},
            severity: "info",
        });
        // Manually set old timestamp
        old.timestamp = new Date(Date.now() - 100_000);
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });

        const removed = await store.cleanupEvents(0); // retain 0 days
        expect(removed).toBeGreaterThanOrEqual(1);
    });

    it("getStatistics aggregates events", async () => {
        await store.recordEvent({ type: "state_change", shardId: "shard-1", details: {}, severity: "info" });
        await store.recordEvent({ type: "failover_started", shardId: "shard-1", details: {}, severity: "warning" });
        await store.recordEvent({ type: "failover_completed", shardId: "shard-2", details: {}, severity: "info" });

        const stats = await store.getStatistics();
        expect(stats.totalEvents).toBe(3);
        expect(stats.eventsByType["state_change"]).toBe(1);
        expect(stats.eventsByType["failover_started"]).toBe(1);
        expect(stats.eventsBySeverity["info"]).toBe(2);
        expect(stats.eventsBySeverity["warning"]).toBe(1);
        expect(stats.eventsByShard["shard-1"]).toBe(2);
        expect(stats.eventsByShard["shard-2"]).toBe(1);
        expect(stats.firstEvent).toBeInstanceOf(Date);
        expect(stats.lastEvent).toBeInstanceOf(Date);
    });
});

describe("PgFailoverEventStore (mocked pool)", () => {
    let pool: any;
    let store: PgFailoverEventStore;

    beforeEach(() => {
        pool = { query: vi.fn() };
        store = new PgFailoverEventStore(pool, logger);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("recordEvent inserts and returns full event", async () => {
        pool.query.mockResolvedValue({ rows: [] });
        const event = await store.recordEvent({
            type: "state_change",
            shardId: "shard-1",
            nodeId: "node-1",
            details: { reason: "failover" },
            severity: "info",
        });
        expect(event.id).toBeDefined();
        expect(event.timestamp).toBeInstanceOf(Date);
        // Two queries: INSERT + nothing else for recordEvent
        expect(pool.query).toHaveBeenCalledTimes(1);
        const call = pool.query.mock.calls[0];
        expect(call[0]).toContain("_database_ha_events");
        const params = call[1] as any[];
        expect(Array.isArray(params)).toBe(true);
        expect(params[0]).toBe(event.id);
    });

    it("queryEvents returns paginated results with total", async () => {
        pool.query
            .mockResolvedValueOnce({ rows: [{ count: "2" }] }) // count
            .mockResolvedValueOnce({
                rows: [
                    {
                        id: 1,
                        eventId: "uuid-1",
                        timestamp: new Date().toISOString(),
                        type: "state_change",
                        shardId: "shard-1",
                        nodeId: "node-1",
                        details: JSON.stringify({}),
                        severity: "info",
                        previousState: null,
                        new_state: "relatedState",
                    },
                ],
            });
        const result = await store.queryEvents({ shardId: "shard-1", limit: 10, offset: 0 });
        expect(result.total).toBe(2);
        expect(result.events.length).toBe(1);
        expect(result.events[0].shardId).toBe("shard-1");
    });

    it("cleanupEvents deletes old events and returns count", async () => {
        pool.query.mockResolvedValue({ rowCount: 3, rows: [] });
        const removed = await store.cleanupEvents(30);
        expect(removed).toBe(3);
        const call = pool.query.mock.calls[0];
        expect(call[0]).toContain("DELETE FROM _database_ha_events");
        expect(call[0]).toContain("timestamp < $1");
    });

    it("getStatistics returns aggregated stats", async () => {
        pool.query
            .mockResolvedValueOnce({ rows: [{ count: "2" }] })
            .mockResolvedValueOnce({
                rows: [
                    { id: 1, event_id: "u1", timestamp: new Date().toISOString(), type: "state_change", shard_id: "s1", node_id: null, details: "{}", severity: "info", previous_state: null, new_state: null },
                    { id: 2, event_id: "u2", timestamp: new Date().toISOString(), type: "failover_started", shard_id: "s1", node_id: null, details: "{}", severity: "warning", previous_state: null, new_state: null }
                ]
            });
        const result = await store.getStatistics();
        expect(result.totalEvents).toBe(2);
        expect(result.eventsByType["state_change"]).toBe(1);
    });
});

describe("FAILOVER_EVENT_SCHEMA", () => {
    it("defines the events table and indexes", () => {
        expect(FAILOVER_EVENT_SCHEMA).toContain("_database_ha_events");
        expect(FAILOVER_EVENT_SCHEMA).toContain("idx_ha_events_shard_id");
        expect(FAILOVER_EVENT_SCHEMA).toContain("idx_ha_events_type");
        expect(FAILOVER_EVENT_SCHEMA).toContain("idx_ha_events_timestamp");
        expect(FAILOVER_EVENT_SCHEMA).toContain("idx_ha_events_severity");
    });
});
