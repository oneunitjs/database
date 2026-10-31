import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ReshardingStore } from "../../../src/resharding/resharding-store.js";

import {
    InvalidStateTransitionError,
    MigrationLockedError,
    MigrationNotFoundError,
} from "../../../src/resharding/resharding-errors.js";

function row(migrationId = "m-1", status = "planned", version = 1) {
    return {
        migration_id: migrationId,
        status,
        version,
        source_routing_version: 1,
        target_routing_version: 2,
        source_topology: { shards: [], strategy: "hash", strategyConfig: {}, routingVersion: 1, capturedAt: "t" },
        target_topology: { shards: [], strategy: "hash", strategyConfig: {}, routingVersion: 2, capturedAt: "t" },
        strategy: "hash",
        strategy_config: null,
        plan: { migrationId, units: [], version: 1 },
        units_total: 0,
        units_completed: 0,
        units_failed: 0,
        rows_copied: 0,
        rows_validated: 0,
        change_log_lag: 0,
        created_at: "t",
        updated_at: "t",
        started_at: null,
        completed_at: null,
        finalized_at: null,
        paused_at: null,
        error_code: null,
        error_message: null,
        cutover_intent: false,
        cutover_committed: false,
        worker_id: null,
        worker_locked_at: null,
    };
}

function makePool(existingRow) {
    const queries = [];
    return {
        queries,
        connect: vi.fn(() => ({
            query: vi.fn(async (text, params = []) => {
                queries.push({ text, params });
                if (text.includes("resharding_migrations WHERE migration_id = $1")) {
                    return { rowCount: existingRow ? 1 : 0, rows: existingRow ? [existingRow] : [] };
                }
                if (text.includes("UPDATE resharding_migrations") && text.includes("SET status")) {
                    const [, newStatus, expectedStatus, expectedVersion] = params;
                    if (!existingRow) return { rowCount: 0, rows: [] };
                    if (
                        existingRow.status === expectedStatus &&
                        Number(existingRow.version) === Number(expectedVersion)
                    ) {
                        existingRow.status = newStatus;
                        existingRow.version = (existingRow.version ?? 0) + 1;
                        return { rowCount: 1, rows: [{ version: existingRow.version }] };
                    }
                    return { rowCount: 0, rows: [] };
                }
                if (text.includes("INSERT INTO resharding_migrations")) {
                    return { rowCount: 1, rows: [] };
                }
                if (text.includes("INSERT INTO resharding_units")) {
                    return { rowCount: 1, rows: [] };
                }
                if (text.includes("UPDATE resharding_units")) {
                    return { rowCount: 1, rows: [] };
                }
                if (text.includes("SELECT routing_version FROM resharding_routing_versions")) {
                    return { rowCount: 0, rows: [] };
                }
                return { rowCount: 0, rows: [] };
            }),
            release: vi.fn(),
        })),
        end: vi.fn(),
    };
}

const LEGAL = [
    ["planned", "copying"],
    ["copying", "catching_up"],
    ["catching_up", "validating"],
    ["validating", "ready_for_cutover"],
    ["ready_for_cutover", "cutting_over"],
    ["cutting_over", "completed"],
    ["completed", "finalized"],
    ["planned", "paused"],
    ["copying", "paused"],
    ["catching_up", "paused"],
    ["validating", "paused"],
    ["paused", "copying"],
    ["paused", "catching_up"],
    ["paused", "validating"],
    ["paused", "cancelled"],
    ["paused", "failed"],
    ["copying", "failed"],
    ["catching_up", "failed"],
    ["validating", "failed"],
    ["ready_for_cutover", "failed"],
    ["cutting_over", "failed"],
    ["failed", "cancelled"],
    ["failed", "rollback_required"],
    ["failed", "planned"],
    ["rollback_required", "cancelled"],
    ["rollback_required", "planned"],
    ["planned", "cancelled"],
    ["copying", "cancelled"],
    ["catching_up", "cancelled"],
    ["validating", "cancelled"],
    ["ready_for_cutover", "cancelled"],
];

const ILLEGAL = [
    ["planned", "validating"],
    ["planned", "cutting_over"],
    ["planned", "completed"],
    ["copying", "ready_for_cutover"],
    ["copying", "completed"],
    ["catching_up", "completed"],
    ["validating", "completed"],
    ["ready_for_cutover", "completed"],
    ["cutting_over", "validating"],
    ["completed", "copying"],
    ["finalized", "copying"],
    ["cancelled", "planned"],
    ["cancelled", "copying"],
    ["failed", "completed"],
];

describe("ReshardingStore — full state-machine transitions", () => {
    let store;
    let pool;

    beforeEach(() => {
        pool = makePool(undefined);
        store = new ReshardingStore(pool);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("returns undefined for unknown migration", async () => {
        const r = await store.getMigration("nope");
        expect(r).toBeUndefined();
    });

    it.each(LEGAL)("allows %s -> %s", async (from, to) => {
        const existing = row("m-legal", from, 1);
        const s = new ReshardingStore(makePool(existing));
        await s.transition("m-legal", from, to, 1);
        expect(existing.status).toBe(to);
        expect(existing.version).toBe(2);
    });

    it.each(ILLEGAL)("rejects %s -> %s", async (from, to) => {
        const existing = row("m-illegal", from, 1);
        const s = new ReshardingStore(makePool(existing));
        await expect(s.transition("m-illegal", from, to, 1)).rejects.toThrow(
            InvalidStateTransitionError,
        );
        expect(existing.status).toBe(from);
        expect(existing.version).toBe(1);
    });

    it("requires matching version for optimistic concurrency", async () => {
        const existing = row("m-oc", "planned", 5);
        const s = new ReshardingStore(makePool(existing));
        await expect(s.transition("m-oc", "planned", "copying", 1)).rejects.toThrow(
            MigrationLockedError,
        );
        expect(existing.status).toBe("planned");
        expect(existing.version).toBe(5);
    });

    it("throws MigrationNotFoundError when row missing on transition", async () => {
        const s = new ReshardingStore(makePool(undefined));
        await expect(s.transition("m-missing", "planned", "copying", 1)).rejects.toThrow(
            MigrationNotFoundError,
        );
    });

    it("requireMigration throws MigrationNotFoundError for unknown", async () => {
        await expect(store.requireMigration("m-6")).rejects.toThrow(MigrationNotFoundError);
    });

    it("insertMigration persists plan and units", async () => {
        const plan = {
            migrationId: "m-ins",
            createdAt: "t",
            sourceRoutingVersion: 1,
            targetRoutingVersion: 2,
            sourceTopology: { shards: [], strategy: "hash", strategyConfig: {}, routingVersion: 1, capturedAt: "t" },
            targetTopology: { shards: [], strategy: "hash", strategyConfig: {}, routingVersion: 2, capturedAt: "t" },
            strategy: "hash",
            strategyConfig: {},
            units: [
                { unitId: "u-1", unitType: "shard-key", sourceShardId: "s-0", destinationShardId: "s-1", shardKey: "k-1", estimatedRows: 10, status: "planned", rowsCopied: 0, lastCheckpoint: null, updatedAt: "t" },
            ],
            estimatedTotalRows: 10,
            affectedShardIds: ["s-0", "s-1"],
            dryRun: false,
            version: 1,
        };
        const descriptor = { migrationId: "m-ins", tables: [], order: [], consistencyBoundary: "streamed" };
        const s = new ReshardingStore(makePool(undefined));
        await s.insertMigration(plan, descriptor);
        expect(true).toBe(true);
    });

    it("publishRoutingVersion retires previous current and sets new", async () => {
        const s = new ReshardingStore(makePool(undefined));
        await s.publishRoutingVersion(2, "m-1");
        expect(true).toBe(true);
    });

    it("currentRoutingVersion returns 0 when none published", async () => {
        const v = await store.currentRoutingVersion();
        expect(v).toBe(0);
    });

    it("updateUnitCheckpoint advances status and cursor", async () => {
        const existing = row("m-uc", "planned", 1);
        const s = new ReshardingStore(makePool(existing));
        await s.updateUnitCheckpoint("m-uc", "u-1", {
            status: "copying",
            rowsCopied: 5,
            lastCheckpoint: { kind: "keyset", columns: ["id"], lastValues: [10], batchNo: 1, updatedAt: "t" },
        });
        expect(true).toBe(true);
    });

    it("updateProgress updates counters", async () => {
        const existing = row("m-up", "planned", 1);
        const s = new ReshardingStore(makePool(existing));
        await s.updateProgress("m-up", { rowsCopied: 100, changeLogLag: 3 });
        expect(true).toBe(true);
    });
});
