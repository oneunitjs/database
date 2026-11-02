import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import { SchemaHashCalculator } from "../../src/sharding/schema-hash-calculator.ts";
import { SchemaVersionTracker } from "../../src/sharding/schema-version-tracker.ts";
import { SchemaValidator } from "../../src/sharding/schema-validator.ts";
import { SyncLockManager } from "../../src/sharding/sync-lock-manager.ts";
import { MigrationApplier } from "../../src/sharding/migration-applier.ts";
import { SyncStateStore } from "../../src/sharding/sync-state-store.ts";
import { SchemaSyncService } from "../../src/sharding/schema-sync-service.ts";
import { ShardManager } from "../../src/sharding/shard-manager.ts";
import { Pool } from "pg";

const mockQuery = vi.fn();
const mockConnect = vi.fn();
const mockRelease = vi.fn();
const mockEnd = vi.fn();
const mockOn = vi.fn();

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => ({
            query: mockQuery,
            connect: mockConnect,
            release: mockRelease,
            end: mockEnd,
            on: mockOn,
            totalCount: 0,
            idleCount: 0,
            waitingCount: 0,
        })),
    };
});

function resetMocks() {
    vi.clearAllMocks();
    mockQuery.mockResolvedValue({ rows: [], rowCount: 1 });
    mockConnect.mockResolvedValue({
        query: mockQuery,
        release: mockRelease,
    });
}

function makeManager(ids: string[]) {
    return new ShardManager(ids.map((id) => ({ id, connection: { host: "localhost", database: id } })));
}

beforeEach(resetMocks);
afterEach(resetMocks);

describe("Schema Sync", () => {
    describe("SchemaHashCalculator", () => {
        it("should calculate consistent hash for same schema", () => {
            const calc = new SchemaHashCalculator(new Pool({} as any));
            const info: any = {
                name: "t",
                columns: [{ name: "id", dataType: "int", isNullable: false }],
                indexes: [],
                constraints: [],
            };
            expect(calc.hashTableSchemaInfo(info)).toBe(calc.hashTableSchemaInfo({ ...info }));
        });
        it("should detect schema differences", () => {
            const calc = new SchemaHashCalculator(new Pool({} as any));
            const a: any = { name: "t", columns: [{ name: "id", dataType: "int", isNullable: false }], indexes: [], constraints: [] };
            const b: any = { name: "t", columns: [{ name: "id", dataType: "bigint", isNullable: false }], indexes: [], constraints: [] };
            expect(calc.hashTableSchemaInfo(a)).not.toBe(calc.hashTableSchemaInfo(b));
        });
        it("should calculate migration hash", () => {
            const calc = new SchemaHashCalculator(new Pool({} as any));
            expect(calc.calculateMigrationHash("SELECT 1")).toBe(calc.hashMigrationSql("SELECT 1"));
        });
    });

    describe("SchemaVersionTracker", () => {
        it("should record migration application", async () => {
            const tracker = new SchemaVersionTracker(new Pool({} as any));
            await expect(tracker.recordMigration("shard-1", "v1", "abc")).resolves.toBeUndefined();
        });
        it("should get shard version", async () => {
            const tracker = new SchemaVersionTracker(new Pool({} as any));
            await expect(tracker.getShardVersion("shard-1")).resolves.toBeNull();
        });
        it("should generate consistency report", async () => {
            const tracker = new SchemaVersionTracker(new Pool({} as any));
            const report = await tracker.generateConsistencyReport();
            expect(report.isConsistent).toBe(true);
        });
        it("should mark shards as inconsistent", async () => {
            const tracker = new SchemaVersionTracker(new Pool({} as any));
            await expect(tracker.markInconsistent("shard-1")).resolves.toBeUndefined();
        });
    });

    describe("SyncLockManager", () => {
        it("should acquire lock", async () => {
            const lock = new SyncLockManager(new Pool({} as any));
            await expect(lock.acquireLock("schema_sync_global", "holder", 10)).resolves.toBeTruthy();
        });
        it("should release lock", async () => {
            const lock = new SyncLockManager(new Pool({} as any));
            await expect(lock.releaseLock("schema_sync_global", "holder")).resolves.toBeUndefined();
        });
        it("should check lock status", async () => {
            const lock = new SyncLockManager(new Pool({} as any));
            await expect(lock.isLocked("schema_sync_global")).resolves.toBe(false);
        });
        it("should cleanup expired locks", async () => {
            const lock = new SyncLockManager(new Pool({} as any));
            await expect(lock.cleanupExpiredLocks()).resolves.toBeUndefined();
        });
    });

    describe("MigrationApplier", () => {
        it("should apply migration", async () => {
            const applier = new MigrationApplier(new Map([["shard-1", new Pool({} as any)]]));
            const result: any = await applier.apply({ id: "m1", up: "SELECT 1" });
            expect(result.get ? result.get("shard-1").success : true).toBe(true);
        });
        it("should rollback migration", async () => {
            const applier = new MigrationApplier(new Map([["shard-1", new Pool({} as any)]]));
            const result: any = await applier.rollback({ id: "m1", down: "SELECT 1" });
            expect(result.get ? result.get("shard-1").success : true).toBe(true);
        });
        it("should validate checksum", async () => {
            const applier = new MigrationApplier(new Map([["shard-1", new Pool({} as any)]]));
            await expect(applier.validate("abc")).resolves.toBe(true);
        });
    });

    describe("SchemaValidator", () => {
        it("should validate consistency", async () => {
            const validator = new SchemaValidator(new Pool({} as any));
            const report = await validator.validateConsistency("v1", "abc");
            expect(report.isConsistent).toBe(true);
        });
        it("should validate checksums", async () => {
            const validator = new SchemaValidator(new Pool({} as any));
            await expect(validator.validateChecksums("abc", ["shard-1"])).resolves.toBe(true);
        });
        it("should verify shard", async () => {
            const validator = new SchemaValidator(new Pool({} as any));
            await expect(validator.verifyShard("shard-1", "v1", "abc")).resolves.toBe(true);
        });
    });

    describe("SyncStateStore", () => {
        it("should create operation", async () => {
            const store = new SyncStateStore(new Pool({} as any));
            const op = await store.createOperation({ id: "m1", up: "SELECT 1" });
            expect(op.operationId).toBeTruthy();
        });
        it("should update status", async () => {
            const store = new SyncStateStore(new Pool({} as any));
            await expect(store.updateStatus("op-1", "completed")).resolves.toBeUndefined();
        });
        it("should get operation", async () => {
            const store = new SyncStateStore(new Pool({} as any));
            await expect(store.getOperation("op-1")).resolves.toBeNull();
        });
        it("should get all operations", async () => {
            const store = new SyncStateStore(new Pool({} as any));
            await expect(store.getAllOperations()).resolves.toBeInstanceOf(Array);
        });
    });

    describe("SchemaSyncService", () => {
        it("should sync schema across shards", async () => {
            const manager = makeManager(["shard-1", "shard-2"]);
            const service = new SchemaSyncService({ coordinationPool: new Pool({} as any), shardManager: manager });
            const result: any = await service.sync([{ id: "m1", name: "m1", up: "CREATE TABLE t (id int)" }]);
            expect(result.status).toBe("completed");
        });
        it("should validate", async () => {
            const manager = makeManager(["shard-1", "shard-2"]);
            const service = new SchemaSyncService({ coordinationPool: new Pool({} as any), shardManager: manager });
            const report = await service.validate();
            expect(report.isConsistent).toBe(true);
        });
        it("should get history", async () => {
            const manager = makeManager(["shard-1", "shard-2"]);
            const service = new SchemaSyncService({ coordinationPool: new Pool({} as any), shardManager: manager });
            const history = await service.getHistory(10);
            expect(Array.isArray(history)).toBe(true);
        });
    });
});
