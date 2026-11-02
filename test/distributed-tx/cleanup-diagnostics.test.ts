import { describe, it, expect, vi, beforeEach } from "vitest";
import {
    TransactionCleanupManager,
    defaultCleanupDiagnosticsLogger,
} from "../../src/distributed-tx/cleanup-diagnostics.js";

function makeMockPool(rows: any[] = []) {
    return {
        query: vi.fn().mockResolvedValue({ rows }),
        connect: vi.fn().mockResolvedValue({
            query: vi.fn().mockResolvedValue({ rows: [] }),
            release: vi.fn(),
        }),
        end: vi.fn(),
    } as any;
}

describe("TransactionCleanupManager", () => {
    let manager: TransactionCleanupManager;

    beforeEach(() => {
        manager = new TransactionCleanupManager(
            makeMockPool(),
            defaultCleanupDiagnosticsLogger,
            { dryRun: true },
        );
    });

    it("diagnose returns stats for scanned transactions", async () => {
        const now = new Date();
        const pool = makeMockPool([
            { gid: "pg_tx-1_s1", prepared: now, owner: "u1", database: "db" },
            { gid: "pg_tx-2_s2", prepared: new Date(Date.now() - 7_200_000), owner: null, database: "db" },
        ]);
        manager = new TransactionCleanupManager(pool, defaultCleanupDiagnosticsLogger, {
            dryRun: true,
        });
        const result = await manager.diagnose();
        expect(result.scanned).toBe(2);
        expect(result.found).toBe(2);
        expect(result.orphaned).toBe(1);
        expect(result.cleanedUp).toBe(0);
        expect(result.recommendations.length).toBeGreaterThan(0);
    });

    it("diagnose autoCleanup cleans orphaned transactions", async () => {
        const pool = makeMockPool([
            { gid: "pg_tx-1_s1", prepared: new Date(), owner: null, database: "db" },
        ]);
        manager = new TransactionCleanupManager(pool, defaultCleanupDiagnosticsLogger, {
            dryRun: false,
            autoCleanup: true,
        });
        const result = await manager.diagnose();
        expect(result.cleanedUp).toBe(1);
    });

    it("cleanupTransaction cleans up a specific transaction", async () => {
        const pool = makeMockPool([{ gid: "pg_tx-1_s1", prepared: new Date(), owner: "u1", database: "db" }]);
        manager = new TransactionCleanupManager(pool, defaultCleanupDiagnosticsLogger, {
            dryRun: false,
        });
        const result = await manager.cleanupTransaction("pg_tx-1_s1");
        expect(result).toBe(true);
    });

    it("cleanupTransaction returns true when no transaction found", async () => {
        const pool = makeMockPool([]);
        manager = new TransactionCleanupManager(pool, defaultCleanupDiagnosticsLogger, {
            dryRun: false,
        });
        const result = await manager.cleanupTransaction("pg_tx-nonexistent");
        expect(result).toBe(true);
    });

    it("cleanupOrphanedTransactions cleans orphaned ones", async () => {
        const pool = makeMockPool([
            { gid: "pg_tx-1_s1", prepared: new Date(), owner: null, database: "db" },
            { gid: "pg_tx-2_s2", prepared: new Date(), owner: "u2", database: "db" },
        ]);
        manager = new TransactionCleanupManager(pool, defaultCleanupDiagnosticsLogger, {
            dryRun: false,
        });
        const result = await manager.cleanupOrphanedTransactions();
        expect(result.orphaned).toBe(1);
        expect(result.cleanedUp).toBe(1);
    });

    it("cleanupOldTransactions cleans old ones", async () => {
        const pool = makeMockPool([
            { gid: "pg_tx-1_s1", prepared: new Date(Date.now() - 7_200_000), owner: "u1", database: "db" },
        ]);
        manager = new TransactionCleanupManager(pool, defaultCleanupDiagnosticsLogger, {
            dryRun: false,
            maxAgeMs: 3_600_000,
        });
        const result = await manager.cleanupOldTransactions();
        expect(result.cleanedUp).toBe(1);
    });

    it("defaultCleanupDiagnosticsLogger is a working no-op logger", () => {
        expect(() => {
            defaultCleanupDiagnosticsLogger.debug({ x: 1 }, "msg");
            defaultCleanupDiagnosticsLogger.info({ x: 1 }, "msg");
            defaultCleanupDiagnosticsLogger.warn({ x: 1 }, "msg");
            defaultCleanupDiagnosticsLogger.error({ x: 1 }, "msg");
        }).not.toThrow();
    });
});
