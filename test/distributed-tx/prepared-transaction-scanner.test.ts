import { describe, it, expect, vi, beforeEach } from "vitest";
import {
    PreparedTransactionScanner,
    defaultPreparedTransactionScannerLogger,
} from "../../src/distributed-tx/prepared-transaction-scanner.js";

function makeMockPool(rows: any[] = []) {
    return {
        query: vi.fn().mockResolvedValue({ rows }),
    } as any;
}

describe("PreparedTransactionScanner", () => {
    let scanner: PreparedTransactionScanner;

    beforeEach(() => {
        scanner = new PreparedTransactionScanner(
            makeMockPool(),
            defaultPreparedTransactionScannerLogger,
        );
    });

    it("scanPreparedTransactions maps rows", async () => {
        const now = new Date();
        const pool = makeMockPool([
            {
                gid: "pg_tx-1_s1",
                prepared: now,
                owner: "user1",
                database: "db1",
            },
            {
                gid: "pg_tx-2_s2",
                prepared: new Date(Date.now() - 7_200_000),
                owner: null,
                database: "db2",
            },
        ]);
        scanner = new PreparedTransactionScanner(pool, defaultPreparedTransactionScannerLogger);
        const infos = await scanner.scanPreparedTransactions();
        expect(infos.length).toBe(2);
        expect(infos[0].transactionId).toBe("pg_tx-1_s1");
        expect(infos[0].preparedBy).toBe("user1");
        expect(infos[0].isOrphaned).toBe(false);
        expect(infos[0].canCleanup).toBe(false);
        expect(infos[1].transactionId).toBe("pg_tx-2_s2");
        expect(infos[1].isOrphaned).toBe(true);
        expect(infos[1].canCleanup).toBe(true);
    });

    it("scanPreparedTransactions filters by transactionId", async () => {
        const pool = makeMockPool([
            { gid: "pg_tx-1_s1", prepared: new Date(), owner: "u1", database: "db" },
        ]);
        scanner = new PreparedTransactionScanner(pool, defaultPreparedTransactionScannerLogger);
        const infos = await scanner.scanPreparedTransactions({ transactionId: "pg_tx-1_s1" });
        expect(infos.length).toBe(1);
        const query = (pool.query as any).mock.calls[0][0];
        expect(query).toContain("WHERE gid = $1");
    });

    it("isOwnerInvalid queries pg_stat_activity", async () => {
        const pool = {
            query: vi.fn().mockResolvedValue({ rows: [] }),
        } as any;
        scanner = new PreparedTransactionScanner(pool, defaultPreparedTransactionScannerLogger);
        const result = await scanner.isOwnerInvalid("someuser");
        expect(result).toBe(true);
        expect(pool.query).toHaveBeenCalledWith(
            "SELECT 1 FROM pg_stat_activity WHERE usename = $1 LIMIT 1",
            ["someuser"],
        );
    });

    it("isOwnerInvalid returns false when owner active", async () => {
        const pool = {
            query: vi.fn().mockResolvedValue({ rows: [{ usename: "someuser" }] }),
        } as any;
        scanner = new PreparedTransactionScanner(pool, defaultPreparedTransactionScannerLogger);
        const result = await scanner.isOwnerInvalid("someuser");
        expect(result).toBe(false);
    });

    it("defaultPreparedTransactionScannerLogger is a working no-op logger", () => {
        expect(() => {
            defaultPreparedTransactionScannerLogger.debug({ x: 1 }, "msg");
            defaultPreparedTransactionScannerLogger.info({ x: 1 }, "msg");
            defaultPreparedTransactionScannerLogger.warn({ x: 1 }, "msg");
            defaultPreparedTransactionScannerLogger.error({ x: 1 }, "msg");
        }).not.toThrow();
    });
});
