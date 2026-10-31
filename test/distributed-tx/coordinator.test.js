import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    DistributedTransactionCoordinator,
    MaxPreparedTransactionsError,
} from "../../src/distributed-tx/index.js";

const mockQuery = vi.fn();

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => ({
            query: mockQuery,
            connect: vi.fn(),
            end: vi.fn(),
            on: vi.fn(),
            totalCount: 0,
            idleCount: 0,
            waitingCount: 0,
        })),
    };
});

function resetMocks() {
    vi.clearAllMocks();
}

function mockShowMaxPrepared(value) {
    mockQuery.mockImplementation((sql) => {
        const s = typeof sql === "string" ? sql : String(sql);
        if (s.includes("SHOW max_prepared_transactions")) {
            return Promise.resolve({ rows: [{ max_prepared_transactions: value }], rowCount: 1 });
        }
        if (s.includes("CREATE TABLE IF NOT EXISTS distributed_transactions")) {
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
        if (s.includes("CREATE TABLE IF NOT EXISTS distributed_transaction_participants")) {
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
        if (s.includes("INSERT INTO distributed_transactions")) {
            return Promise.resolve({ rows: [], rowCount: 1 });
        }
        if (s.includes("UPDATE distributed_transactions SET status")) {
            return Promise.resolve({ rows: [], rowCount: 1 });
        }
        if (s.includes("INSERT INTO distributed_transaction_participants")) {
            return Promise.resolve({ rows: [], rowCount: 1 });
        }
        if (s.includes("UPDATE distributed_transaction_participants")) {
            return Promise.resolve({ rows: [], rowCount: 1 });
        }
        if (s.includes("SELECT * FROM distributed_transactions WHERE transaction_id")) {
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
        if (s.includes("SELECT * FROM distributed_transaction_participants WHERE transaction_id")) {
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
        if (s.includes("SELECT transaction_id, status, global_decision FROM distributed_transactions")) {
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
        return Promise.resolve({ rows: [], rowCount: 0 });
    });
}

describe("DistributedTransactionCoordinator", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    it("throws MaxPreparedTransactionsError when max_prepared_transactions is 0", async () => {
        mockShowMaxPrepared("0");

        const { Pool } = await import("pg");
        const pool = new Pool();

        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });

        await expect(coordinator.initialize()).rejects.toBeInstanceOf(MaxPreparedTransactionsError);
    });

    it("initializes successfully when max_prepared_transactions > 0", async () => {
        mockShowMaxPrepared("100");

        const { Pool } = await import("pg");
        const pool = new Pool();

        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });

        await expect(coordinator.initialize()).resolves.toBeUndefined();
    });

    it("begin creates a durable transaction record", async () => {
        mockShowMaxPrepared("100");

        const { Pool } = await import("pg");
        const pool = new Pool();

        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });
        await coordinator.initialize();

        const txId = await coordinator.begin();
        expect(txId).toBeDefined();
        expect(typeof txId).toBe("string");
        expect(txId.startsWith("tx-")).toBe(true);
    });

    it("recover scans for incomplete transactions", async () => {
        mockShowMaxPrepared("100");

        const { Pool } = await import("pg");
        const pool = new Pool();

        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });
        await coordinator.initialize();

        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

        const result = await coordinator.recover();
        expect(result.recovered).toBe(0);
        expect(result.decisions).toEqual([]);
    });

    it("getMetrics returns coordinator metrics", async () => {
        mockShowMaxPrepared("100");

        const { Pool } = await import("pg");
        const pool = new Pool();

        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });
        await coordinator.initialize();

        mockQuery.mockResolvedValueOnce({
            rows: [{ status: "active", cnt: "2", oldest: new Date().toISOString() }],
            rowCount: 1,
        });

        const metrics = await coordinator.getMetrics();
        expect(metrics.active).toBe(2);
        expect(metrics.oldestActiveAgeMs).toBeGreaterThanOrEqual(0);
    });

    it("getStatus returns null for nonexistent transaction", async () => {
        mockShowMaxPrepared("100");

        const { Pool } = await import("pg");
        const pool = new Pool();

        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });
        await coordinator.initialize();

        mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

        const status = await coordinator.getStatus("nonexistent");
        expect(status).toBeNull();
    });
});
