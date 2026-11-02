import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    DistributedTransactionCoordinator,
    DistributedTransactionError,
    DistributedTxTimeoutError,
} from "../../src/distributed-tx/index.js";
import { RetryPolicy } from "../../src/distributed-tx/retry-policy.js";


const mockQuery = vi.fn();

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => ({
            query: mockQuery,
            connect: vi.fn().mockResolvedValue({
                query: mockQuery,
                release: vi.fn(),
                on: vi.fn(),
            }),
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

describe("DistributedTransactionCoordinator timeouts", () => {
    beforeEach(() => {
        resetMocks();
    });

    afterEach(() => {
        resetMocks();
    });

    function baseMock(): (sql: unknown) => Promise<{ rows: unknown[]; rowCount: number }> {
        return (sql: unknown) => {
            const s = typeof sql === "string" ? sql : String(sql);
            if (s.includes("SHOW max_prepared_transactions")) {
                return Promise.resolve({ rows: [{ max_prepared_transactions: 100 }], rowCount: 1 });
            }
            if (
                s.includes("CREATE TABLE IF NOT EXISTS distributed_transactions") ||
                s.includes("CREATE TABLE IF NOT EXISTS distributed_transaction_participants")
            ) {
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
                return Promise.resolve({ rows: [], rowCount: 0 });
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
        };
    }

    it("commit throws DistributedTxTimeoutError when global timeout exceeded", async () => {
        mockQuery.mockImplementation(baseMock());
        const oldCreatedAt = new Date(Date.now() - 400_000).toISOString();
        mockQuery.mockImplementation((sql: unknown) => {
            const s = typeof sql === "string" ? sql : String(sql);
            if (s.includes("SHOW max_prepared_transactions")) {
                return Promise.resolve({ rows: [{ max_prepared_transactions: 100 }], rowCount: 1 });
            }
            if (
                s.includes("CREATE TABLE IF NOT EXISTS distributed_transactions") ||
                s.includes("CREATE TABLE IF NOT EXISTS distributed_transaction_participants")
            ) {
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
                return Promise.resolve({ rows: [], rowCount: 0 });
            }
            if (s.includes("SELECT * FROM distributed_transactions WHERE transaction_id")) {
                return Promise.resolve({
                    rows: [
                        {
                            transaction_id: "tx-1",
                            status: "prepared",
                            created_at: oldCreatedAt,
                            updated_at: oldCreatedAt,
                            coordinator_id: "coord",
                            participants: [],
                            global_decision: null,
                            decision_persisted_at: null,
                            recovery_metadata: null,
                        },
                    ],
                    rowCount: 1,
                });
            }
            if (s.includes("SELECT * FROM distributed_transaction_participants WHERE transaction_id")) {
                return Promise.resolve({ rows: [], rowCount: 0 });
            }
            return Promise.resolve({ rows: [], rowCount: 0 });
        });

        const { Pool } = await import("pg");
        const pool = new Pool();
        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            logger: { debug: () => { }, info: () => { }, warn: () => { }, error: () => { } },
        });
        await coordinator.initialize();

        await expect(coordinator.commit("tx-1")).rejects.toBeInstanceOf(DistributedTxTimeoutError);
    });

    it("prepare throws DistributedTxTimeoutError when prepare timeout exceeded", async () => {
        mockQuery.mockImplementation(baseMock());
        const { Pool } = await import("pg");
        const pool = new Pool();
        const coordinator = new DistributedTransactionCoordinator({
            coordinationPool: pool,
            timeouts: { prepareTimeoutMs: 10 },
            retryPolicy: new RetryPolicy({ maxRetries: 0 }),
            logger: { debug: () => { }, info: () => { }, warn: () => { }, error: () => { } },
        });
        await coordinator.initialize();

        const txId = await coordinator.begin();
        const client = await coordinator.addParticipant(txId, "shard-1", pool);
        client.query = vi.fn().mockImplementation((text: unknown) => {
            const s = typeof text === "string" ? text : String(text);
            if (s.includes("PREPARE TRANSACTION")) {
                return new Promise(() => { }); // hangs
            }
            return mockQuery(text);
        });

        await expect(coordinator.prepare(txId)).rejects.toMatchObject({
            name: "DistributedTransactionError",
            cause: expect.any(DistributedTxTimeoutError),
        });
    });
});
