import { describe, it, expect } from "vitest";

import { createDatabase } from "../../src/index.js";
import { MaxPreparedTransactionsError } from "../../src/distributed-tx/index.js";
import { ShardingDisabledError } from "../../src/sharding/errors.js";

describe("Distributed transaction API integration", () => {
    it("distributedTransaction is always available as a function", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        // distributedTransaction should exist even without sharding
        expect(typeof db.sharding.distributedTransaction).toBe("function");

        // Without sharding, it throws ShardingDisabledError
        await expect(
            db.sharding.distributedTransaction(async (tx) => {
                await tx.execute("key-0", "SELECT 1");
            }),
        ).rejects.toBeInstanceOf(ShardingDisabledError);

        await db.shutdown();
    });

    it("distributedTransaction exists when sharding is enabled", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    { id: "shard-0", connection: { connectionString: "postgresql://localhost:5432/app" } },
                    { id: "shard-1", connection: { connectionString: "postgresql://localhost:5432/app" } },
                ],
            },
        });

        // distributedTransaction should exist (as a function)
        expect(typeof db.sharding.distributedTransaction).toBe("function");

        // It should throw because max_prepared_transactions is 0
        // (fail closed — no silent fallback to non-atomic transactions)
        await expect(
            db.sharding.distributedTransaction(async (tx) => {
                await tx.execute("key-0", "SELECT 1");
            }),
        ).rejects.toThrow();

        await db.shutdown();
    });

    it("distributedTx coordinator is available", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    { id: "shard-0", connection: { connectionString: "postgresql://localhost:5432/app" } },
                ],
            },
        });

        // distributedTx should exist
        expect(db.sharding.distributedTx).toBeDefined();

        await db.shutdown();
    });
});
