import { describe, it, expect } from "vitest";
import { TransactionSemaphore } from "../../src/distributed-tx/transaction-semaphore.ts";

describe("TransactionSemaphore", () => {
    it("acquires/releases", async () => {
        const sem = new TransactionSemaphore({ maxConcurrent: 1, timeoutMs: 100 });
        const release = await sem.acquire("tx-1");
        expect(sem.getActiveCount()).toBe(1);
        expect(sem.isFull()).toBe(true);
        release();
        expect(sem.getActiveCount()).toBe(0);
    });

    it("queues when full", async () => {
        const sem = new TransactionSemaphore({ maxConcurrent: 1, timeoutMs: 1000 });
        const release = await sem.acquire("tx-1");
        let resolved = false;
        const p = sem.acquire("tx-2").then((r) => {
            resolved = true;
            r();
        });
        expect(sem.getQueueLength()).toBe(1);
        release();
        await p;
        expect(resolved).toBe(true);
    });
});
