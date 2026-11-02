import { describe, it, expect, vi } from "vitest";
import { TransactionSemaphore } from "../../src/distributed-tx/transaction-semaphore.ts";
import { CircuitBreaker } from "../../src/distributed-tx/circuit-breaker.ts";
import { PoolMonitor } from "../../src/client/pool-monitor.ts";
import { calculateRecommendedPoolSize } from "../../src/client/pool-size-calculator.ts";

describe("connection throttling components", () => {
    it("semaphore acquires and reports active count", async () => {
        const sem = new TransactionSemaphore({ maxConcurrent: 2, timeoutMs: 100 });
        const release = await sem.acquire("tx-1");
        expect(sem.getActiveCount()).toBe(1);
        release();
        expect(sem.getActiveCount()).toBe(0);
    });
    it("pool monitor stats", () => {
        const mockPool: any = { totalCount: 5, idleCount: 2, waitingCount: 1, options: { max: 10 } };
        const monitor = new PoolMonitor(new Map([["p", mockPool]]));
        monitor.start();
        monitor.stop();
        const stats = monitor.getStats("p");
        expect(stats).toBeTruthy();
        expect(monitor.getAllStats()).toBeInstanceOf(Array);
    });
    it("circuit breaker initial state", () => {
        const mockPool: any = { totalCount: 1, idleCount: 1, waitingCount: 0, options: { max: 10 } };
        const monitor = new PoolMonitor(new Map([["p", mockPool]]));
        const cb = new CircuitBreaker(monitor);
        expect(cb.getState()).toBe("CLOSED");
        expect(cb.canExecute()).toBe(true);
        cb.forceOpen();
        expect(cb.getState()).toBe("OPEN");
        cb.forceClose();
        expect(cb.getState()).toBe("CLOSED");
    });
    it("pool size calculator", () => {
        expect(calculateRecommendedPoolSize(2, 3, 1.5)).toBe(9);
    });
});
