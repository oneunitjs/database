import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    RetryPolicy,
    DEFAULT_RETRY_POLICY_OPTIONS,
    defaultIsRetryable,
} from "../../src/distributed-tx/retry-policy.js";
import { DistributedTxTimeoutError } from "../../src/distributed-tx/distributed-transaction-coordinator.js";

describe("defaultIsRetryable", () => {
    it("returns true for transient DistributedTransactionError", () => {
        const err = new DistributedTxTimeoutError("timeout", "tx-1");
        expect(defaultIsRetryable(err)).toBe(true);
    });

    it("returns false for non-transient error", () => {
        const err = new Error("boom") as any;
        err.transient = false;
        expect(defaultIsRetryable(err)).toBe(false);
    });

    it("returns true for timeout-like messages", () => {
        expect(defaultIsRetryable(new Error("operation timed out"))).toBe(true);
        expect(defaultIsRetryable(new Error("connection refused"))).toBe(true);
        expect(defaultIsRetryable(new Error("ECONNRESET"))).toBe(true);
    });

    it("returns false for unrelated errors", () => {
        expect(defaultIsRetryable(new Error("permission denied"))).toBe(false);
        expect(defaultIsRetryable(new Error("syntax error"))).toBe(false);
    });
});

describe("RetryPolicy", () => {
    let policy: RetryPolicy;

    beforeEach(() => {
        policy = new RetryPolicy({
            maxRetries: 3,
            initialDelayMs: 5,
            maxDelayMs: 1000,
            backoffMultiplier: 2,
        });
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("returns result on first success without retrying", async () => {
        const fn = vi.fn().mockResolvedValue("ok");
        const result = await policy.execute(fn);
        expect(result).toBe("ok");
        expect(fn).toHaveBeenCalledTimes(1);
    });

    it("retries on transient error then succeeds", async () => {
        const fn = vi.fn()
            .mockRejectedValueOnce(new DistributedTxTimeoutError("t1", "tx"))
            .mockRejectedValueOnce(new DistributedTxTimeoutError("t2", "tx"))
            .mockResolvedValue("ok");
        const result = await policy.execute(fn);
        expect(result).toBe("ok");
        expect(fn).toHaveBeenCalledTimes(3);
    });

    it("does not retry non-transient errors", async () => {
        const fn = vi.fn().mockRejectedValue(new Error("permission denied"));
        await expect(policy.execute(fn)).rejects.toThrow("permission denied");
        expect(fn).toHaveBeenCalledTimes(1);
    });

    it("throws last error after max retries exhausted", async () => {
        const err = new DistributedTxTimeoutError("always", "tx");
        const fn = vi.fn().mockRejectedValue(err);
        await expect(policy.execute(fn)).rejects.toThrow("always");
        expect(fn).toHaveBeenCalledTimes(4);
    });

    it("applies exponential backoff capped at maxDelayMs", async () => {
        const delays: number[] = [];
        const origSetTimeout = globalThis.setTimeout;
        vi.spyOn(globalThis, "setTimeout").mockImplementation((cb: any, ms: number) => {
            delays.push(ms);
            return origSetTimeout(cb, 0);
        });
        const err = new DistributedTxTimeoutError("t", "tx");
        const fn = vi.fn().mockRejectedValue(err);
        try {
            await policy.execute(fn);
        } catch {}
        expect(delays).toEqual([5, 10, 20]);
    });

    it("uses default options when none provided", async () => {
        const defaultPolicy = new RetryPolicy();
        const fn = vi.fn().mockRejectedValue(new Error("permission denied"));
        await expect(defaultPolicy.execute(fn)).rejects.toThrow("permission denied");
        expect(fn).toHaveBeenCalledTimes(1);
    });
});
