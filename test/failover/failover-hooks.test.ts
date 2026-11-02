import { describe, it, expect, vi, beforeEach } from "vitest";
import {
    FailoverHookRunner,
    defaultHookLogger,
} from "../../src/failover/failover-hooks.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

describe("FailoverHookRunner", () => {
    let runner: FailoverHookRunner;

    beforeEach(() => {
        runner = new FailoverHookRunner(logger);
    });

    it("invoke does nothing when no hooks registered", async () => {
        await expect(
            runner.invoke("beforeStateChange", {
                shardId: "shard-1",
                manager: {
                    getCurrentState: () => "healthy",
                    getCurrentPrimary: () => undefined,
                    getCurrentGeneration: () => 1,
                    getShardId: () => "shard-1",
                },
                data: { fromState: { state: "healthy" }, toState: { state: "suspect" }, reason: "test" },
            }),
        ).resolves.toBeUndefined();
    });

    it("registerHook and invoke calls the hook", async () => {
        const hook = vi.fn();
        runner.registerHook("afterStateChange", hook);
        await runner.invoke("afterStateChange", {
            shardId: "shard-1",
            manager: {
                getCurrentState: () => "suspect",
                getCurrentPrimary: () => undefined,
                getCurrentGeneration: () => 1,
                getShardId: () => "shard-1",
            },
            data: { fromState: { state: "healthy" }, toState: { state: "suspect" }, reason: "test" },
        });
        expect(hook).toHaveBeenCalledTimes(1);
        const ctx = hook.mock.calls[0][0];
        expect(ctx.type).toBe("afterStateChange");
        expect(ctx.shardId).toBe("shard-1");
        expect(ctx.timestamp).toBeInstanceOf(Date);
    });

    it("hooks registered for different types are independent", async () => {
        const hookA = vi.fn();
        const hookB = vi.fn();
        runner.registerHook("beforeFailover", hookA);
        runner.registerHook("afterFailover", hookB);

        await runner.invoke("beforeFailover", {
            shardId: "shard-1",
            manager: {
                getCurrentState: () => "healthy",
                getCurrentPrimary: () => undefined,
                getCurrentGeneration: () => 1,
                getShardId: () => "shard-1",
            },
            data: { currentPrimary: undefined, reason: "test" },
        });
        expect(hookA).toHaveBeenCalledTimes(1);
        expect(hookB).not.toHaveBeenCalled();
    });

    it("multiple hooks for same type all invoked", async () => {
        const hook1 = vi.fn();
        const hook2 = vi.fn();
        runner.registerHook("afterStateChange", hook1);
        runner.registerHook("afterStateChange", hook2);
        await runner.invoke("afterStateChange", {
            shardId: "shard-1",
            manager: {
                getCurrentState: () => "suspect",
                getCurrentPrimary: () => undefined,
                getCurrentGeneration: () => 1,
                getShardId: () => "shard-1",
            },
            data: { fromState: { state: "healthy" }, toState: { state: "suspect" }, reason: "test" },
        });
        expect(hook1).toHaveBeenCalledTimes(1);
        expect(hook2).toHaveBeenCalledTimes(1);
    });

    it("hook errors are caught and logged, not thrown", async () => {
        const failingHook = vi.fn().mockRejectedValue(new Error("hook boom"));
        const workingHook = vi.fn();
        runner.registerHook("afterStateChange", failingHook);
        runner.registerHook("afterStateChange", workingHook);

        // Should not throw
        await expect(
            runner.invoke("afterStateChange", {
                shardId: "shard-1",
                manager: {
                    getCurrentState: () => "suspect",
                    getCurrentPrimary: () => undefined,
                    getCurrentGeneration: () => 1,
                    getShardId: () => "shard-1",
                },
                data: { fromState: { state: "healthy" }, toState: { state: "suspect" }, reason: "test" },
            }),
        ).resolves.toBeUndefined();

        expect(failingHook).toHaveBeenCalledTimes(1);
        expect(workingHook).toHaveBeenCalledTimes(1);
        expect(logger.warn).toHaveBeenCalled();
    });

    it("unregisterHook removes a hook", async () => {
        const hook = vi.fn();
        runner.registerHook("afterStateChange", hook);
        expect(runner.getHookCount("afterStateChange")).toBe(1);
        const removed = runner.unregisterHook("afterStateChange", hook);
        expect(removed).toBe(true);
        expect(runner.getHookCount("afterStateChange")).toBe(0);
    });

    it("unregisterHook returns false for unknown hook", () => {
        expect(runner.unregisterHook("afterStateChange", vi.fn())).toBe(false);
    });

    it("clear removes all hooks", () => {
        runner.registerHook("beforeStateChange", vi.fn());
        runner.registerHook("afterStateChange", vi.fn());
        runner.clear();
        expect(runner.getHookCount("beforeStateChange")).toBe(0);
        expect(runner.getHookCount("afterStateChange")).toBe(0);
    });

    it("defaultHookLogger is a working no-op logger", () => {
        expect(() => {
            defaultHookLogger.debug({ x: 1 }, "msg");
            defaultHookLogger.info({ x: 1 }, "msg");
            defaultHookLogger.warn({ x: 1 }, "msg");
            defaultHookLogger.error({ x: 1 }, "msg");
        }).not.toThrow();
    });
});
