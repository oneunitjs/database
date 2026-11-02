import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    DistributedTxHookRunner,
    defaultDistributedTxHookLogger,
} from "../../src/distributed-tx/distributed-tx-hooks.js";

describe("DistributedTxHookRunner", () => {
    let runner: DistributedTxHookRunner;

    beforeEach(() => {
        runner = new DistributedTxHookRunner(defaultDistributedTxHookLogger);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("invoke does nothing when no hooks registered", async () => {
        await expect(
            runner.invoke("beforePrepare", {
                transactionId: "tx-1",
                coordinator: {
                    getCurrentState: () => "active",
                    getCurrentTransactionId: () => "tx-1",
                    getParticipantCount: () => 0,
                },
                data: { participants: [] },
            }),
        ).resolves.toBeUndefined();
    });

    it("registerHook and invoke calls the hook", async () => {
        const hook = vi.fn();
        runner.registerHook("afterPrepare", hook);
        await runner.invoke("afterPrepare", {
            transactionId: "tx-1",
            coordinator: {
                getCurrentState: () => "prepared",
                getCurrentTransactionId: () => "tx-1",
                getParticipantCount: () => 2,
            },
            data: { preparedTransactionIds: ["pg_tx-1_s1"], durationMs: 100 },
        });
        expect(hook).toHaveBeenCalledTimes(1);
        const ctx = hook.mock.calls[0][0];
        expect(ctx.type).toBe("afterPrepare");
        expect(ctx.transactionId).toBe("tx-1");
        expect(ctx.timestamp).toBeInstanceOf(Date);
        expect(ctx.data.preparedTransactionIds).toEqual(["pg_tx-1_s1"]);
    });

    it("hooks registered for different types are independent", async () => {
        const hookA = vi.fn();
        const hookB = vi.fn();
        runner.registerHook("beforeCommit", hookA);
        runner.registerHook("afterCommit", hookB);
        await runner.invoke("beforeCommit", {
            transactionId: "tx-1",
            coordinator: {
                getCurrentState: () => "prepared",
                getCurrentTransactionId: () => "tx-1",
                getParticipantCount: () => 1,
            },
            data: { participantCount: 1 },
        });
        expect(hookA).toHaveBeenCalledTimes(1);
        expect(hookB).not.toHaveBeenCalled();
    });

    it("multiple hooks for same type all invoked", async () => {
        const hook1 = vi.fn();
        const hook2 = vi.fn();
        runner.registerHook("onTimeout", hook1);
        runner.registerHook("onTimeout", hook2);
        await runner.invoke("onTimeout", {
            transactionId: "tx-1",
            coordinator: {
                getCurrentState: () => "preparing",
                getCurrentTransactionId: () => "tx-1",
                getParticipantCount: () => 1,
            },
            data: { phase: "prepare", timeoutMs: 5000 },
        });
        expect(hook1).toHaveBeenCalledTimes(1);
        expect(hook2).toHaveBeenCalledTimes(1);
    });

    it("hook errors are caught and logged, not thrown", async () => {
        const failingHook = vi.fn().mockRejectedValue(new Error("hook boom"));
        const workingHook = vi.fn();
        runner.registerHook("afterStateChange", failingHook);
        runner.registerHook("afterStateChange", workingHook);
        await expect(
            runner.invoke("afterStateChange", {
                transactionId: "tx-1",
                coordinator: {
                    getCurrentState: () => "committed",
                    getCurrentTransactionId: () => "tx-1",
                    getParticipantCount: () => 1,
                },
                data: { fromState: "preparing", toState: "committed" },
            }),
        ).resolves.toBeUndefined();
        expect(failingHook).toHaveBeenCalledTimes(1);
        expect(workingHook).toHaveBeenCalledTimes(1);
    });

    it("unregisterHook removes a hook", async () => {
        const hook = vi.fn();
        runner.registerHook("beforeAbort", hook);
        expect(runner.getHookCount("beforeAbort")).toBe(1);
        const removed = runner.unregisterHook("beforeAbort", hook);
        expect(removed).toBe(true);
        expect(runner.getHookCount("beforeAbort")).toBe(0);
    });

    it("unregisterHook returns false for unknown hook", () => {
        expect(runner.unregisterHook("beforeAbort", vi.fn())).toBe(false);
    });

    it("clear removes all hooks", () => {
        runner.registerHook("prepareFailed", vi.fn());
        runner.registerHook("commitFailed", vi.fn());
        runner.clear();
        expect(runner.getHookCount("prepareFailed")).toBe(0);
        expect(runner.getHookCount("commitFailed")).toBe(0);
    });

    it("defaultDistributedTxHookLogger is a working no-op logger", () => {
        expect(() => {
            defaultDistributedTxHookLogger.debug({ x: 1 }, "msg");
            defaultDistributedTxHookLogger.info({ x: 1 }, "msg");
            defaultDistributedTxHookLogger.warn({ x: 1 }, "msg");
            defaultDistributedTxHookLogger.error({ x: 1 }, "msg");
        }).not.toThrow();
    });
});
