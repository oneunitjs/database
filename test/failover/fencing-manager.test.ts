import { describe, it, expect, vi, beforeEach } from "vitest";
import { FencingManager } from "../../src/failover/fencing-manager.js";
import type {
    FencingProvider,
    FencingOptions,
    FencingStatus,
} from "../../src/failover/fencing.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

function makeMockProvider(name: string): FencingProvider {
    return {
        activate: vi.fn().mockResolvedValue(undefined),
        deactivate: vi.fn().mockResolvedValue(undefined),
        isFenced: vi.fn().mockResolvedValue(false),
        check: vi.fn().mockResolvedValue(undefined),
        getFencedNodes: vi.fn().mockResolvedValue([]),
        getStatus: vi.fn().mockResolvedValue({ nodeId: name, fenced: false }),
        cleanup: vi.fn().mockResolvedValue(undefined),
    };
}

describe("FencingManager", () => {
    let manager: FencingManager;
    let defaultProvider: FencingProvider;

    beforeEach(() => {
        defaultProvider = makeMockProvider("default");
        manager = new FencingManager(defaultProvider, logger);
    });

    it("registers default provider under name 'default'", () => {
        expect(manager.use("default")).toBe(defaultProvider);
        expect(manager.use("nonexistent")).toBe(defaultProvider);
    });

    it("registerProvider adds a named provider and use returns it", () => {
        const custom = makeMockProvider("custom");
        manager.registerProvider("custom", custom);
        expect(manager.use("custom")).toBe(custom);
        expect(manager.use("default")).toBe(defaultProvider);
    });

    it("use falls back to default for unknown provider name", () => {
        expect(manager.use("missing")).toBe(defaultProvider);
    });

    it("activateAll calls all providers including default and collects results", async () => {
        const custom = makeMockProvider("custom");
        manager.registerProvider("custom", custom);

        const results = await manager.activateAll("node-1", "failover", { ttlMs: 1000 });
        expect(results.length).toBe(2);
        expect(results[0]).toEqual({ nodeId: "node-1", success: true });
        expect(defaultProvider.activate).toHaveBeenCalledTimes(1);
        expect(custom.activate).toHaveBeenCalledTimes(1);
    });

    it("activateAll records failure when a non-default provider throws", async () => {
        const failing = makeMockProvider("failing");
        (failing.activate as any).mockRejectedValueOnce(new Error("boom"));
        manager.registerProvider("failing", failing);

        const results = await manager.activateAll("node-1", "failover");
        const failingResult = results.find((r) => r.success === false);
        expect(failingResult).toBeDefined();
        expect(failingResult.error).toBe("boom");
        expect(logger.error).toHaveBeenCalled();
    });

    it("deactivateAll calls deactivate on all providers", async () => {
        const custom = makeMockProvider("custom");
        manager.registerProvider("custom", custom);

        await manager.deactivateAll("node-1");
        expect(defaultProvider.deactivate).toHaveBeenCalledTimes(1);
        expect(custom.deactivate).toHaveBeenCalledTimes(1);
    });

    it("deactivateAll continues when a provider throws", async () => {
        const failing = makeMockProvider("failing");
        (failing.deactivate as any).mockRejectedValueOnce(new Error("boom"));
        manager.registerProvider("failing", failing);

        await expect(manager.deactivateAll("node-1")).resolves.toBeUndefined();
        expect(logger.error).toHaveBeenCalled();
    });

    it("check delegates to default provider", async () => {
        await manager.check("node-1");
        expect(defaultProvider.check).toHaveBeenCalledTimes(1);
        expect(defaultProvider.check).toHaveBeenCalledWith("node-1");
    });

    it("isFenced delegates to default provider", async () => {
        (defaultProvider.isFenced as any).mockResolvedValueOnce(true);
        expect(await manager.isFenced("node-1")).toBe(true);
    });

    it("getFencedNodes delegates to default provider", async () => {
        (defaultProvider.getFencedNodes as any).mockResolvedValueOnce(["a", "b"]);
        expect(await manager.getFencedNodes()).toEqual(["a", "b"]);
    });

    it("getStatus delegates to default provider", async () => {
        const status: FencingStatus = {
            nodeId: "node-1",
            fenced: true,
            reason: "failover",
            priority: 5,
            activatedAt: new Date(),
        };
        (defaultProvider.getStatus as any).mockResolvedValueOnce(status);
        const result = await manager.getStatus("node-1");
        expect(result).toEqual(status);
    });

    it("cleanup calls cleanup on all providers", async () => {
        const custom = makeMockProvider("custom");
        manager.registerProvider("custom", custom);

        await manager.cleanup("node-1");
        expect(defaultProvider.cleanup).toHaveBeenCalledTimes(1);
        expect(custom.cleanup).toHaveBeenCalledTimes(1);
    });

    it("cleanup continues when a provider throws", async () => {
        const failing = makeMockProvider("failing");
        (failing.cleanup as any).mockRejectedValueOnce(new Error("boom"));
        manager.registerProvider("failing", failing);

        await expect(manager.cleanup("node-1")).resolves.toBeUndefined();
        expect(logger.error).toHaveBeenCalled();
    });
});
