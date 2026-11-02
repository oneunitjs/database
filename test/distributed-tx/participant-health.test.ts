import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
    ParticipantHealthChecker,
    defaultParticipantHealthLogger,
} from "../../src/distributed-tx/participant-health.js";

function makeMockClient(shouldFail = false) {
    return {
        query: vi.fn().mockImplementation(async () => {
            if (shouldFail) throw new Error("health check failed");
            return { rows: [{ "?column?": 1 }] };
        }),
    };
}

describe("ParticipantHealthChecker", () => {
    let checker: ParticipantHealthChecker;

    beforeEach(() => {
        checker = new ParticipantHealthChecker(
            new Map([
                ["node-1", { shardId: "s1", client: makeMockClient() }],
                ["node-2", { shardId: "s2", client: makeMockClient(true) }],
            ]),
            defaultParticipantHealthLogger,
        );
    });

    afterEach(() => {
        checker.stop();
        vi.restoreAllMocks();
    });

    it("initial status is healthy for all participants", () => {
        expect(checker.getStatus("node-1")?.healthy).toBe(true);
        expect(checker.getStatus("node-2")?.healthy).toBe(true);
        expect(checker.getStatus("node-1")?.consecutiveFailures).toBe(0);
    });

    it("checkParticipant marks success", async () => {
        await checker.checkParticipant("node-1", { shardId: "s1", client: makeMockClient() });
        const s = checker.getStatus("node-1");
        expect(s?.healthy).toBe(true);
        expect(s?.consecutiveSuccesses).toBe(1);
        expect(s?.consecutiveFailures).toBe(0);
        expect(s?.lastCheckTime).toBeInstanceOf(Date);
        expect(s?.error).toBeUndefined();
    });

    it("checkParticipant marks failure", async () => {
        await checker.checkParticipant("node-2", { shardId: "s2", client: makeMockClient(true) });
        const s = checker.getStatus("node-2");
        expect(s?.healthy).toBe(false);
        expect(s?.consecutiveFailures).toBe(1);
        expect(s?.error).toContain("health check failed");
    });

    it("isHealthy returns false for unhealthy node", async () => {
        await checker.checkParticipant("node-2", { shardId: "s2", client: makeMockClient(true) });
        expect(checker.isHealthy("node-2")).toBe(false);
        expect(checker.isHealthy("node-1")).toBe(true);
        expect(checker.isHealthy("node-unknown")).toBe(true);
    });

    it("getUnhealthyParticipants returns unhealthy nodes", async () => {
        await checker.checkParticipant("node-2", { shardId: "s2", client: makeMockClient(true) });
        expect(checker.getUnhealthyParticipants()).toContain("node-2");
        expect(checker.getUnhealthyParticipants()).not.toContain("node-1");
    });

    it("getAllStatuses returns all tracked nodes", () => {
        const all = checker.getAllStatuses();
        expect(all.size).toBe(2);
        expect(all.has("node-1")).toBe(true);
        expect(all.has("node-2")).toBe(true);
    });

    it("defaultParticipantHealthLogger is a working no-op logger", () => {
        expect(() => {
            defaultParticipantHealthLogger.debug({ x: 1 }, "msg");
            defaultParticipantHealthLogger.info({ x: 1 }, "msg");
            defaultParticipantHealthLogger.warn({ x: 1 }, "msg");
            defaultParticipantHealthLogger.error({ x: 1 }, "msg");
        }).not.toThrow();
    });
});
