import { describe, it, expect, vi, beforeEach } from "vitest";
import {
    HealthCheckTracker,
    DEFAULT_FAIL_OVER_CONFIG,
} from "../../src/failover/health-check-tracker.js";

const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
};

describe("HealthCheckTracker", () => {
    let tracker: HealthCheckTracker;

    beforeEach(() => {
        tracker = new HealthCheckTracker(DEFAULT_FAIL_OVER_CONFIG, logger);
    });

    it("createStatus returns healthy initial status", () => {
        const status = tracker.getStatus("node-1");
        expect(status).toBeUndefined();
        tracker.recordSuccess("node-1");
        const s = tracker.getStatus("node-1");
        expect(s?.nodeId).toBe("node-1");
        expect(s?.currentStatus).toBe("healthy");
        expect(s?.consecutiveFailures).toBe(0);
        expect(s?.consecutiveSuccesses).toBe(1);
    });

    it("recordFailure increments consecutive failures", () => {
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        const s = tracker.getStatus("node-1");
        expect(s?.consecutiveFailures).toBe(2);
        expect(s?.consecutiveSuccesses).toBe(0);
        expect(s?.lastFailureTime).not.toBeNull();
    });

    it("marks node unhealthy after consecutiveFailureThreshold", () => {
        // Default threshold is 3
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        let s = tracker.getStatus("node-1");
        expect(s?.currentStatus).toBe("degraded"); // >= threshold/2

        tracker.recordFailure("node-1");
        s = tracker.getStatus("node-1");
        expect(s?.currentStatus).toBe("unhealthy");
    });

    it("recordSuccess recovers degraded node to healthy after successThreshold", () => {
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        let s = tracker.getStatus("node-1");
        expect(s?.currentStatus).toBe("degraded");

        tracker.recordSuccess("node-1");
        tracker.recordSuccess("node-1");
        s = tracker.getStatus("node-1");
        expect(s?.currentStatus).toBe("healthy");
        expect(s?.consecutiveFailures).toBe(0);
        expect(s?.consecutiveSuccesses).toBe(2);
    });

    it("recordSuccess recovers unhealthy node after duration threshold met", () => {
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        let s = tracker.getStatus("node-1");
        expect(s?.currentStatus).toBe("unhealthy");

        // Advance time past unhealthyDurationThresholdMs (30s default)
        vi.useFakeTimers();
        vi.setSystemTime(new Date(Date.now() + 35_000));
        tracker.recordSuccess("node-1");
        tracker.recordSuccess("node-1");
        s = tracker.getStatus("node-1");
        expect(s?.currentStatus).toBe("healthy");
        vi.useRealTimers();
    });

    it("shouldInitiateFailover returns true when threshold and duration met", () => {
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        // Not enough duration yet
        expect(tracker.shouldInitiateFailover("node-1")).toBe(false);

        vi.useFakeTimers();
        vi.setSystemTime(new Date(Date.now() + 35_000));
        expect(tracker.shouldInitiateFailover("node-1")).toBe(true);
        vi.useRealTimers();
    });

    it("shouldInitiateFailover returns false for unknown node", () => {
        expect(tracker.shouldInitiateFailover("node-unknown")).toBe(false);
    });

    it("getEffectiveHealthCheckInterval returns base when healthy", () => {
        tracker.recordSuccess("node-1");
        expect(tracker.getEffectiveHealthCheckInterval("node-1")).toBe(5000);
    });

    it("getEffectiveHealthCheckInterval applies multiplier when unhealthy", () => {
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        tracker.recordFailure("node-1");
        // Default multiplier 0.5 * 5000 = 2500
        expect(tracker.getEffectiveHealthCheckInterval("node-1")).toBe(2500);
    });

    it("per-node threshold overrides work", () => {
        const config = {
            ...DEFAULT_FAIL_OVER_CONFIG,
            nodeThresholds: {
                "node-special": {
                    consecutiveFailures: 5,
                    unhealthyDurationMs: 10_000,
                },
            },
        };
        const specialTracker = new HealthCheckTracker(config, logger);
        // 3 failures (default threshold) should NOT mark unhealthy since special needs 5
        specialTracker.recordFailure("node-special");
        specialTracker.recordFailure("node-special");
        specialTracker.recordFailure("node-special");
        let s = specialTracker.getStatus("node-special");
        expect(s?.currentStatus).toBe("degraded"); // >= 5/2 = 2.5 -> 3
        expect(s?.currentStatus).not.toBe("unhealthy");

        specialTracker.recordFailure("node-special");
        specialTracker.recordFailure("node-special");
        s = specialTracker.getStatus("node-special");
        expect(s?.currentStatus).toBe("unhealthy");
    });

    it("getAllStatuses returns all tracked nodes", () => {
        tracker.recordSuccess("node-1");
        tracker.recordFailure("node-2");
        const all = tracker.getAllStatuses();
        expect(all.size).toBe(2);
        expect(all.has("node-1")).toBe(true);
        expect(all.has("node-2")).toBe(true);
    });
});
