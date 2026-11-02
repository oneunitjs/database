import { describe, it, expect } from "vitest";
import {
    dimensionProgress,
    ThroughputTracker,
    ReshardingProgressCalculator,
} from "../../src/resharding/resharding-tracker.ts";
import { CopyMetricsTracker } from "../../src/resharding/resharding-metrics.ts";

function status(overrides = {}) {
    return {
        migrationId: "m1",
        status: "copying",
        version: 1,
        sourceRoutingVersion: 1,
        targetRoutingVersion: 2,
        createdAt: "t",
        updatedAt: "t",
        unitsTotal: 10,
        unitsCompleted: 4,
        unitsFailed: 0,
        rowsCopied: 400,
        rowsValidated: 0,
        changeLogLag: 0,
        cutoverIntent: false,
        cutoverCommitted: false,
        ...overrides,
    };
}

describe("resharding progress", () => {
    it("treats empty totals as 0% unless work already completed", () => {
        expect(dimensionProgress(0, 0)).toEqual({
            completed: 0,
            total: 0,
            remaining: 0,
            percentage: 0,
        });
        expect(dimensionProgress(5, 0).percentage).toBe(100);
        expect(dimensionProgress(-3, 10)).toEqual({
            completed: 0,
            total: 10,
            remaining: 10,
            percentage: 0,
        });
        expect(dimensionProgress(Number.NaN, Number.POSITIVE_INFINITY).percentage).toBe(0);
    });

    it("clamps percentage to 0-100 and rounds to two decimals", () => {
        expect(dimensionProgress(1, 3).percentage).toBe(33.33);
        expect(dimensionProgress(12, 10).percentage).toBe(100);
    });

    it("does not estimate remaining until two throughput samples exist", () => {
        const tracker = new ThroughputTracker(30_000);
        tracker.record({ timestampMs: 1_000, rowsCopied: 10, bytesCopied: 100, unitsCompleted: 1 });
        expect(tracker.snapshot(1_000).rowsPerSecond).toBe(0);
        expect(tracker.estimateRemainingMs({ rows: 90, bytes: 900, units: 9 }, 1_000).remainingMs).toBeNull();
    });

    it("estimates remaining from the slowest positive dimension", () => {
        const tracker = new ThroughputTracker(30_000);
        tracker.record({ timestampMs: 0, rowsCopied: 0, bytesCopied: 0, unitsCompleted: 0 });
        tracker.record({ timestampMs: 1_000, rowsCopied: 100, bytesCopied: 1_000, unitsCompleted: 2 });
        const estimate = tracker.estimateRemainingMs({ rows: 100, bytes: 1_000, units: 8 }, 1_000);
        expect(estimate.remainingMs).toBe(4_000);
        expect(estimate.confidence).toBeGreaterThan(0);
    });

    it("drops samples outside the throughput window", () => {
        const tracker = new ThroughputTracker(1_000);
        tracker.record({ timestampMs: 0, rowsCopied: 0, bytesCopied: 0, unitsCompleted: 0 });
        tracker.record({ timestampMs: 2_000, rowsCopied: 50, bytesCopied: 50, unitsCompleted: 1 });
        expect(tracker.snapshot(2_000).sampleCount).toBe(1);
    });

    it("computes weighted overall progress from unit/row/byte dimensions", () => {
        const calc = new ReshardingProgressCalculator();
        const progress = calc.calculate(status(), {}, 1_000);
        expect(progress.units.percentage).toBe(40);
        expect(progress.overallPercentage).toBeGreaterThan(0);
        expect(progress.overallPercentage).toBeLessThanOrEqual(100);
        expect(progress.migrationId).toBe("m1");
    });
});

describe("copy metrics", () => {
    it("ignores completeBatch when no batch is in flight", () => {
        const tracker = new CopyMetricsTracker();
        tracker.completeBatch(true);
        const snap = tracker.snapshot();
        expect(snap.batchesCompleted).toBe(0);
        expect(snap.currentBatch).toBeNull();
    });

    it("counts only successful batches toward copied rows", () => {
        const tracker = new CopyMetricsTracker();
        tracker.startBatch(10, 100, 0);
        tracker.completeBatch(true, undefined, 10);
        tracker.startBatch(5, 50, 20);
        tracker.completeBatch(false, "timeout", 40);
        tracker.recordRetry();
        tracker.recordUnitCompleted();
        const snap = tracker.snapshot(40);
        expect(snap.rowsCopied).toBe(10);
        expect(snap.bytesCopied).toBe(100);
        expect(snap.batchesCompleted).toBe(1);
        expect(snap.batchesFailed).toBe(1);
        expect(snap.retries).toBe(1);
        expect(snap.unitsCopied).toBe(1);
        expect(snap.unitsPerSecond).toBeGreaterThan(0);
        expect(snap.errorsByType.timeout).toBe(1);
        expect(snap.latencyMs.avg).toBe(15);
    });

    it("exposes the in-flight batch until it completes", () => {
        const tracker = new CopyMetricsTracker();
        tracker.startBatch(3, 30, 5);
        expect(tracker.snapshot(6).currentBatch).toEqual({ startedAtMs: 5, rows: 3, bytes: 30 });
        tracker.completeBatch(true, undefined, 15);
        expect(tracker.snapshot(15).currentBatch).toBeNull();
    });
});
