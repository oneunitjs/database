import type { ReshardingStatus, ReshardingStatusInfo } from "./resharding-types.js";

export interface DimensionProgress {
    completed: number;
    total: number;
    remaining: number;
    percentage: number;
}

export interface ReshardingProgress {
    migrationId: string;
    status: ReshardingStatus;
    overallPercentage: number;
    units: DimensionProgress;
    rows: DimensionProgress;
    bytes: DimensionProgress;
    estimatedRemainingMs: number | null;
    estimatedRemainingConfidence: number;
    throughput: ThroughputSnapshot;
    updatedAt: string;
}

export interface ThroughputSample {
    timestampMs: number;
    rowsCopied: number;
    bytesCopied: number;
    unitsCompleted: number;
}

export interface ThroughputSnapshot {
    rowsPerSecond: number;
    bytesPerSecond: number;
    unitsPerSecond: number;
    sampleCount: number;
    windowMs: number;
}

export interface ProgressWeights {
    units: number;
    rows: number;
    bytes: number;
}

export const DEFAULT_PROGRESS_WEIGHTS: ProgressWeights = {
    units: 1,
    rows: 1,
    bytes: 0.5,
};

export const DEFAULT_THROUGHPUT_WINDOW_MS = 30_000;

function clampPercentage(value: number): number {
    if (!Number.isFinite(value)) {
        return 0;
    }
    return Math.min(100, Math.max(0, Math.round(value * 100) / 100));
}

export function dimensionProgress(completed: number, total: number): DimensionProgress {
    const safeCompleted = Number.isFinite(completed) && completed > 0 ? completed : 0;
    const safeTotal = Number.isFinite(total) && total > 0 ? total : 0;
    if (safeTotal === 0) {
        return {
            completed: safeCompleted,
            total: 0,
            remaining: 0,
            percentage: safeCompleted > 0 ? 100 : 0,
        };
    }
    const remaining = Math.max(0, safeTotal - safeCompleted);
    return {
        completed: safeCompleted,
        total: safeTotal,
        remaining,
        percentage: clampPercentage((safeCompleted / safeTotal) * 100),
    };
}

export class ThroughputTracker {
    private readonly samples: ThroughputSample[] = [];
    private readonly windowMs: number;
    private readonly maxSamples: number;

    constructor(windowMs: number = DEFAULT_THROUGHPUT_WINDOW_MS, maxSamples = 256) {
        this.windowMs = windowMs > 0 ? windowMs : DEFAULT_THROUGHPUT_WINDOW_MS;
        this.maxSamples = maxSamples > 1 ? maxSamples : 256;
    }

    record(sample: ThroughputSample): void {
        this.samples.push(sample);
        this.trim(sample.timestampMs);
    }

    private trim(nowMs: number): void {
        const cutoff = nowMs - this.windowMs;
        while (this.samples.length > 0 && this.samples[0].timestampMs < cutoff) {
            this.samples.shift();
        }
        if (this.samples.length > this.maxSamples) {
            this.samples.splice(0, this.samples.length - this.maxSamples);
        }
    }

    snapshot(nowMs: number = Date.now()): ThroughputSnapshot {
        this.trim(nowMs);
        if (this.samples.length < 2) {
            return {
                rowsPerSecond: 0,
                bytesPerSecond: 0,
                unitsPerSecond: 0,
                sampleCount: this.samples.length,
                windowMs: this.windowMs,
            };
        }
        const first = this.samples[0];
        const last = this.samples[this.samples.length - 1];
        const elapsedSec = Math.max(0.001, (last.timestampMs - first.timestampMs) / 1000);
        return {
            rowsPerSecond: Math.max(0, (last.rowsCopied - first.rowsCopied) / elapsedSec),
            bytesPerSecond: Math.max(0, (last.bytesCopied - first.bytesCopied) / elapsedSec),
            unitsPerSecond: Math.max(0, (last.unitsCompleted - first.unitsCompleted) / elapsedSec),
            sampleCount: this.samples.length,
            windowMs: this.windowMs,
        };
    }

    estimateRemainingMs(remaining: {
        rows: number;
        bytes: number;
        units: number;
    }, nowMs: number = Date.now()): { remainingMs: number | null; confidence: number } {
        const snap = this.snapshot(nowMs);
        const estimates: number[] = [];
        if (remaining.rows > 0 && snap.rowsPerSecond > 0) {
            estimates.push((remaining.rows / snap.rowsPerSecond) * 1000);
        }
        if (remaining.bytes > 0 && snap.bytesPerSecond > 0) {
            estimates.push((remaining.bytes / snap.bytesPerSecond) * 1000);
        }
        if (remaining.units > 0 && snap.unitsPerSecond > 0) {
            estimates.push((remaining.units / snap.unitsPerSecond) * 1000);
        }
        if (estimates.length === 0) {
            return { remainingMs: null, confidence: 0 };
        }
        const remainingMs = Math.max(...estimates);
        const confidence = Math.min(1, snap.sampleCount / 8);
        return { remainingMs, confidence };
    }

    clear(): void {
        this.samples.length = 0;
    }
}

export class ReshardingProgressCalculator {
    constructor(
        private readonly weights: ProgressWeights = DEFAULT_PROGRESS_WEIGHTS,
        private readonly bytesPerRowEstimate = 256,
        private readonly throughput = new ThroughputTracker(),
    ) {}

    getThroughputTracker(): ThroughputTracker {
        return this.throughput;
    }

    calculate(
        status: ReshardingStatusInfo,
        extras: { bytesCopied?: number; bytesTotal?: number } = {},
        nowMs: number = Date.now(),
    ): ReshardingProgress {
        const units = dimensionProgress(status.unitsCompleted, status.unitsTotal);
        const rowsTotal =
            status.unitsTotal > 0 && status.rowsCopied >= 0
                ? Math.max(status.rowsCopied + Math.max(0, status.unitsTotal - status.unitsCompleted) * 0, status.rowsCopied)
                : status.rowsCopied;
        const estimatedRowsTotal =
            extras.bytesTotal !== undefined
                ? rowsTotal
                : Math.max(status.rowsCopied, rowsTotal);
        const rows = dimensionProgress(status.rowsCopied, estimatedRowsTotal || status.rowsCopied);
        const bytesCopied = extras.bytesCopied ?? status.rowsCopied * this.bytesPerRowEstimate;
        const bytesTotal = extras.bytesTotal ?? (estimatedRowsTotal || status.rowsCopied) * this.bytesPerRowEstimate;
        const bytes = dimensionProgress(bytesCopied, bytesTotal);

        this.throughput.record({
            timestampMs: nowMs,
            rowsCopied: status.rowsCopied,
            bytesCopied,
            unitsCompleted: status.unitsCompleted,
        });

        const weightedSum =
            units.percentage * this.weights.units +
            rows.percentage * this.weights.rows +
            bytes.percentage * this.weights.bytes;
        const weightTotal = this.weights.units + this.weights.rows + this.weights.bytes;
        const overallPercentage =
            weightTotal <= 0 ? 0 : clampPercentage(weightedSum / weightTotal);

        const remaining = this.throughput.estimateRemainingMs(
            {
                rows: rows.remaining,
                bytes: bytes.remaining,
                units: units.remaining,
            },
            nowMs,
        );

        return {
            migrationId: status.migrationId,
            status: status.status,
            overallPercentage,
            units,
            rows,
            bytes,
            estimatedRemainingMs: remaining.remainingMs,
            estimatedRemainingConfidence: remaining.confidence,
            throughput: this.throughput.snapshot(nowMs),
            updatedAt: new Date(nowMs).toISOString(),
        };
    }
}
