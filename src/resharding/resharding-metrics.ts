export interface CopyBatchRecord {
    startedAtMs: number;
    completedAtMs: number;
    rows: number;
    bytes: number;
    success: boolean;
    errorType?: string;
}

export interface CopyMetrics {
    rowsCopied: number;
    bytesCopied: number;
    unitsCopied: number;
    batchesCompleted: number;
    batchesFailed: number;
    retries: number;
    rowsPerSecond: number;
    bytesPerSecond: number;
    unitsPerSecond: number;
    batchesPerSecond: number;
    latencyMs: { p50: number; p95: number; p99: number; avg: number };
    errorsByType: Record<string, number>;
    currentBatch: { startedAtMs: number; rows: number; bytes: number } | null;
    startedAtMs: number | null;
    updatedAtMs: number;
}

function percentile(sorted: number[], p: number): number {
    if (sorted.length === 0) {
        return 0;
    }
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
    return sorted[idx];
}

export class CopyMetricsTracker {
    private readonly batches: CopyBatchRecord[] = [];
    private readonly latencies: number[] = [];
    private readonly errorsByType: Record<string, number> = {};
    private currentBatch: { startedAtMs: number; rows: number; bytes: number } | null = null;
    private rowsCopied = 0;
    private bytesCopied = 0;
    private unitsCopied = 0;
    private batchesCompleted = 0;
    private batchesFailed = 0;
    private retries = 0;
    private startedAtMs: number | null = null;
    private readonly windowMs: number;

    constructor(windowMs = 30_000) {
        this.windowMs = windowMs > 0 ? windowMs : 30_000;
    }

    start(nowMs: number = Date.now()): void {
        if (this.startedAtMs === null) {
            this.startedAtMs = nowMs;
        }
    }

    startBatch(rows = 0, bytes = 0, nowMs: number = Date.now()): void {
        this.start(nowMs);
        this.currentBatch = { startedAtMs: nowMs, rows, bytes };
    }

    completeBatch(success: boolean, errorType?: string, nowMs: number = Date.now()): void {
        const current = this.currentBatch;
        this.currentBatch = null;
        if (!current) {
            return;
        }
        const record: CopyBatchRecord = {
            startedAtMs: current.startedAtMs,
            completedAtMs: nowMs,
            rows: current.rows,
            bytes: current.bytes,
            success,
            errorType,
        };
        this.batches.push(record);
        this.latencies.push(Math.max(0, nowMs - current.startedAtMs));
        if (success) {
            this.batchesCompleted += 1;
            this.rowsCopied += current.rows;
            this.bytesCopied += current.bytes;
        } else {
            this.batchesFailed += 1;
            if (errorType) {
                this.errorsByType[errorType] = (this.errorsByType[errorType] ?? 0) + 1;
            }
        }
        this.trim(nowMs);
    }

    recordRetry(): void {
        this.retries += 1;
    }

    recordUnitCompleted(): void {
        this.unitsCopied += 1;
    }

    private trim(nowMs: number): void {
        const cutoff = nowMs - this.windowMs;
        while (this.batches.length > 0 && this.batches[0].completedAtMs < cutoff) {
            this.batches.shift();
        }
        if (this.latencies.length > 10_000) {
            this.latencies.splice(0, this.latencies.length - 10_000);
        }
    }

    snapshot(nowMs: number = Date.now()): CopyMetrics {
        this.trim(nowMs);
        const windowBatches = this.batches;
        const elapsedSec = Math.max(
            0.001,
            ((windowBatches.length >= 2
                ? windowBatches[windowBatches.length - 1].completedAtMs - windowBatches[0].startedAtMs
                : this.windowMs) || this.windowMs) / 1000,
        );
        const windowRows = windowBatches.filter((b) => b.success).reduce((s, b) => s + b.rows, 0);
        const windowBytes = windowBatches.filter((b) => b.success).reduce((s, b) => s + b.bytes, 0);
        const elapsedSinceStartSec = Math.max(
            0.001,
            this.startedAtMs === null ? this.windowMs / 1000 : (nowMs - this.startedAtMs) / 1000,
        );
        const sorted = [...this.latencies].sort((a, b) => a - b);
        const avg = sorted.length === 0 ? 0 : sorted.reduce((s, v) => s + v, 0) / sorted.length;
        return {
            rowsCopied: this.rowsCopied,
            bytesCopied: this.bytesCopied,
            unitsCopied: this.unitsCopied,
            batchesCompleted: this.batchesCompleted,
            batchesFailed: this.batchesFailed,
            retries: this.retries,
            rowsPerSecond: windowRows / elapsedSec,
            bytesPerSecond: windowBytes / elapsedSec,
            unitsPerSecond: this.unitsCopied / elapsedSinceStartSec,
            batchesPerSecond: windowBatches.length / elapsedSec,
            latencyMs: {
                p50: percentile(sorted, 50),
                p95: percentile(sorted, 95),
                p99: percentile(sorted, 99),
                avg,
            },
            errorsByType: { ...this.errorsByType },
            currentBatch: this.currentBatch ? { ...this.currentBatch } : null,
            startedAtMs: this.startedAtMs,
            updatedAtMs: nowMs,
        };
    }
}
