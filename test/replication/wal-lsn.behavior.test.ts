import { describe, it, expect, vi } from "vitest";
import { LsnUtils } from "../../src/replication/lsn-utils.js";
import { WalTracker } from "../../src/replication/wal-tracker.js";

describe("WAL LSN tracking behavior", () => {
    it("parses split and single-part LSNs", () => {
        expect(LsnUtils.parseLsn("0/0")).toBe(0n);
        expect(LsnUtils.parseLsn("0/16B3748")).toBe(0x16b3748n);
        expect(LsnUtils.parseLsn("1/0")).toBe(1n << 32n);
        expect(LsnUtils.parseLsn("FFFFFFFF/FFFFFFFF")).toBe((0xffffffffn << 32n) | 0xffffffffn);
        expect(LsnUtils.parseLsn("16B3748")).toBe(0x16b3748n);
    });

    it("rejects malformed LSNs", () => {
        expect(() => LsnUtils.parseLsn("")).toThrow(/Invalid LSN/);
        expect(() => LsnUtils.parseLsn("   ")).toThrow(/Invalid LSN/);
        expect(() => LsnUtils.parseLsn("0/1/2")).toThrow(/Invalid LSN/);
        expect(() => LsnUtils.parseLsn("zz/1")).toThrow(/Invalid LSN/);
        expect(() => LsnUtils.parseLsn("0/")).toThrow(/Invalid LSN/);
        expect(() => LsnUtils.parseLsn(null)).toThrow(/Invalid LSN/);
    });

    it("round-trips format and compare", () => {
        const value = (1n << 32n) | 0xabcdefn;
        const formatted = LsnUtils.formatLsn(value);
        expect(LsnUtils.parseLsn(formatted)).toBe(value);
        expect(LsnUtils.compareLsn("0/1", "0/1")).toBe(0);
        expect(LsnUtils.compareLsn("0/1", "0/2")).toBe(-1);
        expect(LsnUtils.compareLsn("1/0", "0/FFFFFFFF")).toBe(1);
        expect(() => LsnUtils.formatLsn(-1n)).toThrow(/negative/);
    });

    it("computes replica lag as primary minus replica", () => {
        expect(LsnUtils.lagBytes("0/100", "0/40")).toBe(0xc0n);
        expect(LsnUtils.diffLsn("0/40", "0/100")).toBe(0xc0n);
        expect(LsnUtils.lagBytes("0/40", "0/40")).toBe(0n);
        expect(LsnUtils.lagBytes("0/10", "0/20")).toBe(-0x10n);
        expect(LsnUtils.formatBytes(512n)).toBe("512B");
        expect(LsnUtils.formatBytes(2048n)).toBe("2KB");
        expect(LsnUtils.formatBytes(2n * 1024n * 1024n)).toBe("2MB");
        expect(LsnUtils.formatBytes(-2048n)).toBe("-2KB");
    });

    it("reports replica lag and detects excessive lag", async () => {
        const primaryPool = {
            query: vi.fn().mockResolvedValue({ rows: [{ lsn: "0/200" }] }),
        };
        const replicaPool = {
            query: vi.fn().mockResolvedValue({ rows: [{ lsn: "0/100" }] }),
        };
        const tracker = new WalTracker(
            [
                { endpointId: "primary", role: "primary", pool: primaryPool },
                { endpointId: "r1", role: "replica", pool: replicaPool },
            ],
            undefined,
            { maxLsnLagBytes: 10n },
        );
        const lag = await tracker.updateLagInfo();
        expect(lag).toHaveLength(1);
        expect(lag[0].endpointId).toBe("r1");
        expect(lag[0].lsnDiffBytes).toBe(0x100n);
        expect(tracker.hasExcessiveLag()).toBe(true);
        expect(tracker.getLaggingReplicas().map((r) => r.endpointId)).toEqual(["r1"]);
        tracker.stop();
    });

    it("skips a replica that times out without throwing", async () => {
        const primaryPool = {
            query: vi.fn().mockResolvedValue({ rows: [{ lsn: "0/200" }] }),
        };
        const replicaPool = {
            query: vi.fn().mockImplementation(() => new Promise(() => {})),
        };
        const tracker = new WalTracker(
            [
                { endpointId: "primary", role: "primary", pool: primaryPool },
                { endpointId: "slow", role: "replica", pool: replicaPool },
            ],
            undefined,
            { queryTimeoutMs: 20, maxLsnLagBytes: 1n },
        );
        const lag = await tracker.updateLagInfo();
        expect(lag).toEqual([]);
        expect(tracker.getLsn("slow")).toBeNull();
        tracker.stop();
    });

    it("treats empty LSN results as a failure for that endpoint", async () => {
        const primaryPool = {
            query: vi.fn().mockResolvedValue({ rows: [{ lsn: null }] }),
        };
        const tracker = new WalTracker(
            [{ endpointId: "primary", role: "primary", pool: primaryPool }],
            undefined,
            { queryTimeoutMs: 50 },
        );
        await tracker.updateLagInfo();
        expect(tracker.getPrimaryLsn()).toBeNull();
        tracker.stop();
    });

    it("does not report lag for a replica that returns an empty LSN", async () => {
        const primaryPool = {
            query: vi.fn().mockResolvedValue({ rows: [{ lsn: "0/200" }] }),
        };
        const replicaPool = {
            query: vi.fn().mockResolvedValue({ rows: [{ lsn: null }] }),
        };
        const tracker = new WalTracker(
            [
                { endpointId: "primary", role: "primary", pool: primaryPool },
                { endpointId: "empty", role: "replica", pool: replicaPool },
            ],
            undefined,
            { queryTimeoutMs: 50 },
        );
        const lag = await tracker.updateLagInfo();
        expect(lag).toEqual([]);
        expect(tracker.getLsn("empty")).toBeNull();
        expect(tracker.getPrimaryLsn()).toBe("0/200");
        tracker.stop();
    });

    it("start is idempotent and stop clears the interval", async () => {
        const tracker = new WalTracker([], undefined, { checkIntervalMs: 50_000 });
        tracker.start();
        tracker.start();
        expect(tracker.isTracking).toBe(true);
        tracker.stop();
        tracker.stop();
        expect(tracker.isTracking).toBe(false);
    });
});
