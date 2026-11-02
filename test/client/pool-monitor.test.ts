import { describe, it, expect, vi } from "vitest";
import { PoolMonitor } from "../../src/client/pool-monitor.ts";

describe("PoolMonitor", () => {
    it("tracks stats", () => {
        const pool: any = { totalCount: 3, idleCount: 1, waitingCount: 1, options: { max: 10 } };
        const m = new PoolMonitor(new Map([["p", pool]]));
        m.start();
        m.stop();
        const s = m.getStats("p");
        expect(s).toBeTruthy();
        expect(m.getAllStats().length).toBe(1);
    });

    it("average and max and near exhaustion", () => {
        const pool: any = { totalCount: 9, idleCount: 0, waitingCount: 0, options: { max: 10 } };
        const m = new PoolMonitor(new Map([["p", pool]]));
        m.start();
        m.stop();
        expect(m.getAverageUtilization()).toBeGreaterThan(0);
        expect(m.getMaxUtilization()).toBeGreaterThan(0);
        expect(m.isNearExhaustion(0.5)).toBe(true);
    });

    it("onStats listener registration", () => {
        const pool: any = { totalCount: 1, idleCount: 1, waitingCount: 0, options: { max: 10 } };
        const m = new PoolMonitor(new Map([["p", pool]]));
        const cb = vi.fn();
        const off = m.onStats(cb);
        m.start();
        m.stop();
        expect(cb).toHaveBeenCalled();
        off();
    });
});
