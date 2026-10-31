import { describe, it, expect } from "vitest";

import { createCheck } from "../../src/index.js";

describe("check", () => {
    it("reports pool statistics when available", async () => {
        const pool = {
            totalCount: 3,
            idleCount: 2,
            waitingCount: 0,
            async query() {
                return {};
            },
        };

        const { check, health } = createCheck(pool);

        const result = await check();
        expect(result.status).toBe("up");
        expect(result.pool).toEqual({ total: 3, idle: 2, waiting: 0 });

        const detailed = await health();
        expect(detailed.healthy).toBe(true);
        expect(detailed.error).toBe(null);
        expect(detailed.checkedAt).toBeTruthy();
    });

    it("health surfaces the error code when down", async () => {
        const pool = {
            async query() {
                throw Object.assign(new Error("refused"), { code: "ECONNREFUSED" });
            },
        };

        const { health } = createCheck(pool);
        const result = await health();

        expect(result.healthy).toBe(false);
        expect(result.error).toBe("refused");
        expect(result.errorCode).toBe("ECONNREFUSED");
    });

    it("createCheck reports up for a healthy pool", async () => {
        const pool = {
            async query(sql) {
                expect(sql).toBe("SELECT 1");
                return {};
            },
        };

        const { check } = createCheck(pool);
        const result = await check();

        expect(result.status).toBe("up");
        expect(result.latency.unit).toBe("ms");
    });

    it("createCheck reports down when the pool errors", async () => {
        const pool = {
            async query() {
                throw new Error("connection refused");
            },
        };

        const { check } = createCheck(pool);
        const result = await check();

        expect(result.status).toBe("down");
        expect(result.error).toMatch(/connection refused/);
    });
});
