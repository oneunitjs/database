import { describe, it, expect } from "vitest";

import { createCheck } from "../../src/index.js";

describe("check", () => {
    it("reports pool statistics when available", async () => {
        const pool = {
            totalCount: 3,
            idleCount: 2,
            waitingCount: 0,
            pendingAcquireCount: 0,
            options: { max: 10 },
            async query() {
                return {};
            },
        };

        const { check, health } = createCheck(pool);

        const result = await check();
        expect(result.status).toBe("up");
        expect(result.pool).toEqual({ total: 3, idle: 2, waiting: 0, pendingAcquire: 0, max: 10 });

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

    it("check uses a custom validation query when provided", async () => {
        const seen = [];
        const pool = {
            async query(sql) {
                seen.push(sql);
                return {};
            },
        };

        const { check } = createCheck(pool);
        const result = await check({ validationQuery: "SELECT 1 FROM pg_database LIMIT 1" });

        expect(result.status).toBe("up");
        expect(seen).toEqual(["SELECT 1 FROM pg_database LIMIT 1"]);
    });

    it("health passes validation query through", async () => {
        const seen = [];
        const pool = {
            async query(sql) {
                seen.push(sql);
                return {};
            },
        };

        const { health } = createCheck(pool);
        const result = await health({ validationQuery: "SELECT version()" });

        expect(result.healthy).toBe(true);
        expect(seen).toEqual(["SELECT version()"]);
    });

    it("check defaults to SELECT 1 when no validation query provided", async () => {
        const seen = [];
        const pool = {
            async query(sql) {
                seen.push(sql);
                return {};
            },
        };

        const { check } = createCheck(pool);
        await check();

        expect(seen).toEqual(["SELECT 1"]);
    });

    it("check reports down when custom validation query fails", async () => {
        const pool = {
            async query(sql) {
                if (sql === "SELECT bogus") {
                    throw Object.assign(new Error("syntax error"), { code: "42P01" });
                }
                return {};
            },
        };

        const { check } = createCheck(pool);
        const result = await check({ validationQuery: "SELECT bogus" });

        expect(result.status).toBe("down");
        expect(result.error).toMatch(/syntax error/);
        expect(result.code).toBe("42P01");
    });
});
