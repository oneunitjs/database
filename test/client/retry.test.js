import { describe, it, expect } from "vitest";

import { withRetry, createRetry } from "../../src/index.js";

function transientError() {
    return Object.assign(new Error("serialization failure"), { code: "40001" });
}

describe("retry", () => {
    it("withRetry retries transient errors until success", async () => {
        let attempts = 0;
        const result = await withRetry(async () => {
            attempts += 1;
            if (attempts < 3) {
                throw transientError();
            }
            return "ok";
        }, { retries: 5, baseDelay: 1, jitter: 0 });

        expect(result).toBe("ok");
        expect(attempts).toBe(3);
    });

    it("withRetry does not retry non-transient errors", async () => {
        let attempts = 0;
        await expect(
            withRetry(async () => {
                attempts += 1;
                throw Object.assign(new Error("unique"), { code: "23505" });
            }, { retries: 5, baseDelay: 1, jitter: 0 }),
        ).rejects.toThrow(/unique/);
        expect(attempts).toBe(1);
    });

    it("withRetry gives up after the configured number of retries", async () => {
        let attempts = 0;
        await expect(
            withRetry(async () => {
                attempts += 1;
                throw transientError();
            }, { retries: 2, baseDelay: 1, jitter: 0 }),
        ).rejects.toThrow();
        expect(attempts).toBe(3);
    });

    it("withRetry invokes the onRetry hook with backoff metadata", async () => {
        const seen = [];
        let attempts = 0;

        await withRetry(async () => {
            attempts += 1;
            if (attempts < 2) {
                throw transientError();
            }
            return true;
        }, {
            retries: 3,
            baseDelay: 1,
            jitter: 0,
            onRetry: (info) => seen.push(info),
        });

        expect(seen.length).toBe(1);
        expect(seen[0].attempt).toBe(1);
        expect(seen[0].error.code).toBe("40001");
    });

    it("withRetry honours an aborted signal", async () => {
        const controller = new AbortController();
        controller.abort();

        await expect(
            withRetry(async () => "never", {
                retries: 2,
                signal: controller.signal,
            }),
        ).rejects.toThrow(/aborted/i);
    });

    it("createRetry binds defaults", async () => {
        let attempts = 0;
        const retry = createRetry({ retries: 1, baseDelay: 1, jitter: 0 });

        await expect(
            retry.run(async () => {
                attempts += 1;
                throw transientError();
            }),
        ).rejects.toThrow();
        expect(attempts).toBe(2);
        expect(retry.isTransient({ code: "40001" })).toBe(true);
    });
});
