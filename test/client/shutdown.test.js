import { describe, it, expect } from "vitest";

import { createShutdown, TimeoutError } from "../../src/index.js";

describe("shutdown", () => {
    it("is idempotent across concurrent and repeated calls", async () => {
        let ended = 0;
        const pool = {
            async end() {
                ended += 1;
            },
        };

        const { shutdown } = createShutdown(pool);
        await Promise.all([shutdown(), shutdown(), shutdown()]);
        await shutdown();

        expect(ended).toBe(1);
    });

    it("rejects with a TimeoutError when the pool will not close", async () => {
        const pool = {
            end() {
                return new Promise(() => {});
            },
        };

        const { shutdown } = createShutdown(pool);
        await expect(shutdown({ timeout: 20 })).rejects.toThrow(TimeoutError);
    });
});
