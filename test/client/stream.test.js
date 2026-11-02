import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { EventEmitter } from "node:events";

import { createStream } from "../../src/index.js";

describe("stream", () => {
    it("releases the connection on end", async () => {
        let client;
        const pool = {
            async connect() {
                const emitter = new EventEmitter();
                emitter.destroy = () => {};
                client = {
                    released: false,
                    async query() {
                        setImmediate(() => emitter.emit("end"));
                        return emitter;
                    },
                    release() {
                        client.released = true;
                    },
                };
                return client;
            },
        };

        const { stream } = createStream(pool);
        const result = await stream("SELECT 1");
        expect(result).toBeTruthy();

        await new Promise((resolve) => setTimeout(resolve, 10));
        expect(client.released).toBe(true);
    });

    it("releases (and destroys) the connection on connection error", async () => {
        let client;
        const pool = {
            async connect() {
                const emitter = new EventEmitter();
                emitter.destroy = () => {};
                client = {
                    released: false,
                    releaseError: undefined,
                    async query() {
                        setImmediate(() => emitter.emit("error", Object.assign(new Error("gone"), { code: "08006" })));
                        return emitter;
                    },
                    release(error) {
                        client.released = true;
                        client.releaseError = error;
                    },
                };
                return client;
            },
        };

        const { stream } = createStream(pool, { error() {} });
        await stream("SELECT 1");
        await new Promise((resolve) => setTimeout(resolve, 10));

        expect(client.released).toBe(true);
        expect(client.releaseError.code).toBe("08006");
    });

    it("cancels the stream via AbortSignal and releases the connection", async () => {
        let client;
        let released = false;
        const pool = {
            async connect() {
                const emitter = new EventEmitter();
                emitter.destroy = vi.fn();
                client = {
                    released: false,
                    destroy: () => { released = true; },
                    async query() {
                        return emitter;
                    },
                    release() {
                        client.released = true;
                    },
                };
                return client;
            },
        };

        const { stream } = createStream(pool);
        const controller = new AbortController();

        const promise = stream("SELECT 1", [], { signal: controller.signal });
        controller.abort();

        await expect(promise).rejects.toThrow(/aborted/);
        await new Promise((resolve) => setTimeout(resolve, 10));
        expect(client.released).toBe(true);
    });

    it("retries the query on failure when retries is configured", async () => {
        let client;
        let attempts = 0;
        const pool = {
            async connect() {
                const emitter = new EventEmitter();
                emitter.destroy = () => {};
                client = {
                    released: false,
                    async query() {
                        attempts++;
                        if (attempts < 2) {
                            throw new Error("transient failure");
                        }
                        setImmediate(() => emitter.emit("end"));
                        return emitter;
                    },
                    release() {
                        client.released = true;
                    },
                };
                return client;
            },
        };

        const { stream } = createStream(pool, { error() {} });
        await stream("SELECT 1", [], { retries: 2 });
        await new Promise((resolve) => setTimeout(resolve, 10));

        expect(attempts).toBe(2);
        expect(client.released).toBe(true);
    });

    it("reports progress via onProgress callback", async () => {
        let client;
        const progressCalls = [];
        const pool = {
            async connect() {
                const emitter = new EventEmitter();
                emitter.destroy = () => {};
                client = {
                    released: false,
                    async query() {
                        // Emit 3 data events then end
                        setImmediate(() => emitter.emit("data", { id: 1 }));
                        setImmediate(() => emitter.emit("data", { id: 2 }));
                        setImmediate(() => emitter.emit("data", { id: 3 }));
                        setImmediate(() => emitter.emit("end"));
                        return emitter;
                    },
                    release() {
                        client.released = true;
                    },
                };
                return client;
            },
        };

        const { stream } = createStream(pool);
        await stream("SELECT 1", [], {
            onProgress: (info) => progressCalls.push(info),
        });

        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(progressCalls.length).toBe(3);
        expect(progressCalls[0].rows).toBe(1);
        expect(progressCalls[1].rows).toBe(2);
        expect(progressCalls[2].rows).toBe(3);
        expect(progressCalls[2].elapsedMs).toBeGreaterThanOrEqual(0);
        expect(client.released).toBe(true);
    });
});
