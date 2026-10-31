import { describe, it, expect } from "vitest";
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
});
