import { describe, it, expect } from "vitest";

import { createCursor } from "../../src/index.js";

describe("cursor", () => {
    it("reads batches and releases on close", async () => {
        let closed = false;
        let client;
        const batches = [[{ id: 1 }, { id: 2 }], [{ id: 3 }]];

        const pool = {
            async connect() {
                client = {
                    released: false,
                    async query() {
                        let index = 0;
                        return {
                            read(count, callback) {
                                setImmediate(() => callback(null, batches[index++] ?? []));
                            },
                            close(callback) {
                                closed = true;
                                setImmediate(() => callback(null));
                            },
                        };
                    },
                    release() {
                        client.released = true;
                    },
                };
                return client;
            },
        };

        const { cursor } = createCursor(pool);
        const handle = await cursor("SELECT * FROM t");

        expect(await handle.read(2)).toEqual([{ id: 1 }, { id: 2 }]);
        await handle.close();

        expect(closed).toBe(true);
        expect(client.released).toBe(true);
    });

    it("supports async iteration and releases at the end", async () => {
        let client;
        const batches = [[{ id: 1 }, { id: 2 }], [{ id: 3 }]];

        const pool = {
            async connect() {
                client = {
                    released: false,
                    async query() {
                        let index = 0;
                        return {
                            read(count, callback) {
                                setImmediate(() => callback(null, batches[index++] ?? []));
                            },
                            close(callback) {
                                setImmediate(() => callback(null));
                            },
                        };
                    },
                    release() {
                        client.released = true;
                    },
                };
                return client;
            },
        };

        const { cursor } = createCursor(pool);
        const handle = await cursor("SELECT * FROM t");

        const rows = [];
        for await (const row of handle) {
            rows.push(row);
        }

        expect(rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
        expect(client.released).toBe(true);
    });
});
