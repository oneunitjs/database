import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

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

    it("uses configurable batchSize from options", async () => {
        let client;
        let readCountUsed;
        const pool = {
            async connect() {
                client = {
                    released: false,
                    async query() {
                        return {
                            read(count, callback) {
                                readCountUsed = count;
                                setImmediate(() => callback(null, []));
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
        const handle = await cursor("SELECT * FROM t", [], { batchSize: 500 });

        await handle.read(); // uses default batchSize from options
        expect(readCountUsed).toBe(500);
        await handle.close();
        expect(client.released).toBe(true);
    });

    it("reports progress via onProgress callback", async () => {
        let client;
        const progressCalls = [];
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
        const handle = await cursor("SELECT * FROM t", [], {
            onProgress: (info) => progressCalls.push(info),
        });

        await handle.read(2);
        await handle.close();

        expect(progressCalls.length).toBeGreaterThan(0);
        expect(progressCalls[0].rows).toBe(2);
        expect(progressCalls[0].elapsedMs).toBeGreaterThanOrEqual(0);
        expect(client.released).toBe(true);
    });

    it("cancel() closes the cursor and releases the connection", async () => {
        let closed = false;
        let client;
        const pool = {
            async connect() {
                client = {
                    released: false,
                    async query() {
                        return {
                            read(count, callback) {
                                setImmediate(() => callback(null, []));
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

        await handle.cancel();

        expect(closed).toBe(true);
        expect(client.released).toBe(true);
    });
});
