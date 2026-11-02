import { describe, it, expect } from "vitest";

import { createClient, createHooks, createMetrics, TimeoutError } from "../../src/index.js";

function makeClient(queryImpl) {
    const client = {
        calls: [],
        released: false,
        releaseError: undefined,
        cancelled: false,
        activeQuery: null,
        async query(config, values) {
            client.calls.push({ config, values });
            if (queryImpl) {
                return queryImpl(config, values, client);
            }
            return { rows: [], rowCount: 0 };
        },
        release(error) {
            client.released = true;
            client.releaseError = error;
        },
        cancel(c, q) {
            client.cancelled = true;
        },
        _getActiveQuery() {
            return client.activeQuery;
        },
    };
    return client;
}

function makePool(factory) {
    let index = 0;
    return {
        async connect() {
            const client = factory(index);
            index += 1;
            return client;
        },
    };
}

describe("client extended", () => {
    it("queryOne returns the first row or null", async () => {
        const client = makeClient(async () => ({ rows: [{ id: 1 }], rowCount: 1 }));
        const db = createClient(makePool(() => client));

        expect(await db.queryOne("SELECT * FROM users")).toEqual({ id: 1 });

        const empty = makeClient(async () => ({ rows: [], rowCount: 0 }));
        const db2 = createClient(makePool(() => empty));
        expect(await db2.queryOne("SELECT * FROM users")).toBeNull();
    });

    it("query retries transient errors by default", async () => {
        let attempts = 0;
        const db = createClient(makePool(() => makeClient(async () => {
            attempts += 1;
            throw Object.assign(new Error("serialization"), { code: "40001" });
        })));

        await expect(db.query("SELECT 1")).rejects.toThrow();
        expect(attempts).toBeGreaterThan(1);
    });

    it("query retries genuinely transient errors when opted in", async () => {
        let attempts = 0;
        const db = createClient(makePool(() => makeClient(async () => {
            attempts += 1;
            if (attempts === 1) {
                throw Object.assign(new Error("serialization"), { code: "40001" });
            }
            return { rows: [{ ok: true }], rowCount: 1 };
        })));

        const result = await db.query("SELECT 1", [], {
            retry: { retries: 2, baseDelay: 1, jitter: 0 },
        });

        expect(attempts).toBe(2);
        expect(result.rows).toEqual([{ ok: true }]);
    });

    it("prepared statements send a named query config", async () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));

        const statement = db.prepare("find_user", "SELECT * FROM users WHERE id = $1");
        await statement.execute([42]);

        expect(client.calls[0].config.name).toBe("find_user");
        expect(client.calls[0].config.text).toBe("SELECT * FROM users WHERE id = $1");
        expect(client.calls[0].config.values).toEqual([42]);
    });

    it("query timeout rejects and destroys the connection", async () => {
        const client = makeClient(() => new Promise(() => {}));
        const db = createClient(makePool(() => client), undefined, { timeout: 20 });

        await expect(db.query("SELECT pg_sleep(10)")).rejects.toThrow(TimeoutError);

        expect(client.released).toBe(true);
        expect(client.releaseError).toBeInstanceOf(Error);
    });

    it("connection errors destroy the client on release", async () => {
        const client = makeClient(async () => {
            throw Object.assign(new Error("connection terminated"), { code: "08006" });
        });
        const db = createClient(makePool(() => client));

        await expect(db.query("SELECT 1")).rejects.toThrow();
        expect(client.released).toBe(true);
        expect(client.releaseError.code).toBe("08006");
    });

    it("instrumentation hooks observe successful and failed queries", async () => {
        const metrics = createMetrics();
        const events = [];
        const hooks = createHooks({
            metrics,
            instrumentation: {
                onQuery: (event) => events.push(event),
                onError: (event) => events.push(event),
            },
        });

        const good = makeClient(async () => ({ rows: [], rowCount: 3 }));
        const db = createClient(makePool(() => good), undefined, { hooks });
        await db.query("SELECT 1");

        const bad = makeClient(async () => {
            throw Object.assign(new Error("syntax"), { code: "42601" });
        });
        const db2 = createClient(makePool(() => bad), undefined, { hooks });
        await expect(db2.query("SELECT nope")).rejects.toThrow();

        const snapshot = metrics.snapshot();
        expect(snapshot.queries).toBe(1);
        expect(snapshot.errors).toBe(1);
        expect(events.length).toBe(2);
        expect(events[0].success).toBe(true);
        expect(events[0].rowCount).toBe(3);
    });

    it("logQueries does not leak parameter values by default", async () => {
        const logs = [];
        const logger = {
            debug: (payload) => logs.push(payload),
            error: () => {},
            warn: () => {},
            info: () => {},
        };
        const hooks = createHooks({ logger, logQueries: true });
        const client = makeClient();
        const db = createClient(makePool(() => client), undefined, { hooks });

        await db.query("SELECT * FROM users WHERE password = $1", ["secret"]);

        expect(logs.length).toBe(1);
        expect(logs[0].sql).toBe("SELECT * FROM users WHERE password = $1");
        expect(logs[0].parameters).toBeUndefined();
        expect(logs[0].parameterCount).toBe(1);
    });

    it("createClient.query acquires, queries and releases", async () => {
        let released = false;
        const pool = {
            async connect() {
                return {
                    async query() {
                        return { rows: [{ id: 1 }] };
                    },
                    release() {
                        released = true;
                    },
                };
            },
        };

        const client = createClient(pool);
        const result = await client.query("SELECT * FROM users WHERE id = $1", [1]);

        expect(result.rows).toEqual([{ id: 1 }]);
        expect(released).toBe(true);
    });

    it("getClient releases a late-arriving connection after acquire timeout", async () => {
        let released = false;
        let resolveConnect;
        const pool = {
            connect() {
                return new Promise((resolve) => {
                    resolveConnect = () => resolve({
                        release() {
                            released = true;
                        },
                    });
                });
            },
        };

        const db = createClient(pool, undefined, { connectTimeout: 20 });
        await expect(db.getClient({ timeout: 20 })).rejects.toBeInstanceOf(TimeoutError);
        resolveConnect();
        await new Promise((resolve) => setTimeout(resolve, 10));
        expect(released).toBe(true);
    });

    it("cancel() is exposed on the client API", () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        expect(typeof db.cancel).toBe("function");
    });

    it("cancel() calls pg cancel on the client", () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        client.activeQuery = { text: "SELECT pg_sleep(100)" };
        db.cancel(client);
        expect(client.cancelled).toBe(true);
    });

    it("cancel() is a no-op when no query is active", () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        expect(() => db.cancel(client)).not.toThrow();
        expect(client.cancelled).toBe(false);
    });

    it("query timeout cancels the query on the server by default", async () => {
        const client = makeClient(() => new Promise(() => {}));
        client.activeQuery = { text: "SELECT pg_sleep(100)" };
        const db = createClient(makePool(() => client), undefined, { timeout: 20 });

        await expect(db.query("SELECT pg_sleep(100)")).rejects.toThrow(TimeoutError);
        expect(client.cancelled).toBe(true);
    });

    it("query timeout does not cancel when cancel: false", async () => {
        const client = makeClient(() => new Promise(() => {}));
        client.activeQuery = { text: "SELECT pg_sleep(100)" };
        const db = createClient(makePool(() => client), undefined, { timeout: 20 });

        await expect(db.query("SELECT pg_sleep(100)", [], { cancel: false })).rejects.toThrow(TimeoutError);
        expect(client.cancelled).toBe(false);
    });
});
