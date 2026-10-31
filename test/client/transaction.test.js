import { describe, it, expect } from "vitest";

import { createTransaction, createClient, DatabaseError } from "../../src/index.js";

function makeClient(store) {
    const client = {
        released: false,
        releaseError: undefined,
        async query(sql) {
            store.push(sql);
            return {};
        },
        release(error) {
            client.released = true;
            client.releaseError = error;
        },
    };
    return client;
}

function makeFakePool(client) {
    return {
        async connect() {
            return client;
        },
    };
}

function makeFakeClient(queryImpl) {
    let released = false;

    const client = {
        released: () => released,
        async query(sql) {
            if (queryImpl) {
                return queryImpl(sql);
            }
            return {};
        },
        release() {
            released = true;
        },
    };

    return client;
}

describe("transaction extended", () => {
    it("honours isolation and read-only options", async () => {
        const queries = [];
        const client = makeClient(queries);
        const { transaction } = createTransaction({ pool: { async connect() { return client; } } });

        await transaction(async () => "done", { isolation: "SERIALIZABLE", readOnly: true });

        expect(queries[0]).toBe("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY");
        expect(queries[queries.length - 1]).toBe("COMMIT");
    });

    it("applies a local statement timeout", async () => {
        const queries = [];
        const client = makeClient(queries);
        const { transaction } = createTransaction({ pool: { async connect() { return client; } } });

        await transaction(async () => true, { timeout: 250 });

        expect(queries).toEqual(["BEGIN", "SET LOCAL statement_timeout = 250", "COMMIT"]);
    });

    it("rejects invalid isolation levels", async () => {
        const client = makeClient([]);
        const { transaction } = createTransaction({ pool: { async connect() { return client; } } });

        await expect(
            transaction(async () => true, { isolation: "BOGUS" }),
        ).rejects.toThrow(/Invalid isolation level/);
        expect(client.released).toBe(false);
    });

    it("retries transient failures", async () => {
        let attempt = 0;
        const order = [];
        const pool = {
            async connect() {
                const index = attempt;
                attempt += 1;
                return {
                    async query(sql) {
                        order.push(`${index}:${sql}`);
                        return {};
                    },
                    release() {},
                };
            },
        };

        const { transaction } = createTransaction({ pool });

        const result = await transaction(async () => {
            if (order.filter((entry) => entry.endsWith(":BEGIN")).length === 1) {
                throw Object.assign(new Error("serialization"), { code: "40001" });
            }
            return "committed";
        }, { retry: { retries: 1, baseDelay: 1, jitter: 0 } });

        expect(result).toBe("committed");
        expect(order).toContain("0:ROLLBACK");
        expect(order).toContain("1:COMMIT");
    });

    it("destroys the connection when it fails with a connection error", async () => {
        const queries = [];
        const client = makeClient(queries);
        const { transaction } = createTransaction({ pool: { async connect() { return client; } } });

        await expect(
            transaction(async () => {
                throw Object.assign(new Error("terminated"), { code: "08006" });
            }),
        ).rejects.toThrow(/terminated/);

        expect(client.released).toBe(true);
        expect(client.releaseError).toBeInstanceOf(DatabaseError);
        expect(client.releaseError.code).toBe("08006");
    });

    it("savepoint rejects unsafe names", async () => {
        const client = makeClient([]);
        const { savepoint } = createTransaction({ pool: { async connect() { return client; } } });

        await expect(
            savepoint(client, "1; DROP TABLE users", async () => {}),
        ).rejects.toThrow(/Invalid savepoint name/);
    });

    it("transaction commits and returns the callback result", async () => {
        const queries = [];
        const client = makeFakeClient(async (sql) => {
            queries.push(sql);
            return {};
        });
        const pool = makeFakePool(client);

        const { transaction } = createTransaction({ pool });

        const result = await transaction(async (c) => {
            expect(c).toBe(client);
            await c.query("UPDATE profiles SET name = $1", ["x"]);
            return { ok: true };
        });

        expect(queries).toEqual(["BEGIN", "UPDATE profiles SET name = $1", "COMMIT"]);
        expect(result).toEqual({ ok: true });
        expect(client.released()).toBe(true);
    });

    it("transaction rolls back and rethrows when the callback fails", async () => {
        const queries = [];
        const client = makeFakeClient(async (sql) => {
            queries.push(sql);
            return {};
        });
        const pool = makeFakePool(client);

        const { transaction } = createTransaction({ pool });

        await expect(
            transaction(async () => {
                throw new Error("boom");
            }),
        ).rejects.toThrow(/boom/);

        expect(queries).toEqual(["BEGIN", "ROLLBACK"]);
        expect(client.released()).toBe(true);
    });

    it("transaction logs a failed rollback but still rethrows", async () => {
        const client = makeFakeClient(async (sql) => {
            if (sql === "ROLLBACK") {
                throw new Error("rollback failed");
            }
            return {};
        });
        const pool = makeFakePool(client);

        const { transaction } = createTransaction({ pool, logger: { error() {} } });

        await expect(
            transaction(async () => {
                throw new Error("boom");
            }),
        ).rejects.toThrow(/boom/);
    });

    it("savepoint releases on success", async () => {
        const queries = [];
        const client = makeFakeClient(async (sql) => {
            queries.push(sql);
            return {};
        });

        const { savepoint } = createTransaction({ pool: makeFakePool(client) });

        const result = await savepoint(client, "sp1", async () => "inner");

        expect(result).toBe("inner");
        expect(queries).toEqual(["SAVEPOINT sp1", "RELEASE SAVEPOINT sp1"]);
    });

    it("savepoint rolls back to savepoint on failure", async () => {
        const queries = [];
        const client = makeFakeClient(async (sql) => {
            queries.push(sql);
            return {};
        });

        const { savepoint } = createTransaction({ pool: makeFakePool(client) });

        await expect(
            savepoint(client, "sp1", async () => {
                throw new Error("inner boom");
            }),
        ).rejects.toThrow(/inner boom/);

        expect(queries).toEqual(["SAVEPOINT sp1", "ROLLBACK TO SAVEPOINT sp1"]);
    });
});
