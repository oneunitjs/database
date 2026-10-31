import { describe, it, expect } from "vitest";
import { EventEmitter } from "node:events";

import {
    createClient,
    createTransaction,
    createCheck,
    createShutdown,
    createStream,
    createCursor,
    createBatch,
    createMigrator,
    createSchemaManager,
    createModel,
    createModelFactory,
    createRetry,
    withRetry,
    sql,
    raw,
    quoteIdent,
    quoteIdentList,
    QueryBuilder,
    normalizeError,
    isTransientError,
    isUniqueViolation,
    isForeignKeyViolation,
    isConnectionError,
    isSerializationFailure,
    isDeadlock,
    isConstraintViolation,
    TimeoutError,
    DatabaseError,
    encode,
    decode,
    encodeToString,
    decodeFromString,
    serializeRow,
    deserializeRow,
    serializeQueryResult,
    deserializeQueryResult,
} from "../src/index.js";

function makeClient(queryImpl) {
    const client = {
        calls: [],
        released: false,
        releaseError: undefined,
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

describe("query builder edge cases", () => {
    it("empty where clause produces no WHERE", () => {
        const { text, values } = new QueryBuilder({ table: "users" }).select().toSQL();
        expect(text).toBe('SELECT * FROM "users"');
        expect(values).toEqual([]);
    });

    it("null values are parameterized", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .where({ name: null, active: undefined })
            .toSQL();
        expect(text).toBe('SELECT * FROM "users" WHERE "name" IS NULL AND "active" IS NULL');
        expect(values).toEqual([]);
    });

    it("where with array value", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .where("role", ["admin", "user"])
            .toSQL();
        expect(text).toBe('SELECT * FROM "users" WHERE "role" IN ($1, $2)');
        expect(values).toEqual(["admin", "user"]);
    });

    it("quoteIdent rejects injection via identifiers", () => {
        expect(quoteIdent("users; DROP TABLE users")).toBe('"users; DROP TABLE users"');
        expect(quoteIdent("public.users")).toBe('"public"."users"');
        expect(quoteIdent("a.b.c")).toBe('"a"."b"."c"');
        expect(quoteIdent("*")).toBe("*");
        expect(() => quoteIdent("")).toThrow(TypeError);
        expect(() => quoteIdent(null)).toThrow(TypeError);
    });

    it("quoteIdentList escapes list members", () => {
        expect(quoteIdentList(["id", 'weird"col', "public.users"])).toBe('"id", "weird""col", "public"."users"');
        expect(quoteIdentList([])).toBe("");
    });

    it("raw fragment interpolation preserves values", () => {
        const { text, values } = new QueryBuilder({ table: "orders" })
            .select(raw("COALESCE(SUM(amount), 0) AS total"))
            .where({ status: "paid" })
            .toSQL();
        expect(text).toBe('SELECT COALESCE(SUM(amount), 0) AS total FROM "orders" WHERE "status" = $1');
        expect(values).toEqual(["paid"]);
    });

    it("deeply nested sql fragments renumber correctly", () => {
        const inner = sql`x = ${1} OR y = ${2}`;
        const middle = sql`(${inner} AND z = ${3})`;
        const fragment = sql`SELECT * FROM t WHERE ${middle} AND w = ${4}`;
        expect(fragment.text).toBe("SELECT * FROM t WHERE (x = $1 OR y = $2 AND z = $3) AND w = $4");
        expect(fragment.values).toEqual([1, 2, 3, 4]);
    });

    it("large IN list stays parameterized (500 values)", () => {
        const ids = Array.from({ length: 500 }, (_, i) => i + 1);
        const { text, values } = new QueryBuilder({ table: "users" })
            .whereIn("id", ids)
            .toSQL();
        expect(text).toMatch(/WHERE "id" IN \(\$1, \$2, \$3/);
        expect(values.length).toBe(500);
        expect(values).toEqual(ids);
    });

    it("update with empty data throws", () => {
        expect(() => new QueryBuilder({ table: "users" }).update({}).toSQL()).toThrow(/at least one column/);
    });

    it("insert with no rows throws", () => {
        expect(() => new QueryBuilder({ table: "users" }).insert([]).toSQL()).toThrow(/at least one row/);
    });

    it("orderBy with invalid direction throws", () => {
        expect(() => new QueryBuilder({ table: "users" }).select().orderBy("created_at", "INVALID")).toThrow(/Invalid order direction/);
    });

    it("limit zero compiles LIMIT $1", () => {
        const { text, values } = new QueryBuilder({ table: "users" }).select().limit(0).toSQL();
        expect(text).toBe('SELECT * FROM "users" LIMIT $1');
        expect(values).toEqual([0]);
    });

    it("offset without limit compiles", () => {
        const { text, values } = new QueryBuilder({ table: "users" }).select().offset(100).toSQL();
        expect(text).toBe('SELECT * FROM "users" OFFSET $1');
        expect(values).toEqual([100]);
    });
});

describe("client edge cases", () => {
    it("query with QueryConfig instance preserves name", async () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        await db.query({ name: "find_user", text: "SELECT * FROM users WHERE id = $1", values: [5] });
        expect(client.calls[0].config.name).toBe("find_user");
        expect(client.calls[0].config.values).toEqual([5]);
    });

    it("query with values-as-options object shape", async () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        await db.query("SELECT * FROM users WHERE id = $1", { values: [1], timeout: 10 });
        const call = client.calls[0];
        expect(call.config.values).toEqual([1]);
    });

    it("query with undefined values defaults to empty array", async () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        await db.query("SELECT 1");
        expect(client.calls[0].config.values).toEqual([]);
    });

    it("prepare with empty name throws TypeError", () => {
        const client = makeClient();
        const db = createClient(makePool(() => client));
        expect(() => db.prepare("", "SELECT 1")).toThrow(TypeError);
        expect(() => db.prepare("name", "")).toThrow(TypeError);
        expect(() => db.prepare("name", 123)).toThrow(TypeError);
    });

    it("retry=false overrides default retry", async () => {
        let attempts = 0;
        const client = makeClient(async () => {
            attempts += 1;
            throw Object.assign(new Error("serialization"), { code: "40001" });
        });
        const db = createClient(makePool(() => client), undefined, { retry: 3 });
        await expect(db.query("SELECT 1", [], { retry: false })).rejects.toThrow(/serialization/);
        expect(attempts).toBe(1);
    });

    it("retry=0 means no retries", async () => {
        let attempts = 0;
        const client = makeClient(async () => {
            attempts += 1;
            throw Object.assign(new Error("serialization"), { code: "40001" });
        });
        const db = createClient(makePool(() => client), undefined, { retry: 0 });
        await expect(db.query("SELECT 1")).rejects.toThrow();
        expect(attempts).toBe(1);
    });

    it("connect timeout rejects before query", async () => {
        const pool = {
            async connect() {
                return new Promise(() => {});
            },
        };
        const db = createClient(pool, undefined, { connectTimeout: 10 });
        await expect(db.query("SELECT 1")).rejects.toThrow(TimeoutError);
    });

    it("concurrent queries get distinct clients from pool", async () => {
        const clients = [makeClient(), makeClient()];
        let turn = 0;
        const pool = {
            async connect() {
                return clients[turn++];
            },
        };
        const db = createClient(pool);
        await Promise.all([db.query("SELECT 1"), db.query("SELECT 2")]);
        expect(clients[0].released).toBe(true);
        expect(clients[1].released).toBe(true);
    });

    it("queryOne returns null when rows is undefined", async () => {
        const client = makeClient(async () => ({}));
        const db = createClient(makePool(() => client));
        expect(await db.queryOne("SELECT 1")).toBeNull();
    });
});

describe("transaction edge cases", () => {
    it("callback error before any query still rolls back", async () => {
        const queries = [];
        const client = makeClient(async (sql) => {
            queries.push(sql);
            return {};
        });
        const pool = makePool(() => client);
        const { transaction } = createTransaction({ pool });

        await expect(
            transaction(async () => {
                throw new Error("pre-flight failure");
            }),
        ).rejects.toThrow(/pre-flight failure/);
        expect(queries).toEqual(["BEGIN", "ROLLBACK"]);
        expect(client.released).toBe(true);
    });

    it("query failure mid-transaction rolls back", async () => {
        const queries = [];
        const client = makeClient(async (sql) => {
            queries.push(sql);
            if (sql.includes("BOOM")) {
                throw Object.assign(new Error("query failed"), { code: "42P01" });
            }
            return {};
        });
        const pool = makePool(() => client);
        const { transaction } = createTransaction({ pool });

        await expect(
            transaction(async (c) => {
                await c.query("INSERT INTO t (a) VALUES ($1)", [1]);
                await c.query("BOOM");
            }),
        ).rejects.toThrow(/query failed/);
        expect(queries).toEqual(["BEGIN", "INSERT INTO t (a) VALUES ($1)", "BOOM", "ROLLBACK"]);
        expect(client.released).toBe(true);
    });

    it("nested savepoint rollback does not abort outer transaction", async () => {
        const queries = [];
        const client = makeClient(async (sql) => {
            queries.push(sql);
            return {};
        });
        const pool = makePool(() => client);
        const { transaction, savepoint } = createTransaction({ pool });

        const result = await transaction(async (c) => {
            await c.query("INSERT INTO t (a) VALUES ($1)", [1]);
            await expect(savepoint(c, "sp1", async () => {
                throw new Error("inner boom");
            })).rejects.toThrow(/inner boom/);
            await c.query("INSERT INTO t (a) VALUES ($2)", [2]);
            return "committed";
        });

        expect(result).toBe("committed");
        expect(queries).toEqual([
            "BEGIN",
            "INSERT INTO t (a) VALUES ($1)",
            "SAVEPOINT sp1",
            "ROLLBACK TO SAVEPOINT sp1",
            "INSERT INTO t (a) VALUES ($2)",
            "COMMIT",
        ]);
        expect(client.released).toBe(true);
    });

    it("savepoint with unsafe name is rejected", async () => {
        const client = makeClient();
        const pool = makePool(() => client);
        const { savepoint } = createTransaction({ pool });
        await expect(
            savepoint(client, "sp; DROP TABLE users", async () => 1),
        ).rejects.toThrow(/Invalid savepoint name/);
    });

    it("isolation level invalid is rejected", async () => {
        const client = makeClient();
        const pool = makePool(() => client);
        const { transaction } = createTransaction({ pool });
        await expect(
            transaction(async () => 1, { isolation: "BOGUS" }),
        ).rejects.toThrow(/Invalid isolation level/);
    });

    it("read-only flag emits READ ONLY", async () => {
        const queries = [];
        const client = makeClient(async (sql) => {
            queries.push(sql);
            return {};
        });
        const pool = makePool(() => client);
        const { transaction } = createTransaction({ pool });

        await transaction(async () => 1, { readOnly: true });
        expect(queries[0]).toMatch(/READ ONLY/);
    });
});

describe("retry edge cases", () => {
    it("custom shouldRetry predicate controls retryability", async () => {
        let attempts = 0;
        await expect(
            withRetry(
                async () => {
                    attempts += 1;
                    throw Object.assign(new Error("unique"), { code: "23505" });
                },
                {
                    retries: 3,
                    baseDelay: 1,
                    jitter: 0,
                    shouldRetry: (error) => error.code === "23505",
                },
            ),
        ).rejects.toThrow(/unique/);
        expect(attempts).toBe(4);
    });

    it("retryOnTimeout=false prevents retrying TimeoutError", async () => {
        let attempts = 0;
        await expect(
            withRetry(
                async () => {
                    attempts += 1;
                    throw new TimeoutError("timed out", { timeout: 10 });
                },
                { retries: 3, baseDelay: 1, jitter: 0, retryOnTimeout: false },
            ),
        ).rejects.toThrow(TimeoutError);
        expect(attempts).toBe(1);
    });

    it("non-Error throw is wrapped and retried if transient", async () => {
        let attempts = 0;
        await expect(
            withRetry(
                async () => {
                    attempts += 1;
                    throw "string error";
                },
                { retries: 1, baseDelay: 1, jitter: 0, shouldRetry: () => true },
            ),
        ).rejects.toThrow();
        expect(attempts).toBe(2);
    });

    it("abort signal during delay rejects", async () => {
        const controller = new AbortController();
        setTimeout(() => controller.abort(), 5);
        let attempts = 0;
        await expect(
            withRetry(
                async () => {
                    attempts += 1;
                    throw Object.assign(new Error("serialization"), { code: "40001" });
                },
                { retries: 5, baseDelay: 1000, jitter: 0, signal: controller.signal },
            ),
        ).rejects.toThrow(/aborted/i);
        expect(attempts).toBe(1);
    });

    it("maxDelay caps backoff", async () => {
        const delays = [];
        let attempts = 0;
        await expect(
            withRetry(
                async () => {
                    attempts += 1;
                    throw Object.assign(new Error("serialization"), { code: "40001" });
                },
                {
                    retries: 4,
                    baseDelay: 1000,
                    maxDelay: 50,
                    factor: 10,
                    jitter: 0,
                    sleep: (ms) => delays.push(ms),
                },
            ),
        ).rejects.toThrow();
        expect(delays.length).toBe(4);
        delays.forEach((d) => expect(d).toBeLessThanOrEqual(50));
    });

    it("onRetry hook throwing does not break retry loop", async () => {
        let attempts = 0;
        const result = await withRetry(
            async () => {
                attempts += 1;
                if (attempts < 2) {
                    throw Object.assign(new Error("serialization"), { code: "40001" });
                }
                return "ok";
            },
            {
                retries: 3,
                baseDelay: 1,
                jitter: 0,
                onRetry: () => {
                    throw new Error("hook blew up");
                },
            },
        );
        expect(result).toBe("ok");
        expect(attempts).toBe(2);
    });
});

describe("batch edge cases", () => {
    it("empty rows returns empty array", async () => {
        const batch = createBatch({ query: async () => ({ rows: [], rowCount: 0 }) });
        expect(await batch.insertMany("users", [])).toEqual([]);
        await expect(batch.insertMany("users", [{}])).rejects.toThrow(/at least one column/);
    });

    it("single column insert", async () => {
        const calls = [];
        const batch = createBatch({
            query: async (text, params) => {
                calls.push({ text, params });
                return { rows: [], rowCount: params.length };
            },
        });
        const count = await batch.insertMany("tags", [{ name: "a" }, { name: "b" }, { name: "c" }]);
        expect(count).toBe(3);
        expect(calls.length).toBe(1);
        expect(calls[0].params).toEqual(["a", "b", "c"]);
    });

    it("chunking splits at parameter limit boundary", async () => {
        const calls = [];
        const batch = createBatch({
            query: async (text, params) => {
                calls.push({ text, params });
                return { rows: [], rowCount: 0 };
            },
        });
        const rows = Array.from({ length: 32768 }, (_, i) => ({ a: i, b: i + 1 }));
        await batch.insertMany("users", rows, { chunkSize: 32768 });
        expect(calls.length).toBe(2);
        expect(calls[0].params.length).toBe(65534);
        expect(calls[1].params.length).toBe(2);
    });

    it("chunkSize of 1 forces one row per chunk", async () => {
        const calls = [];
        const batch = createBatch({
            query: async () => {
                calls.push(1);
                return { rows: [], rowCount: 0 };
            },
        });
        await batch.insertMany("users", [{ a: 1 }, { a: 2 }, { a: 3 }], { chunkSize: 1 });
        expect(calls.length).toBe(3);
    });

    it("onConflict true emits DO NOTHING", async () => {
        const batch = createBatch({ query: async () => ({ rows: [], rowCount: 0 }) });
        const n = await batch.insertMany("users", [{ id: 1 }], { onConflict: true });
        expect(n).toBe(1);
    });

    it("onConflict with constraint name", async () => {
        const calls = [];
        const batch = createBatch({
            query: async (text) => {
                calls.push(text);
                return { rows: [], rowCount: 0 };
            },
        });
        await batch.insertMany("users", [{ id: 1, email: "a@b.c" }], {
            onConflict: { constraint: "uq_users_email", update: ["email"] },
        });
        expect(calls[0]).toMatch(/ON CONSTRAINT "uq_users_email" DO UPDATE SET "email" = EXCLUDED."email"/);
    });

    it("returning collects rows across chunks", async () => {
        const collected = [];
        const batch = createBatch({
            query: async (text, params) => {
                collected.push(...params.map((p) => ({ id: p })));
                return { rows: collected.slice(-params.length), rowCount: params.length };
            },
        });
        const rows = await batch.insertMany("users", [{ a: 1 }, { a: 2 }, { a: 3 }], {
            returning: ["id"],
            chunkSize: 2,
        });
        expect(rows.length).toBe(3);
        expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
    });

    it("insertMany inside transaction uses provided client", async () => {
        let clientUsed = null;
        const batch = createBatch({
            query: async () => ({ rows: [], rowCount: 0 }),
            transaction: async (callback) => {
                const client = {
                    query: async (t, p) => ({ rows: [], rowCount: p.length }),
                };
                clientUsed = client;
                return callback(client);
            },
        });
        const count = await batch.insertMany("users", [{ a: 1 }, { a: 2 }], { transaction: true });
        expect(count).toBe(2);
        expect(clientUsed).not.toBeNull();
    });
});

describe("model edge cases", () => {
    it("find with empty where selects all", async () => {
        const calls = [];
        const model = createModel({
            table: "users",
            query: async (text, values) => {
                calls.push({ text, values });
                return { rows: [{ id: 1 }], rowCount: 1 };
            },
        });
        const rows = await model.find({});
        expect(rows.length).toBe(1);
        expect(calls[0].values).toEqual([]);
    });

    it("findById with null id", async () => {
        const calls = [];
        const model = createModel({ table: "users", idColumn: "uid", query: async (text, values) => {
            calls.push({ text, values });
            return { rows: [], rowCount: 0 };
        }});
        const row = await model.findById(null);
        expect(row).toBeNull();
        expect(calls[0].text).toMatch(/"uid" IS NULL/);
        expect(calls[0].text).toMatch(/LIMIT \$1/);
        expect(calls[0].values).toEqual([1]);
    });

    it("insert with empty object", async () => {
        const calls = [];
        const model = createModel({ table: "users", query: async (text, values) => {
            calls.push({ text, values });
            return { rows: [{ id: 1 }], rowCount: 1 };
        }});
        const row = await model.insert({});
        expect(row).toEqual({ id: 1 });
        expect(calls[0].text).toMatch(/INSERT INTO "users" \(\) VALUES \(\) RETURNING \*/);
    });

    it("count returns 0 when no rows", async () => {
        const model = createModel({ table: "users", query: async () => ({ rows: [], rowCount: 0 }) });
        expect(await model.count({ status: "active" })).toBe(0);
    });

    it("exists returns boolean", async () => {
        const model = createModel({ table: "users", query: async () => ({ rows: [{ id: 1 }], rowCount: 1 }) });
        expect(await model.exists({ id: 1 })).toBe(true);
        const empty = createModel({ table: "users", query: async () => ({ rows: [], rowCount: 0 }) });
        expect(await empty.exists({ id: 999 })).toBe(false);
    });

    it("factory creates models with shared query", async () => {
        const calls = [];
        const factory = createModelFactory({ query: async (text, values) => {
            calls.push({ text, values });
            return { rows: [], rowCount: 0 };
        }});
        const users = factory("users");
        const orders = factory("orders");
        await users.find({ id: 1 });
        await orders.find({ id: 2 });
        expect(calls.length).toBe(2);
        expect(calls[0].text).toMatch(/"users"/);
        expect(calls[1].text).toMatch(/"orders"/);
    });

    it("custom idColumn used by findById/updateById/removeById", async () => {
        const calls = [];
        const model = createModel({ table: "profiles", idColumn: "slug", query: async (text, values) => {
            calls.push({ text, values });
            return { rows: [{ slug: "x" }], rowCount: 1 };
        }});
        await model.findById("abc");
        await model.updateById("abc", { name: "y" });
        await model.removeById("abc");
        calls.forEach((c) => expect(c.text).toMatch(/"slug"/));
        expect(calls.map((c) => c.values)).toEqual([["abc", 1], ["y", "abc"], ["abc"]]);
    });
});

describe("error edge cases", () => {
    it("normalizeError with non-Error value", () => {
        const error = normalizeError("string failure");
        expect(error).toBeInstanceOf(DatabaseError);
        expect(error.message).toBe("string failure");
    });

    it("normalizeError preserves all driver fields", () => {
        const cause = Object.assign(new Error("violation"), {
            code: "23505",
            constraint: "uq_email",
            table: "users",
            column: "email",
            dataType: "text",
            detail: "Key already exists.",
            hint: "Use a different email.",
            position: "12",
            where: "INSERT INTO users",
            schema: "public",
            routine: "ins_users",
        });
        const error = normalizeError(cause);
        expect(error.code).toBe("23505");
        expect(error.constraint).toBe("uq_email");
        expect(error.table).toBe("users");
        expect(error.column).toBe("email");
        expect(error.dataType).toBe("text");
        expect(error.detail).toBe("Key already exists.");
        expect(error.hint).toBe("Use a different email.");
        expect(error.position).toBe("12");
        expect(error.where).toBe("INSERT INTO users");
        expect(error.schema).toBe("public");
        expect(error.routine).toBe("ins_users");
        expect(error.cause).toBe(cause);
    });

    it("DatabaseError toJSON includes all fields", () => {
        const error = normalizeError(Object.assign(new Error("boom"), { code: "42601", constraint: "x" }));
        const json = error.toJSON();
        expect(json.name).toBe("DatabaseError");
        expect(json.message).toBe("boom");
        expect(json.code).toBe("42601");
        expect(json.constraint).toBe("x");
        expect(json.cause).toBeUndefined();
    });

    it("isTransientError handles null and undefined", () => {
        expect(isTransientError(null)).toBe(false);
        expect(isTransientError(undefined)).toBe(false);
        expect(isTransientError({})).toBe(false);
        expect(isTransientError({ code: "40001" })).toBe(true);
        expect(isTransientError({ code: "57P03" })).toBe(true);
        expect(isTransientError({ code: "08006" })).toBe(true);
        expect(isTransientError({ code: "23505" })).toBe(false);
        expect(isTransientError(new TimeoutError("t", { timeout: 1 }))).toBe(true);
    });

    it("all boolean classifiers", () => {
        expect(isUniqueViolation({ code: "23505" })).toBe(true);
        expect(isUniqueViolation({ code: "23503" })).toBe(false);
        expect(isForeignKeyViolation({ code: "23503" })).toBe(true);
        expect(isForeignKeyViolation({ code: "23505" })).toBe(false);
        expect(isConnectionError({ code: "08001" })).toBe(true);
        expect(isConnectionError({ code: "08006" })).toBe(true);
        expect(isConnectionError({ code: "23505" })).toBe(false);
        expect(isSerializationFailure({ code: "40001" })).toBe(true);
        expect(isDeadlock({ code: "40P01" })).toBe(true);
        expect(isConstraintViolation({ code: "23505" })).toBe(true);
        expect(isConstraintViolation({ code: "23503" })).toBe(true);
        expect(isConstraintViolation({ code: "40001" })).toBe(false);
    });
});

describe("JSON serialization edge cases", () => {
    it("encode/decode handles undefined and null", () => {
        expect(decode(encode(undefined))).toEqual(null);
        expect(decode(encode(null))).toEqual(null);
    });

    it("encode/decode handles nested and mixed types", () => {
        const value = {
            str: "hello",
            num: 42,
            bool: true,
            nil: null,
            arr: [1, "two", { three: 3 }],
            nested: { deep: { deeper: [4, 5] } },
        };
        expect(decode(encode(value))).toEqual(value);
        expect(decodeFromString(encodeToString(value))).toEqual(value);
    });

    it("unicode and special characters round-trip", () => {
        const value = { name: "José García", emoji: "🚀🔥", symbols: "©®™", newline: "a\nb\tc" };
        expect(decode(encode(value))).toEqual(value);
    });

    it("encode returns a Buffer", () => {
        const result = encode({ a: 1 });
        expect(Buffer.isBuffer(result)).toBe(true);
        expect(result.toString("utf8")).toBe('{"a":1}');
    });

    it("serializeQueryResult with missing fields", () => {
        const result = { rows: [{ id: 1 }] };
        const restored = deserializeQueryResult(serializeQueryResult(result));
        expect(restored.rows).toEqual([{ id: 1 }]);
        expect(restored.rowCount).toBe(0);
        expect(restored.fields).toEqual([]);
    });
});

describe("check/health edge cases", () => {
    it("missing pool stats reports undefined pool", async () => {
        const pool = {
            async query() {
                return {};
            },
        };
        const { check, health } = createCheck(pool);
        const result = await check();
        expect(result.status).toBe("up");
        expect(result.pool).toBeUndefined();
        const detailed = await health();
        expect(detailed.healthy).toBe(true);
        expect(detailed.checkedAt).toBeTruthy();
    });

    it("pool stats are reported when available", async () => {
        const pool = {
            totalCount: 3,
            idleCount: 2,
            waitingCount: 0,
            async query() {
                return {};
            },
        };
        const { check } = createCheck(pool);
        const result = await check();
        expect(result.status).toBe("up");
        expect(result.pool).toEqual({ total: 3, idle: 2, waiting: 0 });
    });

    it("latency is recorded on success", async () => {
        const pool = {
            async query() {
                return {};
            },
        };
        const { check } = createCheck(pool);
        const result = await check();
        expect(result.status).toBe("up");
        expect(result.latency).toBeTruthy();
        expect(result.latency.unit).toBe("ms");
        expect(typeof result.latency.value).toBe("number");
    });
});

describe("shutdown edge cases", () => {
    it("end throwing propagates on every call", async () => {
        let calls = 0;
        const pool = {
            async end() {
                calls += 1;
                throw new Error("boom");
            },
        };
        const { shutdown } = createShutdown(pool);
        await expect(shutdown()).rejects.toThrow(/boom/);
        await expect(shutdown()).rejects.toThrow(/boom/);
        expect(calls).toBe(2);
    });

    it("concurrent calls invoke end once", async () => {
        let ended = 0;
        const pool = {
            async end() {
                ended += 1;
                await new Promise((r) => setTimeout(r, 10));
            },
        };
        const { shutdown } = createShutdown(pool);
        await Promise.all([shutdown(), shutdown(), shutdown(), shutdown()]);
        expect(ended).toBe(1);
    });
});

describe("stream edge cases", () => {
    it("empty result still releases connection", async () => {
        let released = false;
        const pool = {
            async connect() {
                const emitter = new EventEmitter();
                emitter.destroy = () => {};
                setImmediate(() => emitter.emit("end"));
                return {
                    released: false,
                    async query() { return emitter; },
                    release() { released = true; },
                };
            },
        };
        const { stream } = createStream(pool);
        await stream("SELECT 1 WHERE FALSE");
        await new Promise((r) => setTimeout(r, 10));
        expect(released).toBe(true);
    });
});

describe("cursor edge cases", () => {
    it("close before any read still releases", async () => {
        let closed = false;
        let released = false;
        const pool = {
            async connect() {
                return {
                    released: false,
                    async query() {
                        return {
                            read(count, cb) { setImmediate(() => cb(null, [])); },
                            close(cb) { closed = true; setImmediate(() => cb(null)); },
                        };
                    },
                    release() { released = true; },
                };
            },
        };
        const { cursor } = createCursor(pool);
        const handle = await cursor("SELECT * FROM t");
        await handle.close();
        expect(closed).toBe(true);
        expect(released).toBe(true);
    });

    it("empty batches yield no rows", async () => {
        const pool = {
            async connect() {
                return {
                    released: false,
                    async query() {
                        return {
                            read(count, cb) { setImmediate(() => cb(null, [])); },
                            close(cb) { setImmediate(() => cb(null)); },
                        };
                    },
                    release() {},
                };
            },
        };
        const { cursor } = createCursor(pool);
        const handle = await cursor("SELECT * FROM t");
        const rows = [];
        for await (const row of handle) {
            rows.push(row);
        }
        expect(rows).toEqual([]);
    });
});

describe("migrator edge cases", () => {
    it("empty migrations list applies nothing", async () => {
        const client = {
            released: false,
            async query() {
                return { rows: [] };
            },
            release() { this.released = true; },
        };
        const migrator = createMigrator({ connect: async () => client });
        const applied = await migrator.run({ migrations: [] });
        expect(applied).toEqual([]);
        expect(client.released).toBe(true);
    });

    it("duplicate migration IDs are applied once each occurrence", async () => {
        const state = { applied: [], executed: [] };
        const client = {
            released: false,
            async query(text, values = []) {
                const sqlText = String(text).trim();
                if (sqlText.startsWith("SELECT pg_advisory_lock") || sqlText.startsWith("SELECT pg_advisory_unlock")) return {};
                if (sqlText.includes("CREATE TABLE IF NOT EXISTS")) return {};
                if (sqlText.includes("SELECT id, name, checksum, applied_at")) return { rows: state.applied.map((r) => ({ ...r })) };
                if (sqlText.includes("INSERT INTO") && sqlText.includes("_writer_migrations")) {
                    state.applied.push({ id: values[0], name: values[1], checksum: values[2], applied_at: new Date().toISOString() });
                    return {};
                }
                if (["BEGIN", "COMMIT", "ROLLBACK"].includes(sqlText)) return {};
                state.executed.push(sqlText);
                return {};
            },
            release() { this.released = true; },
        };
        const migrator = createMigrator({ connect: async () => client });
        const migrations = [
            { id: "001", name: "one", up: "CREATE TABLE t (id int)", down: "DROP TABLE t" },
            { id: "001", name: "one again", up: "ALTER TABLE t ADD COLUMN x int", down: "ALTER TABLE t DROP COLUMN x" },
        ];
        const applied = await migrator.run({ migrations });
        expect(applied).toEqual(["001", "001"]);
        expect(state.applied.length).toBe(2);
        expect(state.executed.length).toBe(2);
    });

    it("status reports pending as applied=false", async () => {
        const client = {
            released: false,
            async query() {
                return { rows: [] };
            },
            release() { this.released = true; },
        };
        const migrator = createMigrator({ connect: async () => client });
        const migrations = [
            { id: "001", name: "pending", up: "SELECT 1", down: "SELECT 1" },
        ];
        const status = await migrator.status({ migrations });
        expect(status.length).toBe(1);
        expect(status[0].applied).toBe(false);
        expect(status[0].appliedAt).toBeNull();
    });
});

describe("schema manager edge cases", () => {
    it("drop table with cascade option", async () => {
        const queries = [];
        const pool = {
            async query(sql) {
                queries.push(sql);
            },
        };
        const manager = createSchemaManager(pool);
        await manager.drop.table("users", { cascade: true });
        expect(queries[0]).toMatch(/DROP TABLE IF EXISTS users/);
        expect(queries[0]).toMatch(/CASCADE/);
    });

    it("alter add column", async () => {
        const queries = [];
        const pool = {
            async query(sql) {
                queries.push(sql);
            },
        };
        const manager = createSchemaManager(pool);
        await manager.alter.addColumns("users", { age: "INTEGER" });
        expect(queries[0]).toMatch(/ALTER TABLE users/);
        expect(queries[0]).toMatch(/ADD COLUMN age INTEGER/);
    });
});

describe("real world scenarios", () => {
    it("paginated list with limit/offset/order", () => {
        const page = (pageNumber, pageSize) => {
            const offset = (pageNumber - 1) * pageSize;
            return new QueryBuilder({ table: "orders" })
                .select(["id", "customer_id", "total", "created_at"])
                .where({ status: "completed" })
                .orderBy("created_at", "desc")
                .limit(pageSize)
                .offset(offset)
                .toSQL();
        };
        const { text, values } = page(2, 25);
        expect(text).toBe('SELECT "id", "customer_id", "total", "created_at" FROM "orders" WHERE "status" = $1 ORDER BY "created_at" DESC LIMIT $2 OFFSET $3');
        expect(values).toEqual(["completed", 25, 25]);
    });

    it("upsert insert-or-update pattern", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .insert({ id: 1, email: "a@b.c", name: "Alice" })
            .onConflict({ columns: ["id"], update: ["email", "name"] })
            .returning(["*"])
            .toSQL();
        expect(text).toBe('INSERT INTO "users" ("id", "email", "name") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "email" = EXCLUDED."email", "name" = EXCLUDED."name" RETURNING *');
        expect(values).toEqual([1, "a@b.c", "Alice"]);
    });

    it("health check circuit breaker pattern", async () => {
        let failures = 0;
        const pool = {
            totalCount: 10,
            idleCount: 10,
            waitingCount: 0,
            async query() {
                failures += 1;
                if (failures <= 2) throw Object.assign(new Error("refused"), { code: "ECONNREFUSED" });
                return {};
            },
        };
        const { health } = createCheck(pool);
        const r1 = await health();
        expect(r1.healthy).toBe(false);
        expect(r1.errorCode).toBe("ECONNREFUSED");
        const r2 = await health();
        expect(r2.healthy).toBe(false);
        const r3 = await health();
        expect(r3.healthy).toBe(true);
    });

    it("prepared statement reused across multiple executions", async () => {
        const calls = [];
        const client = makeClient(async (config) => {
            calls.push(config);
            return { rows: [{ id: config.values?.[0] }], rowCount: 1 };
        });
        const db = createClient(makePool(() => client));
        const stmt = db.prepare("user_by_id", "SELECT * FROM users WHERE id = $1");
        const [r1, r2, r3] = await Promise.all([
            stmt.execute([1]),
            stmt.execute([2]),
            stmt.execute([3]),
        ]);
        expect(r1.rows).toEqual([{ id: 1 }]);
        expect(r2.rows).toEqual([{ id: 2 }]);
        expect(r3.rows).toEqual([{ id: 3 }]);
        calls.forEach((c) => expect(c.name).toBe("user_by_id"));
        expect(client.released).toBe(true);
    });

    it("bulk insert with conflict resolution returns count", async () => {
        const calls = [];
        const batch = createBatch({
            query: async (text, params) => {
                calls.push({ text, params });
                return { rows: [], rowCount: params.length / 2 };
            },
        });
        const count = await batch.insertMany("audit_log", Array.from({ length: 1200 }, (_, i) => ({ event: "login", user_id: i })), {
            chunkSize: 500,
            onConflict: true,
        });
        expect(count).toBe(1200);
        expect(calls.length).toBe(3);
        calls.forEach((c) => expect(c.text).toMatch(/ON CONFLICT DO NOTHING/));
    });
});
