import { describe, it, expect } from "vitest";

import { QueryBuilder, createQueryBuilder, sql, raw, quoteIdent } from "../../src/index.js";

describe("query builder", () => {
    it("quoteIdent quotes schema-qualified names and escapes quotes", () => {
        expect(quoteIdent("public.users")).toBe('"public"."users"');
        expect(quoteIdent('weird"name')).toBe('"weird""name"');
        expect(quoteIdent("*")).toBe("*");
    });

    it("sql tagged template parameterizes values", () => {
        const fragment = sql`SELECT * FROM users WHERE id = ${1} AND status IN (${["a", "b"]})`;
        expect(fragment.text).toBe("SELECT * FROM users WHERE id = $1 AND status IN ($2, $3)");
        expect(fragment.values).toEqual([1, "a", "b"]);
    });

    it("sql nesting renumbers placeholders", () => {
        const fragment = sql`SELECT * FROM t WHERE (${sql`a = ${1} OR b = ${2}`}) AND c = ${3}`;
        expect(fragment.text).toBe("SELECT * FROM t WHERE (a = $1 OR b = $2) AND c = $3");
        expect(fragment.values).toEqual([1, 2, 3]);
    });

    it("sql.join merges nested values", () => {
        const fragment = sql`WHERE x IN (${sql.join([1, 2, 3])})`;
        expect(fragment.text).toBe("WHERE x IN ($1, $2, $3)");
        expect(fragment.values).toEqual([1, 2, 3]);
    });

    it("sql.join with empty array compiles to FALSE", () => {
        const fragment = sql`WHERE x IN (${sql.join([])})`;
        expect(fragment.text).toBe("WHERE x IN (FALSE)");
        expect(fragment.values).toEqual([]);
    });

    it("select builder produces parameterized SQL", () => {
        const query = new QueryBuilder({ table: "users" })
            .select(["id", "email"])
            .where({ status: "active" })
            .whereIn("role", ["admin", "user"])
            .whereNull("deleted_at")
            .orderBy("created_at", "desc")
            .limit(10)
            .offset(5);

        const { text, values } = query.toSQL();

        expect(text).toBe(
            'SELECT "id", "email" FROM "users" WHERE "status" = $1 AND "role" IN ($2, $3) AND "deleted_at" IS NULL ORDER BY "created_at" DESC LIMIT $4 OFFSET $5',
        );
        expect(values).toEqual(["active", "admin", "user", 10, 5]);
    });

    it("whereIn with an empty list compiles to FALSE", () => {
        const { text, values } = new QueryBuilder({ table: "t" }).select().whereIn("id", []).toSQL();
        expect(text).toMatch(/WHERE FALSE/);
        expect(values).toEqual([]);
    });

    it("unsupported operators are rejected", () => {
        expect(() =>
            new QueryBuilder({ table: "t" }).select().where("id", "; DROP TABLE t", 1),
        ).toThrow(/Unsupported SQL operator/);
    });

    it("insert builder handles multi-row values and conflicts", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .insert([{ a: 1, b: 2 }, { a: 3, b: 4 }])
            .onConflict({ columns: ["a"], update: ["b"] })
            .returning(["id"])
            .toSQL();

        expect(text).toBe(
            'INSERT INTO "users" ("a", "b") VALUES ($1, $2), ($3, $4) ON CONFLICT ("a") DO UPDATE SET "b" = EXCLUDED."b" RETURNING "id"',
        );
        expect(values).toEqual([1, 2, 3, 4]);
    });

    it("update and delete builders are parameterized", () => {
        const update = new QueryBuilder({ table: "users" })
            .update({ name: "x" })
            .where({ id: 1 })
            .toSQL();
        expect(update.text).toBe('UPDATE "users" SET "name" = $1 WHERE "id" = $2');
        expect(update.values).toEqual(["x", 1]);

        const remove = new QueryBuilder({ table: "users" }).delete().where({ id: 9 }).toSQL();
        expect(remove.text).toBe('DELETE FROM "users" WHERE "id" = $1');
        expect(remove.values).toEqual([9]);
    });

    it("builder executes through the injected query function and is awaitable", async () => {
        const calls = [];
        const from = createQueryBuilder({
            query: async (text, values) => {
                calls.push({ text, values });
                return { rows: [{ id: 1 }], rowCount: 1 };
            },
        });

        const result = await from("users").select().where({ id: 1 });
        expect(result.rows).toEqual([{ id: 1 }]);
        expect(calls.length).toBe(1);

        const first = await from("users").select().where({ id: 2 }).first();
        expect(first).toEqual({ id: 1 });
    });

    it("raw fragments can be used in select lists", () => {
        const { text } = new QueryBuilder({ table: "users" })
            .select(raw("COUNT(*)::int AS count"))
            .toSQL();
        expect(text).toBe('SELECT COUNT(*)::int AS count FROM "users"');
    });

    it("clone does not share parameter collectors", () => {
        const base = new QueryBuilder({ table: "users" }).select().where({ id: 1 });
        const clone = base.clone().where({ status: "active" });
        const original = base.toSQL();
        const cloned = clone.toSQL();
        expect(original.values).toEqual([1]);
        expect(cloned.values).toEqual([1, "active"]);
    });

    it("jsonContains and jsonContained bind the JSON payload", () => {
        const contains = sql`${sql.jsonContains("metadata", { color: "blue" })}`;
        expect(contains.text).toBe('"metadata" @> $1');
        expect(contains.values).toEqual([JSON.stringify({ color: "blue" })]);

        const contained = sql.jsonContained("metadata", { color: "blue" });
        expect(contained.text).toBe('"metadata" <@ $1');
        expect(contained.values).toEqual([JSON.stringify({ color: "blue" })]);
    });

    it("window helpers quote identifiers and preserve ASC/DESC", () => {
        const numbered = sql.rowNumber(["dept"], ["salary DESC"]);
        expect(numbered.text).toBe('ROW_NUMBER() OVER ( PARTITION BY "dept" ORDER BY "salary" DESC )');

        const named = sql.window("ranking", { partitionBy: ["dept"], orderBy: ["salary DESC"] });
        expect(named.text).toBe('(PARTITION BY "dept" ORDER BY "salary" DESC)');
    });

    it("explain() prefixes query with EXPLAIN", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .select(["id", "email"])
            .where({ status: "active" })
            .explain()
            .toSQL();

        expect(text).toBe("EXPLAIN SELECT \"id\", \"email\" FROM \"users\" WHERE \"status\" = $1");
        expect(values).toEqual(["active"]);
    });

    it("explain() with analyze option produces EXPLAIN ANALYZE", () => {
        const { text } = new QueryBuilder({ table: "users" })
            .select("*")
            .explain({ analyze: true })
            .toSQL();

        expect(text).toBe("EXPLAIN (ANALYZE) SELECT * FROM \"users\"");
    });

    it("explain() with multiple options produces combined option list", () => {
        const { text } = new QueryBuilder({ table: "orders" })
            .select("*")
            .explain({ analyze: true, buffers: true, verbose: true, format: "json" })
            .toSQL();

        expect(text).toBe("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) SELECT * FROM \"orders\"");
    });

    it("explain() with format option", () => {
        const { text } = new QueryBuilder({ table: "users" })
            .select("id")
            .explain({ format: "xml" })
            .toSQL();

        expect(text).toBe("EXPLAIN (FORMAT XML) SELECT \"id\" FROM \"users\"");
    });

    it("explain() with empty options produces plain EXPLAIN", () => {
        const { text } = new QueryBuilder({ table: "users" })
            .select("*")
            .explain({})
            .toSQL();

        expect(text).toBe("EXPLAIN SELECT * FROM \"users\"");
    });

    it("explain() works with execute() and passes through to query runner", async () => {
        const calls = [];
        const builder = new QueryBuilder({
            table: "users",
            query: async (text, values) => {
                calls.push({ text, values });
                return { rows: [{ plan: "Seq Scan on users" }], rowCount: 1 };
            },
        });

        const result = await builder.select("*").where({ id: 1 }).explain({ analyze: true }).execute();

        expect(calls[0].text).toBe("EXPLAIN (ANALYZE) SELECT * FROM \"users\" WHERE \"id\" = $1");
        expect(calls[0].values).toEqual([1]);
        expect(result.rows).toEqual([{ plan: "Seq Scan on users" }]);
    });

    it("execute() rejects when signal is already aborted", async () => {
        const controller = new AbortController();
        controller.abort();
        const builder = new QueryBuilder({
            table: "users",
            query: async () => ({ rows: [], rowCount: 0 }),
        });

        await expect(builder.select("*").execute({ signal: controller.signal })).rejects.toThrow("aborted");
    });

    it("execute() rejects when signal aborts during query", async () => {
        const controller = new AbortController();
        let resolveQuery;
        const builder = new QueryBuilder({
            table: "users",
            query: async () => new Promise((resolve) => {
                resolveQuery = resolve;
            }),
        });

        const promise = builder.select("*").execute({ signal: controller.signal });
        controller.abort();

        await expect(promise).rejects.toThrow("aborted");
        resolveQuery(); // release the pending query
    });

    it("execute() does not reject when signal aborts after query completes", async () => {
        const controller = new AbortController();
        const builder = new QueryBuilder({
            table: "users",
            query: async () => ({ rows: [{ id: 1 }], rowCount: 1 }),
        });

        const result = await builder.select("*").execute({ signal: controller.signal });
        expect(result.rows).toEqual([{ id: 1 }]);

        // Aborting after completion should not affect the resolved result
        controller.abort();
        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("execute() removes abort listener after query completes", async () => {
        const controller = new AbortController();
        let listenerCount = 0;
        const originalAddEventListener = AbortSignal.prototype.addEventListener;
        AbortSignal.prototype.addEventListener = function(type, listener, options) {
            if (type === "abort") listenerCount++;
            return originalAddEventListener.call(this, type, listener, options);
        };

        try {
            const builder = new QueryBuilder({
                table: "users",
                query: async () => ({ rows: [], rowCount: 0 }),
            });
            await builder.select("*").execute({ signal: controller.signal });
            expect(listenerCount).toBe(1);
        } finally {
            AbortSignal.prototype.addEventListener = originalAddEventListener;
        }
    });

    it("keysetPagination adds WHERE id > lastId for ASC order", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .select("*")
            .orderBy("id", "ASC")
            .keysetPagination({ id: 100 })
            .toSQL();

        expect(text).toBe('SELECT * FROM "users" WHERE "id" > $1 ORDER BY "id" ASC');
        expect(values).toEqual([100]);
    });

    it("keysetPagination adds WHERE id < lastId for DESC order", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .select("*")
            .orderBy("id", "DESC")
            .keysetPagination({ id: 100 })
            .toSQL();

        expect(text).toBe('SELECT * FROM "users" WHERE "id" < $1 ORDER BY "id" DESC');
        expect(values).toEqual([100]);
    });

    it("keysetPagination works with limit", () => {
        const { text, values } = new QueryBuilder({ table: "users" })
            .select("*")
            .orderBy("id", "ASC")
            .keysetPagination({ id: 100 })
            .limit(20)
            .toSQL();

        expect(text).toBe('SELECT * FROM "users" WHERE "id" > $1 ORDER BY "id" ASC LIMIT $2');
        expect(values).toEqual([100, 20]);
    });

    it("keysetPagination supports composite keys with OR logic", () => {
        const { text, values } = new QueryBuilder({ table: "orders" })
            .select("*")
            .orderBy("id", "ASC")
            .orderBy("created_at", "DESC")
            .keysetPagination({ id: 50, created_at: "2024-01-15" })
            .toSQL();

        expect(text).toBe(
            'SELECT * FROM "orders" WHERE ("id" > $1) OR ("id" = $2 AND "created_at" < $3) ORDER BY "id" ASC, "created_at" DESC',
        );
        expect(values).toEqual([50, 50, "2024-01-15"]);
    });

    it("keysetPagination throws when no orderBy is set", () => {
        const builder = new QueryBuilder({ table: "users" }).select("*");
        expect(() => builder.keysetPagination({ id: 1 })).toThrow(
            /requires at least one orderBy clause/,
        );
    });

    it("keysetPagination throws when a value is missing for an order column", () => {
        const builder = new QueryBuilder({ table: "users" })
            .select("*")
            .orderBy("id", "ASC");
        expect(() => builder.keysetPagination({})).toThrow(
            /requires a value for order column "id"/,
        );
    });
});
