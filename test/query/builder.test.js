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
});
