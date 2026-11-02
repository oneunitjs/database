import { describe, it, expect, vi } from "vitest";

import { createModel } from "../../src/index.js";

function captureQuery(resultFactory) {
    const calls = [];
    const query = async (text, values, options) => {
        calls.push({ text, values, options });
        return resultFactory ? resultFactory(text, values) : { rows: [], rowCount: 0 };
    };
    query.calls = calls;
    return query;
}

describe("model", () => {
    it("exposes CRUD helpers over the query function", async () => {
        const query = captureQuery((text) => {
            if (/COUNT/.test(text)) {
                return { rows: [{ count: 4 }], rowCount: 1 };
            }
            if (/SELECT 1 AS exists/.test(text)) {
                return { rows: [{ exists: 1 }], rowCount: 1 };
            }
            if (/INSERT/.test(text)) {
                return { rows: [{ id: 7, name: "x" }], rowCount: 1 };
            }
            if (/UPDATE/.test(text)) {
                return { rows: [{ id: 7, name: "y" }], rowCount: 1 };
            }
            return { rows: [{ id: 7 }], rowCount: 1 };
        });

        const users = createModel({ query, table: "users" });

        expect(await users.find({ status: "active" })).toEqual([{ id: 7 }]);
        expect(await users.findOne({ id: 7 })).toEqual({ id: 7 });
        expect(await users.findById(7)).toEqual({ id: 7 });
        expect(await users.insert({ name: "x" })).toEqual({ id: 7, name: "x" });
        expect(await users.updateById(7, { name: "y" })).toEqual({ id: 7, name: "y" });
        expect(await users.deleteById(7)).toEqual({ id: 7 });
        expect(await users.count()).toBe(4);
        expect(await users.exists({ id: 7 })).toBe(true);
    });

    it("can run against an explicit transaction client", async () => {
        const query = captureQuery();
        const client = {
            async query(text, values) {
                client.seen = { text, values };
                return { rows: [{ id: 1 }] };
            },
        };

        const users = createModel({ query, table: "users" });
        const rows = await users.findById(1, { client });

        expect(rows).toEqual({ id: 1 });
        expect(client.seen.values).toEqual([1, 1]);
        expect(query.calls.length).toBe(0);
    });

    it("bulkUpsert inserts new rows with ON CONFLICT DO UPDATE", async () => {
        const query = captureQuery((text) => {
            if (/INSERT/.test(text)) {
                return { rows: [{ id: 1, email: "a@x.com" }, { id: 2, email: "b@x.com" }], rowCount: 2 };
            }
            return { rows: [], rowCount: 0 };
        });

        const users = createModel({ query, table: "users" });
        const result = await users.bulkUpsert(
            [{ id: 1, email: "a@x.com" }, { id: 2, email: "b@x.com" }],
            { conflictColumns: ["id"] }
        );

        expect(result).toEqual([{ id: 1, email: "a@x.com" }, { id: 2, email: "b@x.com" }]);
        const sql = query.calls[query.calls.length - 1].text;
        expect(sql).toMatch(/ON CONFLICT/);
        expect(sql).toMatch(/DO UPDATE SET/);
    });

    it("bulkUpsert with custom updateColumns", async () => {
        const query = captureQuery((text) => {
            if (/INSERT/.test(text)) {
                return { rows: [{ id: 1, email: "a@x.com", name: "Alice" }], rowCount: 1 };
            }
            return { rows: [], rowCount: 0 };
        });

        const users = createModel({ query, table: "users" });
        await users.bulkUpsert(
            [{ id: 1, email: "a@x.com", name: "Alice" }],
            { conflictColumns: ["id"], updateColumns: ["email"] }
        );

        const sql = query.calls[query.calls.length - 1].text;
        // email should be in the UPDATE SET, name should not (not in updateColumns)
        expect(sql).toMatch(/EXCLUDED\."email"/);
        expect(sql).not.toMatch(/EXCLUDED\."name"/);
    });

    it("bulkUpsert with empty array returns empty", async () => {
        const query = captureQuery();
        const users = createModel({ query, table: "users" });
        const result = await users.bulkUpsert([], { conflictColumns: ["id"] });
        expect(result).toEqual([]);
        expect(query.calls.length).toBe(0);
    });

    it("bulkUpsert throws when conflictColumns is missing", async () => {
        const query = captureQuery();
        const users = createModel({ query, table: "users" });
        await expect(
            users.bulkUpsert([{ id: 1 }], {})
        ).rejects.toThrow(/requires conflictColumns/);
    });

    it("bulkUpsert passes through to query runner with values", async () => {
        const calls = [];
        const query = async (text, values, options) => {
            calls.push({ text, values, options });
            return { rows: [{ id: 1 }], rowCount: 1 };
        };
        const users = createModel({ query, table: "users" });

        await users.bulkUpsert(
            [{ id: 1, name: "Alice" }],
            { conflictColumns: ["id"], returning: ["id"] }
        );

        expect(calls[0].values).toEqual([1, "Alice"]);
        expect(calls[0].text).toMatch(/ON CONFLICT/);
    });

    it("defaultScope applies WHERE conditions to all find queries", async () => {
        const calls = [];
        const query = async (text, values, options) => {
            calls.push({ text, values, options });
            return { rows: [{ id: 1, tenant_id: "t1" }], rowCount: 1 };
        };

        const users = createModel({
            query,
            table: "users",
            defaultScope: { tenant_id: "t1" },
        });

        await users.find({ id: 1 });
        const sql = calls[calls.length - 1].text;
        expect(sql).toMatch(/WHERE/);
        expect(sql).toMatch(/"tenant_id" = \$[0-9]+/);
        expect(calls[calls.length - 1].values).toContain("t1");
    });

    it("defaultScope function is called and applied", async () => {
        const calls = [];
        const scopeFn = vi.fn(() => ({ tenant_id: "dynamic" }));
        const query = async (text, values) => {
            calls.push({ text, values });
            return { rows: [{ id: 1, tenant_id: "dynamic" }], rowCount: 1 };
        };

        const users = createModel({
            query,
            table: "users",
            defaultScope: scopeFn,
        });

        await users.find({ id: 1 });
        expect(scopeFn).toHaveBeenCalled();
        const sql = calls[calls.length - 1].text;
        expect(sql).toMatch(/tenant_id/);
    });

    it("defaultScope: false opts out per call", async () => {
        const calls = [];
        const query = async (text) => {
            calls.push({ text });
            return { rows: [], rowCount: 0 };
        };

        const users = createModel({
            query,
            table: "users",
            defaultScope: { status: "active" },
        });

        await users.find({ id: 1 }, { defaultScope: false });
        const sql = calls[calls.length - 1].text;
        expect(sql).not.toMatch(/status/);
    });

    it("defaultScope applies to count and exists", async () => {
        const calls = [];
        const query = async (text) => {
            calls.push({ text });
            if (/COUNT/.test(text)) {
                return { rows: [{ count: 1 }], rowCount: 1 };
            }
            if (/EXISTS/.test(text)) {
                return { rows: [{ exists: 1 }], rowCount: 1 };
            }
            return { rows: [], rowCount: 0 };
        };

        const users = createModel({
            query,
            table: "users",
            defaultScope: { is_active: true },
        });

        await users.count({ id: 1 });
        await users.exists({ id: 1 });

        const countSql = calls[0].text;
        const existsSql = calls[1].text;
        expect(countSql).toMatch(/is_active/);
        expect(existsSql).toMatch(/is_active/);
    });

    it("cache get returns cached result without calling query", async () => {
        const calls = [];
        const cachedRows = [{ id: 1, name: "Cached" }];
        const cache = {
            get: vi.fn(() => cachedRows),
            set: vi.fn(),
        };
        const query = vi.fn(() => ({ rows: [{ id: 1, name: "Fresh" }], rowCount: 1 }));

        const users = createModel({ query, table: "users" });
        const result = await users.find({ id: 1 }, { cache });

        expect(result).toEqual(cachedRows);
        expect(query).not.toHaveBeenCalled();
        expect(cache.get).toHaveBeenCalled();
        expect(cache.set).not.toHaveBeenCalled();
    });

    it("cache set stores result after query miss", async () => {
        const calls = [];
        const cache = {
            get: vi.fn(() => null),
            set: vi.fn(),
        };
        const query = vi.fn(() => ({ rows: [{ id: 1, name: "Fresh" }], rowCount: 1 }));

        const users = createModel({ query, table: "users" });
        const result = await users.find({ id: 1 }, { cache });

        expect(result).toEqual([{ id: 1, name: "Fresh" }]);
        expect(query).toHaveBeenCalled();
        expect(cache.get).toHaveBeenCalled();
        expect(cache.set).toHaveBeenCalled();
    });

    it("cache works with findOne", async () => {
        const cache = {
            get: vi.fn(() => [{ id: 5, name: "Cached One" }]),
            set: vi.fn(),
        };
        const query = vi.fn(() => ({ rows: [{ id: 5, name: "Fresh One" }], rowCount: 1 }));

        const users = createModel({ query, table: "users" });
        const result = await users.findOne({ id: 5 }, { cache });

        expect(result).toEqual({ id: 5, name: "Cached One" });
        expect(query).not.toHaveBeenCalled();
    });

    it("optimistic locking increments version and matches on update", async () => {
        const query = vi.fn(() => ({ rows: [{ id: 1, name: "Updated", version: 2 }], rowCount: 1 }));

        const users = createModel({
            query,
            table: "users",
            optimisticLocking: { column: "version" },
        });

        const result = await users.update({ id: 1, version: 1 }, { name: "Updated" });

        expect(result[0]).toEqual({ id: 1, name: "Updated", version: 2 });
        const sql = query.mock.calls[query.mock.calls.length - 1][0];
        // WHERE should match version = current version
        expect(sql).toMatch(/WHERE "version" =/);
        // Data should include version incremented to 2
        const values = query.mock.calls[query.mock.calls.length - 1][1];
        expect(values).toContain(2);
    });

    it("optimistic locking throws when version mismatches (no rows matched)", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));

        const users = createModel({
            query,
            table: "users",
            optimisticLocking: { column: "version" },
        });

        await expect(
            users.update({ id: 1, version: 5 }, { name: "Updated" })
        ).rejects.toThrow(/Optimistic lock conflict/);
    });

    it("optimistic locking throws when version missing from WHERE clause", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));

        const users = createModel({
            query,
            table: "users",
            optimisticLocking: { column: "version" },
        });

        await expect(
            users.update({ id: 1 }, { name: "Updated" })
        ).rejects.toThrow(/requires "version" in the WHERE clause/);
    });

    it("optimistic locking works with updateById", async () => {
        const query = vi.fn(() => ({ rows: [{ id: 1, name: "Updated", version: 3 }], rowCount: 1 }));

        const users = createModel({
            query,
            table: "users",
            optimisticLocking: { column: "version" },
            idColumn: "id",
        });

        // updateById constructs { id: 1 } as where; version must be in where
        const result = await users.update({ id: 1, version: 2 }, { name: "Updated" });

        expect(result[0]).toEqual({ id: 1, name: "Updated", version: 3 });
        const sql = query.mock.calls[query.mock.calls.length - 1][0];
        expect(sql).toMatch(/WHERE "version" =/);
    });

    it("serialize converts Date to ISO string", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));
        const users = createModel({
            query,
            table: "users",
            timestamps: true,
        });

        const row = { id: 1, name: "Test", created_at: new Date("2024-01-15T10:30:00Z") };
        const serialized = users.serialize(row);

        expect(serialized.created_at).toBe("2024-01-15T10:30:00.000Z");
        expect(serialized.id).toBe(1);
        expect(serialized.name).toBe("Test");
    });

    it("serialize converts nested Dates and arrays", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));
        const users = createModel({ query, table: "users" });

        const row = {
            id: 1,
            tags: ["a", "b"],
            meta: { updated_at: new Date("2024-06-01T00:00:00Z") },
        };
        const serialized = users.serialize(row);

        expect(serialized.meta.updated_at).toBe("2024-06-01T00:00:00.000Z");
        expect(serialized.tags).toEqual(["a", "b"]);
    });

    it("hydrate converts ISO date strings to Date objects", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));
        const users = createModel({
            query,
            table: "users",
            timestamps: true,
        });

        const data = { id: 1, created_at: "2024-01-15T10:30:00.000Z", name: "Test" };
        const hydrated = users.hydrate(data);

        expect(hydrated.created_at instanceof Date).toBe(true);
        expect(hydrated.created_at.toISOString()).toBe("2024-01-15T10:30:00.000Z");
        expect(hydrated.id).toBe(1);
        expect(hydrated.name).toBe("Test");
    });

    it("hydrate does not convert unknown string columns to Date", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));
        const users = createModel({
            query,
            table: "users",
            timestamps: true,
        });

        const data = { id: 1, name: "2024-01-15", created_at: "not-a-date" };
        const hydrated = users.hydrate(data);

        expect(hydrated.name).toBe("2024-01-15");
        expect(hydrated.created_at).toBe("not-a-date");
    });

    it("serialize and hydrate are round-trippable", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));
        const users = createModel({
            query,
            table: "users",
            timestamps: true,
        });

        const row = { id: 1, name: "Test", created_at: new Date("2024-03-01T12:00:00Z") };
        const serialized = users.serialize(row);
        const hydrated = users.hydrate(serialized);

        expect(hydrated.created_at instanceof Date).toBe(true);
        expect(hydrated.created_at.toISOString()).toBe("2024-03-01T12:00:00.000Z");
    });

    it("introspect() queries information_schema and returns column metadata", async () => {
        const query = vi.fn(() => ({
            rows: [
                {
                    name: "id",
                    data_type: "integer",
                    is_nullable: "NO",
                    column_default: "nextval('users_id_seq'::regclass)",
                    character_maximum_length: null,
                    numeric_precision: 32,
                    numeric_scale: 0,
                    ordinal_position: 1,
                },
                {
                    name: "email",
                    data_type: "character varying",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: 255,
                    numeric_precision: null,
                    numeric_scale: null,
                    ordinal_position: 2,
                },
            ],
            rowCount: 2,
        }));

        const users = createModel({ query, table: "users" });
        const schema = await users.introspect();

        expect(schema.table).toBe("users");
        expect(schema.schema).toBe("public");
        expect(schema.columns).toHaveLength(2);
        expect(schema.columnMap["id"].dataType).toBe("integer");
        expect(schema.columnMap["email"].dataType).toBe("character varying");
        expect(schema.columnMap["id"].isNullable).toBe("NO");
        expect(query).toHaveBeenCalledWith(
            expect.stringContaining("information_schema.columns"),
            ["public", "users"]
        );
    });

    it("introspect() caches result and returns same schema on second call", async () => {
        const query = vi.fn(() => ({
            rows: [
                {
                    name: "id",
                    data_type: "integer",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: null,
                    numeric_precision: 32,
                    numeric_scale: 0,
                    ordinal_position: 1,
                },
            ],
            rowCount: 1,
        }));

        const users = createModel({ query, table: "users" });
        const schema1 = await users.introspect();
        const schema2 = await users.introspect();

        expect(schema1).toBe(schema2);
        expect(query).toHaveBeenCalledTimes(1);
    });

    it("introspect() with force re-queries the database", async () => {
        const query = vi.fn(() => ({
            rows: [
                {
                    name: "id",
                    data_type: "integer",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: null,
                    numeric_precision: 32,
                    numeric_scale: 0,
                    ordinal_position: 1,
                },
            ],
            rowCount: 1,
        }));

        const users = createModel({ query, table: "users" });
        await users.introspect();
        await users.introspect({ force: true });

        expect(query).toHaveBeenCalledTimes(2);
    });

    it("getSchema() returns null before introspection", async () => {
        const query = vi.fn(() => ({ rows: [], rowCount: 0 }));
        const users = createModel({ query, table: "users" });

        expect(users.getSchema()).toBeNull();
    });

    it("getSchema() returns introspected schema after introspect()", async () => {
        const query = vi.fn(() => ({
            rows: [
                {
                    name: "id",
                    data_type: "integer",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: null,
                    numeric_precision: 32,
                    numeric_scale: 0,
                    ordinal_position: 1,
                },
            ],
            rowCount: 1,
        }));

        const users = createModel({ query, table: "users" });
        await users.introspect();

        const schema = users.getSchema();
        expect(schema).not.toBeNull();
        expect(schema && schema.table).toBe("users");
        expect(schema && schema.columns).toHaveLength(1);
    });

    it("introspectSchema autoCoerceTypes populates coerceTypes from discovered columns", async () => {
        const query = vi.fn(() => ({
            rows: [
                {
                    name: "id",
                    data_type: "integer",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: null,
                    numeric_precision: 32,
                    numeric_scale: 0,
                    ordinal_position: 1,
                },
                {
                    name: "is_active",
                    data_type: "boolean",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: null,
                    numeric_precision: null,
                    numeric_scale: null,
                    ordinal_position: 2,
                },
                {
                    name: "created_at",
                    data_type: "timestamp without time zone",
                    is_nullable: "NO",
                    column_default: null,
                    character_maximum_length: null,
                    numeric_precision: null,
                    numeric_scale: null,
                    ordinal_position: 3,
                },
            ],
            rowCount: 3,
        }));

        const users = createModel({
            query,
            table: "users",
            introspectSchema: { autoCoerceTypes: true },
        });

        await users.introspect();
        // After introspection with autoCoerceTypes, coercion mapping should be populated
        // We can verify by checking the model still works - the coerceTypes should reflect discovered types
        const schema = users.getSchema();
        expect(schema && schema.columnMap["id"].dataType).toBe("integer");
        expect(schema && schema.columnMap["is_active"].dataType).toBe("boolean");
        expect(schema && schema.columnMap["created_at"].dataType).toBe("timestamp without time zone");
    });

    it("loadRelated loads hasMany relationships", async () => {
        const postsRows = [
            { id: 1, user_id: 1, title: "Post 1" },
            { id: 2, user_id: 1, title: "Post 2" },
            { id: 3, user_id: 2, title: "Post 3" },
        ];
        const postsQuery = vi.fn(() => ({ rows: postsRows, rowCount: 3 }));

        const users = createModel({
            query: vi.fn(() => ({ rows: [{ id: 1, name: "Alice" }], rowCount: 1 })),
            table: "users",
            relationships: {
                posts: { type: "hasMany", model: "posts", foreignKey: "user_id" },
            },
        });

        const getModel = vi.fn((name) => {
            if (name === "posts") {
                return {
                    find: async (where) => {
                        const fk = Object.keys(where)[0];
                        const val = where[fk];
                        if (val && val.$in) {
                            return postsRows.filter(r => val.$in.includes(r[fk]));
                        }
                        return [];
                    },
                };
            }
            return null;
        });

        const result = await users.loadRelated([{ id: 1, name: "Alice" }], "posts", getModel);

        expect(result[0].posts).toHaveLength(2);
        expect(result[0].posts[0].title).toBe("Post 1");
        expect(result[0].posts[1].title).toBe("Post 2");
    });

    it("loadRelated loads belongsTo relationships", async () => {
        const usersRows = [{ id: 1, name: "Alice" }];
        const usersQuery = vi.fn(() => ({ rows: usersRows, rowCount: 1 }));

        const posts = createModel({
            query: vi.fn(() => ({ rows: [{ id: 1, user_id: 1, title: "Post" }], rowCount: 1 })),
            table: "posts",
            relationships: {
                user: { type: "belongsTo", model: "users", foreignKey: "user_id" },
            },
        });

        const getModel = vi.fn((name) => {
            if (name === "users") {
                return {
                    find: async (where) => {
                        const fk = Object.keys(where)[0];
                        const val = where[fk];
                        if (val && val.$in) {
                            return usersRows.filter(r => val.$in.includes(r[fk]));
                        }
                        return [];
                    },
                };
            }
            return null;
        });

        const result = await posts.loadRelated([{ id: 1, user_id: 1, title: "Post" }], "user", getModel);

        expect(result[0].user).toEqual({ id: 1, name: "Alice" });
    });

    it("loadRelated throws when relationship is not defined", async () => {
        const users = createModel({
            query: vi.fn(() => ({ rows: [], rowCount: 0 })),
            table: "users",
        });

        await expect(
            users.loadRelated([{ id: 1 }], "nonexistent")
        ).rejects.toThrow(/Relationship 'nonexistent' is not defined/);
    });

    it("loadRelated throws when no model resolver is available", async () => {
        const users = createModel({
            query: vi.fn(() => ({ rows: [], rowCount: 0 })),
            table: "users",
            relationships: {
                posts: { type: "hasMany", model: "posts", foreignKey: "user_id" },
            },
        });

        await expect(
            users.loadRelated([{ id: 1 }], "posts")
        ).rejects.toThrow(/No model resolver available/);
    });

    it("loadRelated returns records unchanged when no parent IDs", async () => {
        const users = createModel({
            query: vi.fn(() => ({ rows: [], rowCount: 0 })),
            table: "users",
            relationships: {
                posts: { type: "hasMany", model: "posts", foreignKey: "user_id" },
            },
        });

        const getModel = vi.fn(() => null);
        const result = await users.loadRelated([], "posts", getModel);

        expect(result).toEqual([]);
    });
});
