import { describe, it, expect, beforeEach, vi } from "vitest";

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

function captureAtomicQuery(resultFactory) {
    const calls = [];
    const clientCalls = [];
    let released = false;
    let beginCount = 0;
    let commitCount = 0;
    let rollbackCount = 0;
    const client = {
        query: async (text) => {
            const upper = String(text).trim().toUpperCase();
            if (upper === "BEGIN") { beginCount++; return { rows: [], rowCount: 0 }; }
            if (upper === "COMMIT") { commitCount++; return { rows: [], rowCount: 0 }; }
            if (upper === "ROLLBACK") { rollbackCount++; return { rows: [], rowCount: 0 }; }
            clientCalls.push({ text });
            return resultFactory ? resultFactory(text) : { rows: [], rowCount: 0 };
        },
        release: () => { released = true; },
    };
    const query = async (text, values, options) => {
        calls.push({ text, values, options });
        return resultFactory ? resultFactory(text, values) : { rows: [], rowCount: 0 };
    };
    query.getClient = async () => client;
    query.releaseClient = (c) => { released = true; };
    query.calls = calls;
    query.clientCalls = clientCalls;
    query.counts = () => ({ begin: beginCount, commit: commitCount, rollback: rollbackCount });
    query.released = () => released;
    return query;
}

describe("Enhanced Model Layer", () => {
    let query;
    let users;

    beforeEach(() => {
        query = captureQuery((text) => {
            if (/COUNT/.test(text)) {
                return { rows: [{ count: 4 }], rowCount: 1 };
            }
            if (/SELECT 1 AS exists/.test(text)) {
                return { rows: [{ exists: 1 }], rowCount: 1 };
            }
            if (/INSERT/.test(text)) {
                return { rows: [{ id: 1, name: "test", created_at: "2024-01-01", updated_at: "2024-01-01" }], rowCount: 1 };
            }
            if (/UPDATE/.test(text)) {
                return { rows: [{ id: 1, name: "updated", updated_at: "2024-01-02" }], rowCount: 1 };
            }
            return { rows: [{ id: 1, name: "test", status: "active" }], rowCount: 1 };
        });
        users = createModel({ query, table: "users" });
    });

    describe("Complex WHERE Conditions", () => {
        it("supports comparison operators ($gt, $lt, $gte, $lte)", async () => {
            await users.find({ age: { $gt: 25, $lt: 65 } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"age" > $1');
            expect(sql).toContain('"age" < $2');
            expect(query.calls[0].values).toEqual([25, 65]);
        });

        it("supports $ne operator", async () => {
            await users.find({ status: { $ne: "inactive" } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"status" != $1');
            expect(query.calls[0].values).toEqual(["inactive"]);
        });

        it("supports $in operator", async () => {
            await users.find({ id: { $in: [1, 2, 3] } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"id" IN ($1, $2, $3)');
            expect(query.calls[0].values).toEqual([1, 2, 3]);
        });

        it("supports $nin operator", async () => {
            await users.find({ id: { $nin: [1, 2, 3] } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"id" NOT IN ($1, $2, $3)');
            expect(query.calls[0].values).toEqual([1, 2, 3]);
        });

        it("supports $like operator", async () => {
            await users.find({ name: { $like: "%John%" } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"name" LIKE $1');
            expect(query.calls[0].values).toEqual(["%John%"]);
        });

        it("supports $ilike operator", async () => {
            await users.find({ name: { $ilike: "%john%" } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"name" ILIKE $1');
            expect(query.calls[0].values).toEqual(["%john%"]);
        });

        it("supports $null operator", async () => {
            await users.find({ deleted_at: { $null: true } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"deleted_at" IS NULL');
        });

        it("supports $null: false operator", async () => {
            await users.find({ deleted_at: { $null: false } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"deleted_at" IS NOT NULL');
        });

        it("supports AND groups", async () => {
            await users.find([
                { status: "active" },
                { AND: [{ age: { $gt: 25 } }, { age: { $lt: 65 } }] }
            ]);
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"status" = $1');
            expect(sql).toContain('"age" > $2');
            expect(sql).toContain('"age" < $3');
        });

        it("supports OR groups", async () => {
            await users.find([
                { status: "active" },
                { OR: [{ role: "admin" }, { role: "superuser" }] }
            ]);
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"status" = $1');
            expect(sql).toContain('"role" = $2 OR "role" = $3');
        });
    });

    describe("Soft Delete Support", () => {
        it("filters out soft-deleted records by default", async () => {
            const softDeleteUsers = createModel({
                query,
                table: "users",
                softDelete: true,
                softDeleteField: "deleted_at"
            });
            
            await softDeleteUsers.find({});
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"deleted_at" IS NULL');
        });

        it("includes soft-deleted records with withDeleted option", async () => {
            const softDeleteUsers = createModel({
                query,
                table: "users",
                softDelete: true,
                softDeleteField: "deleted_at"
            });
            
            await softDeleteUsers.findWithDeleted({});
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).not.toContain('"deleted_at" IS NULL');
        });

        it("performs soft delete instead of hard delete", async () => {
            const sq = captureQuery((text) => {
                if (/UPDATE/.test(text)) {
                    return { rows: [{ id: 1, deleted_at: "2024-01-01" }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
            const softDeleteUsers = createModel({
                query: sq,
                table: "users",
                softDelete: true,
                softDeleteField: "deleted_at"
            });
            
            const result = await softDeleteUsers.deleteById(1);
            
            expect(sq.calls.length).toBe(1);
            const sql = sq.calls[0].text;
            expect(sql).toContain("UPDATE");
            expect(sql).toContain('"deleted_at" = $1');
            expect(sql).not.toContain("DELETE");
            expect(result).toEqual({ id: 1, deleted_at: "2024-01-01" });
        });

        it("performs hard delete with forceDelete option", async () => {
            const sq = captureQuery((text) => {
                if (/DELETE/.test(text)) {
                    return { rows: [{ id: 1 }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
            const softDeleteUsers = createModel({
                query: sq,
                table: "users",
                softDelete: true,
                softDeleteField: "deleted_at"
            });
            
            const result = await softDeleteUsers.forceDeleteById(1);
            
            expect(sq.calls.length).toBe(1);
            const sql = sq.calls[0].text;
            expect(sql).toContain("DELETE");
            expect(result).toEqual({ id: 1 });
        });

        it("restores soft-deleted records", async () => {
            const sq = captureQuery((text) => {
                if (/UPDATE/.test(text)) {
                    return { rows: [{ id: 1, deleted_at: null }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
            const softDeleteUsers = createModel({
                query: sq,
                table: "users",
                softDelete: true,
                softDeleteField: "deleted_at"
            });
            
            const result = await softDeleteUsers.restoreById(1);
            
            expect(sq.calls.length).toBe(1);
            const sql = sq.calls[0].text;
            expect(sql).toContain("UPDATE");
            expect(sql).toContain('"deleted_at" = $1');
            expect(sq.calls[0].values).toEqual([null, 1]);
            expect(result).toEqual({ id: 1, deleted_at: null });
        });

        it("throws error when restore is called without softDelete enabled", async () => {
            const regularUsers = createModel({ query, table: "users" });
            
            await expect(regularUsers.restoreById(1)).rejects.toThrow("restore() requires softDelete to be enabled");
        });
    });

    describe("Automatic Timestamps", () => {
        it("automatically sets created_at and updated_at on insert", async () => {
            const sq = captureQuery((text) => {
                if (/INSERT/.test(text)) {
                    return { rows: [{ id: 1, name: "test", created_at: "2024-01-01T00:00:00.000Z", updated_at: "2024-01-01T00:00:00.000Z" }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
            const timestampUsers = createModel({
                query: sq,
                table: "users",
                timestamps: true,
                createdAtField: "created_at",
                updatedAtField: "updated_at"
            });
            
            await timestampUsers.insert({ name: "test" });
            
            expect(sq.calls.length).toBe(1);
            const sql = sq.calls[0].text;
            expect(sql).toContain("created_at");
            expect(sql).toContain("updated_at");
        });

        it("automatically updates updated_at on update", async () => {
            const sq = captureQuery((text) => {
                if (/UPDATE/.test(text)) {
                    return { rows: [{ id: 1, name: "updated", updated_at: "2024-01-02T00:00:00.000Z" }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
            const timestampUsers = createModel({
                query: sq,
                table: "users",
                timestamps: true,
                createdAtField: "created_at",
                updatedAtField: "updated_at"
            });
            
            await timestampUsers.updateById(1, { name: "updated" });
            
            expect(sq.calls.length).toBe(1);
            const sql = sq.calls[0].text;
            expect(sql).toContain("updated_at");
            expect(sql).not.toContain("created_at");
        });
    });

    describe("Lifecycle Hooks", () => {
        it("runs beforeCreate hook", async () => {
            const beforeCreate = vi.fn((data) => ({ ...data, processed: true }));
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/INSERT/.test(text)) {
                        return { rows: [{ id: 1, name: "test", processed: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { beforeCreate }
            });
            
            await hookUsers.insert({ name: "test" });
            
            expect(beforeCreate).toHaveBeenCalledWith({ name: "test" }, undefined);
        });

        it("runs afterCreate hook", async () => {
            const afterCreate = vi.fn((result) => result);
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/INSERT/.test(text)) {
                        return { rows: [{ id: 1, name: "test" }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { afterCreate }
            });
            
            await hookUsers.insert({ name: "test" });
            
            expect(afterCreate).toHaveBeenCalledWith({ id: 1, name: "test" }, undefined);
        });

        it("runs beforeUpdate hook with where clause", async () => {
            const beforeUpdate = vi.fn((data, where) => ({ ...data, processed: true }));
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/UPDATE/.test(text)) {
                        return { rows: [{ id: 1, name: "updated", processed: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { beforeUpdate }
            });
            
            await hookUsers.updateById(1, { name: "updated" });
            
            expect(beforeUpdate).toHaveBeenCalledWith({ name: "updated" }, { id: 1 });
        });

        it("runs afterUpdate hook with where clause", async () => {
            const afterUpdate = vi.fn((result, where) => result);
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/UPDATE/.test(text)) {
                        return { rows: [{ id: 1, name: "updated" }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { afterUpdate }
            });
            
            await hookUsers.updateById(1, { name: "updated" });
            
            expect(afterUpdate).toHaveBeenCalledWith({ id: 1, name: "updated" }, { id: 1 });
        });

        it("runs beforeDelete hook", async () => {
            const beforeDelete = vi.fn((where) => where);
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/DELETE/.test(text)) {
                        return { rows: [{ id: 1 }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { beforeDelete }
            });
            
            await hookUsers.deleteById(1);
            
            expect(beforeDelete).toHaveBeenCalledWith({ id: 1 }, undefined);
        });

        it("runs afterDelete hook", async () => {
            const afterDelete = vi.fn((result, where) => result);
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/DELETE/.test(text)) {
                        return { rows: [{ id: 1 }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { afterDelete }
            });
            
            await hookUsers.deleteById(1);
            
            expect(afterDelete).toHaveBeenCalledWith({ id: 1 }, { id: 1 });
        });

        it("hooks can modify data", async () => {
            const beforeCreate = vi.fn((data) => ({ ...data, modified: true }));
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/INSERT/.test(text)) {
                        return { rows: [{ id: 1, name: "test", modified: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { beforeCreate }
            });
            
            const result = await hookUsers.insert({ name: "test" });
            
            expect(beforeCreate).toHaveBeenCalledWith({ name: "test" }, undefined);
            expect(result?.modified).toBe(true);
        });

        it("async hooks are supported", async () => {
            const beforeCreate = vi.fn().mockResolvedValue({ name: "test", processed: true });
            const hookUsers = createModel({
                query: captureQuery((text) => {
                    if (/INSERT/.test(text)) {
                        return { rows: [{ id: 1, name: "test", processed: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                hooks: { beforeCreate }
            });
            
            const result = await hookUsers.insert({ name: "test" });
            
            expect(beforeCreate).toHaveBeenCalledWith({ name: "test" }, undefined);
            expect(result?.processed).toBe(true);
        });
    });

    describe("Validation", () => {
        it("runs validation on insert", async () => {
            const validate = vi.fn((data) => ({ ...data, validated: true }));
            const validatedUsers = createModel({
                query: captureQuery((text) => {
                    if (/INSERT/.test(text)) {
                        return { rows: [{ id: 1, name: "test", validated: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                validate
            });
            
            await validatedUsers.insert({ name: "test" });
            
            expect(validate).toHaveBeenCalledWith({ name: "test" }, "create");
        });

        it("runs validation on update", async () => {
            const validate = vi.fn((data) => ({ ...data, validated: true }));
            const validatedUsers = createModel({
                query: captureQuery((text) => {
                    if (/UPDATE/.test(text)) {
                        return { rows: [{ id: 1, name: "updated", validated: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                validate
            });
            
            await validatedUsers.updateById(1, { name: "updated" });
            
            expect(validate).toHaveBeenCalledWith({ name: "updated" }, "update");
        });

        it("validation can throw errors", async () => {
            const validate = vi.fn(() => {
                throw new Error("Invalid data");
            });
            const validatedUsers = createModel({
                query: captureQuery(),
                table: "users",
                validate
            });
            
            await expect(validatedUsers.insert({ name: "test" })).rejects.toThrow("Invalid data");
            expect(validate).toHaveBeenCalled();
        });

        it("async validation is supported", async () => {
            const validate = vi.fn().mockResolvedValue({ name: "test", validated: true });
            const validatedUsers = createModel({
                query: captureQuery((text) => {
                    if (/INSERT/.test(text)) {
                        return { rows: [{ id: 1, name: "test", validated: true }], rowCount: 1 };
                    }
                    return { rows: [], rowCount: 0 };
                }),
                table: "users",
                validate
            });
            
            const result = await validatedUsers.insert({ name: "test" });
            
            expect(validate).toHaveBeenCalledWith({ name: "test" }, "create");
            expect(result?.validated).toBe(true);
        });
    });

    describe("Atomic insertMany", () => {
        it("uses transaction for atomic inserts when client is provided", async () => {
            const client = {
                query: vi.fn().mockResolvedValue({ rows: [{ id: 1 }, { id: 2 }] })
            };
            
            const atomicUsers = createModel({
                query: captureQuery(),
                table: "users"
            });
            
            await atomicUsers.insertMany(
                [{ name: "user1" }, { name: "user2" }],
                { client, atomic: true }
            );
            
            expect(client.query).toHaveBeenCalled();
        });

        it("throws error when atomic is true but no client is provided", async () => {
            const atomicUsers = createModel({ query, table: "users" });
            
            await expect(
                atomicUsers.insertMany([{ name: "user1" }, { name: "user2" }], { atomic: true })
            ).rejects.toThrow("Atomic insertMany requires a transaction client");
        });

        it("uses non-atomic mode when atomic is false", async () => {
            const sq = captureQuery((text) => {
                if (/INSERT/.test(text)) {
                    return { rows: [{ id: 1 }, { id: 2 }], rowCount: 2 };
                }
                return { rows: [], rowCount: 0 };
            });
            const nonAtomicUsers = createModel({
                query: sq,
                table: "users"
            });
            
            await nonAtomicUsers.insertMany(
                [{ name: "user1" }, { name: "user2" }],
                { atomic: false }
            );
            
            // Non-atomic should still work
            expect(sq.calls.length).toBeGreaterThan(0);
        });
    });

    describe("Configuration Getters", () => {
        it("exposes softDeleteEnabled getter", () => {
            const softDeleteUsers = createModel({
                query,
                table: "users",
                softDelete: true
            });
            
            expect(softDeleteUsers.softDeleteEnabled).toBe(true);
        });

        it("exposes timestampsEnabled getter", () => {
            const timestampUsers = createModel({
                query,
                table: "users",
                timestamps: true
            });
            
            expect(timestampUsers.timestampsEnabled).toBe(true);
        });

        it("exposes hooks getter", () => {
            const hooks = { beforeCreate: () => {} };
            const hookUsers = createModel({
                query,
                table: "users",
                hooks
            });
            
            expect(hookUsers.hooks).toEqual(hooks);
        });

        it("exposes validate getter", () => {
            const validate = () => {};
            const validatedUsers = createModel({
                query,
                table: "users",
                validate
            });
            
            expect(validatedUsers.validate).toBe(validate);
        });
    });

    describe("Count and Exists with Complex WHERE", () => {
        it("supports complex WHERE in count", async () => {
            await users.count({ age: { $gt: 25 } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"age" > $1');
            expect(sql).toContain("COUNT");
        });

        it("supports complex WHERE in exists", async () => {
            await users.exists({ age: { $gt: 25 } });
            
            expect(query.calls.length).toBe(1);
            const sql = query.calls[0].text;
            expect(sql).toContain('"age" > $1');
            expect(sql).toContain("1 AS exists");
        });
    });

    describe("Automatic Transactions (atomic)", () => {
        let aq;
        let atomicUsers;

        beforeEach(() => {
            aq = captureAtomicQuery((text) => {
                if (/INSERT/.test(text)) {
                    return { rows: [{ id: 1, name: "test" }], rowCount: 1 };
                }
                if (/UPDATE/.test(text)) {
                    return { rows: [{ id: 1, name: "updated" }], rowCount: 1 };
                }
                if (/DELETE/.test(text)) {
                    return { rows: [{ id: 1 }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
            atomicUsers = createModel({ query: aq, table: "users" });
        });

        it("insert wraps in BEGIN/COMMIT when atomic and no client", async () => {
            await atomicUsers.insert({ name: "test" }, { atomic: true });

            const { begin, commit, rollback } = aq.counts();
            expect(begin).toBe(1);
            expect(commit).toBe(1);
            expect(rollback).toBe(0);
            expect(aq.released()).toBe(true);
            // The INSERT ran on the transaction client, not the top-level query.
            expect(aq.clientCalls.some((c) => /INSERT/.test(c.text))).toBe(true);
            expect(aq.calls.length).toBe(0);
        });

        it("insert rolls back and releases on error", async () => {
            const failing = captureAtomicQuery(() => {
                throw new Error("insert failed");
            });
            const model = createModel({ query: failing, table: "users" });

            await expect(model.insert({ name: "test" }, { atomic: true })).rejects.toThrow("insert failed");

            const { begin, commit, rollback } = failing.counts();
            expect(begin).toBe(1);
            expect(commit).toBe(0);
            expect(rollback).toBe(1);
            expect(failing.released()).toBe(true);
        });

        it("insert does not open a transaction by default (atomic false)", async () => {
            await atomicUsers.insert({ name: "test" });

            expect(aq.counts().begin).toBe(0);
            expect(aq.counts().commit).toBe(0);
            expect(aq.calls.length).toBe(1);
            expect(aq.calls[0].text).toContain("INSERT");
        });

        it("insert joins an existing client without BEGIN/COMMIT", async () => {
            const client = {
                query: async (text) => ({ rows: [{ id: 1 }], rowCount: 1 }),
            };
            await atomicUsers.insert({ name: "test" }, { client, atomic: true });

            expect(aq.counts().begin).toBe(0);
            expect(aq.counts().commit).toBe(0);
            // Used the provided client, no top-level query call.
            expect(aq.calls.length).toBe(0);
        });

        it("update wraps in BEGIN/COMMIT when atomic", async () => {
            await atomicUsers.update({ id: 1 }, { name: "updated" }, { atomic: true });

            const { begin, commit, rollback } = aq.counts();
            expect(begin).toBe(1);
            expect(commit).toBe(1);
            expect(rollback).toBe(0);
            expect(aq.released()).toBe(true);
            expect(aq.clientCalls.some((c) => /UPDATE/.test(c.text))).toBe(true);
        });

        it("delete wraps in BEGIN/COMMIT when atomic", async () => {
            await atomicUsers.delete({ id: 1 }, { atomic: true });

            const { begin, commit, rollback } = aq.counts();
            expect(begin).toBe(1);
            expect(commit).toBe(1);
            expect(rollback).toBe(0);
            expect(aq.released()).toBe(true);
            expect(aq.clientCalls.some((c) => /DELETE/.test(c.text))).toBe(true);
        });
    });

    describe("JOIN Support", () => {
        let jq;
        let users;

        beforeEach(() => {
            jq = captureQuery((text) => {
                if (/INNER JOIN/.test(text) || /LEFT JOIN/.test(text)) {
                    return { rows: [{ id: 1, name: "test", order_id: 10, total: 99.99 }], rowCount: 1 };
                }
                return { rows: [{ id: 1, name: "test" }], rowCount: 1 };
            });
            users = createModel({ query: jq, table: "users" });
        });

        it("innerJoin returns a QueryBuilder with JOIN clause", () => {
            const qb = users.innerJoin("orders", "users.id", "orders.user_id");
            const sql = qb.toSQL().text;
            expect(sql).toContain("INNER JOIN");
            expect(sql).toContain('"orders"');
        });

        it("leftJoin returns a QueryBuilder with JOIN clause", () => {
            const qb = users.leftJoin("orders", "users.id = orders.user_id");
            const sql = qb.toSQL().text;
            expect(sql).toContain("LEFT JOIN");
            expect(sql).toContain('"orders"');
        });

        it("find with queryBuilder executes JOIN query", async () => {
            const qb = users.innerJoin("orders", "users.id", "orders.user_id");
            await users.find({}, { queryBuilder: qb });

            expect(jq.calls.length).toBe(1);
            const sql = jq.calls[0].text;
            expect(sql).toContain("INNER JOIN");
            expect(sql).toContain('"orders"');
        });

        it("find with queryBuilder and WHERE conditions", async () => {
            const qb = users.innerJoin("orders", "users.id", "orders.user_id");
            await users.find({ "orders.total": { $gt: 50 } }, { queryBuilder: qb });

            expect(jq.calls.length).toBe(1);
            const sql = jq.calls[0].text;
            expect(sql).toContain("INNER JOIN");
            expect(sql).toContain('"total" >');
        });

        it("count with queryBuilder executes JOIN count query", async () => {
            const qb = users.leftJoin("orders", "users.id = orders.user_id");
            await users.count({}, { queryBuilder: qb });

            expect(jq.calls.length).toBe(1);
            const sql = jq.calls[0].text;
            expect(sql).toContain("LEFT JOIN");
            expect(sql).toContain("COUNT");
        });

        it("join method is exposed on model", () => {
            expect(typeof users.join).toBe("function");
            expect(typeof users.innerJoin).toBe("function");
            expect(typeof users.leftJoin).toBe("function");
            expect(typeof users.rightJoin).toBe("function");
            expect(typeof users.fullJoin).toBe("function");
            expect(typeof users.crossJoin).toBe("function");
        });

        it("crossJoin returns a QueryBuilder with CROSS JOIN clause", () => {
            const qb = users.crossJoin("categories");
            const sql = qb.toSQL().text;
            expect(sql).toContain("CROSS JOIN");
        });
    });

    describe("Type Coercion", () => {
        let cq;
        let coerceUsers;

        beforeEach(() => {
            cq = captureQuery((text) => {
                if (/INSERT/.test(text)) {
                    return { rows: [{ id: 1 }], rowCount: 1 };
                }
                if (/UPDATE/.test(text)) {
                    return { rows: [{ id: 1 }], rowCount: 1 };
                }
                return { rows: [], rowCount: 0 };
            });
        });

        it("does not coerce by default", async () => {
            coerceUsers = createModel({ query: cq, table: "users" });
            await coerceUsers.insert({ age: "25", active: "true" });

            const sql = cq.calls[0].text;
            // Values are passed as parameters, check the values array
            expect(cq.calls[0].values).toContain("25");
            expect(cq.calls[0].values).toContain("true");
        });

        it("coerces string to number with coerceTypes", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
                coerceTypes: { age: 'number' },
            });
            await coerceUsers.insert({ age: "25" });

            expect(cq.calls[0].values).toContain(25);
            expect(typeof cq.calls[0].values.find(v => v === 25)).toBe('number');
        });

        it("coerces string to boolean with coerceTypes", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
                coerceTypes: { active: 'boolean' },
            });
            await coerceUsers.insert({ active: "true" });

            expect(cq.calls[0].values).toContain(true);
        });

        it("coerces string to date with coerceTypes", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
                coerceTypes: { created_at: 'date' },
            });
            await coerceUsers.insert({ created_at: "2024-01-15" });

            const val = cq.calls[0].values.find(v => v instanceof Date);
            expect(val).toBeInstanceOf(Date);
        });

        it("coerces update data with coerceTypes", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
                coerceTypes: { age: 'number' },
            });
            await coerceUsers.update({ id: 1 }, { age: "30" });

            expect(cq.calls[0].values).toContain(30);
        });

        it("heuristic coercion converts numeric strings", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
            });
            await coerceUsers.insert({ age: "42", score: "3.14" });

            expect(cq.calls[0].values).toContain(42);
            expect(cq.calls[0].values).toContain(3.14);
        });

        it("heuristic coercion converts boolean strings", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
            });
            await coerceUsers.insert({ active: "true", flag: "false" });

            expect(cq.calls[0].values).toContain(true);
            expect(cq.calls[0].values).toContain(false);
        });

        it("heuristic coercion converts ISO date strings", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerce: true,
            });
            await coerceUsers.insert({ created_at: "2024-06-01" });

            const val = cq.calls[0].values.find(v => v instanceof Date);
            expect(val).toBeInstanceOf(Date);
        });

        it("does not coerce when coerce is false even with coerceTypes", async () => {
            coerceUsers = createModel({
                query: cq,
                table: "users",
                coerceTypes: { age: 'number' },
            });
            await coerceUsers.insert({ age: "25" });

            expect(cq.calls[0].values).toContain("25");
        });

        it("exposes coerceEnabled getter", () => {
            const coercing = createModel({ query: cq, table: "users", coerce: true });
            const nonCoercing = createModel({ query: cq, table: "users" });

            expect(coercing.coerceEnabled).toBe(true);
            expect(nonCoercing.coerceEnabled).toBe(false);
        });
    });
});
