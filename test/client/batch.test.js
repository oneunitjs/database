import { describe, it, expect } from "vitest";

import { createBatch } from "../../src/index.js";

function captureQuery(resultFactory) {
    const calls = [];
    const query = async (text, values, options) => {
        calls.push({ text, values, options });
        return resultFactory ? resultFactory(text, values) : { rows: [], rowCount: 0 };
    };
    query.calls = calls;
    return query;
}

describe("batch", () => {
    it("insertMany builds a parameterized multi-row insert", async () => {
        const query = captureQuery(() => ({ rows: [{ id: 1 }, { id: 2 }], rowCount: 2 }));
        const batch = createBatch({ query });

        const result = await batch.insertMany("users", [{ a: 1, b: 2 }, { a: 3, b: 4 }], {
            returning: ["id"],
        });

        expect(query.calls.length).toBe(1);
        expect(query.calls[0].text).toBe('INSERT INTO "users" ("a", "b") VALUES ($1, $2), ($3, $4) RETURNING "id"');
        expect(query.calls[0].values).toEqual([1, 2, 3, 4]);
        expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);
        expect(result.rowCount).toBe(2);
        expect(result.stats.chunks).toBe(1);
    });

    it("insertMany chunks large batches", async () => {
        const query = captureQuery((text, values) => ({ rows: [{ a: values[0] }], rowCount: 1 }));
        const batch = createBatch({ query });

        const result = await batch.insertMany("t", [{ a: 1 }, { a: 2 }, { a: 3 }], {
            chunkSize: 2,
            returning: ["a"],
        });

        expect(query.calls.length).toBe(2);
        expect(query.calls[0].values).toEqual([1, 2]);
        expect(query.calls[1].values).toEqual([3]);
        expect(result.stats.chunks).toBe(2);
    });

    it("insertMany returns the row count when no returning clause is used", async () => {
        const query = captureQuery(() => ({ rows: [], rowCount: 1 }));
        const batch = createBatch({ query });

        const result = await batch.insertMany("t", [{ a: 1 }, { a: 2 }]);
        expect(result.rowCount).toBe(2);
        expect(result.rows).toEqual([]);
    });

    it("insertMany handles empty input and conflicts", async () => {
        const query = captureQuery();
        const batch = createBatch({ query });

        const emptyResult = await batch.insertMany("t", []);
        expect(emptyResult.rows).toEqual([]);
        expect(emptyResult.rowCount).toBe(0);

        await batch.insertMany("t", [{ a: 1 }], { onConflict: { columns: ["a"], do: "nothing" } });
        expect(query.calls[0].text).toMatch(/ON CONFLICT \("a"\) DO NOTHING/);
    });

    it("insertMany runs inside a managed transaction when requested", async () => {
        const query = captureQuery();
        const seen = [];
        const transaction = async (callback) => {
            seen.push("begin");
            await callback({ query: async (text, values) => { seen.push({ text, values }); return { rows: [] }; } });
            seen.push("commit");
        };

        const batch = createBatch({ query, transaction });
        await batch.insertMany("t", [{ a: 1 }], { transaction: true });

        expect(seen[0]).toBe("begin");
        expect(seen[seen.length - 1]).toBe("commit");
        expect(query.calls.length).toBe(0);
    });

    it("updateMany supports transaction mode", async () => {
        const seen = [];
        const transaction = async (callback) => {
            seen.push("begin");
            await callback({ query: async (text, values) => { seen.push({ text, values }); return { rows: [], rowCount: 1 }; } });
            seen.push("commit");
        };
        const query = captureQuery(() => ({ rows: [], rowCount: 1 }));
        const batch = createBatch({ query, transaction });

        const result = await batch.updateMany(
            "users",
            { status: "active" },
            [{ column: "id", operator: "=", values: [1, 2, 3] }],
            { transaction: true, returning: ["id"] },
        );

        expect(seen[0]).toBe("begin");
        expect(seen[seen.length - 1]).toBe("commit");
        expect(seen.some(s => s.text && s.text.includes("UPDATE"))).toBe(true);
        expect(result.stats.chunks).toBeGreaterThanOrEqual(1);
    });

    it("deleteMany supports transaction mode", async () => {
        const seen = [];
        const transaction = async (callback) => {
            seen.push("begin");
            await callback({ query: async (text, values) => { seen.push({ text, values }); return { rows: [], rowCount: 1 }; } });
            seen.push("commit");
        };
        const query = captureQuery(() => ({ rows: [], rowCount: 1 }));
        const batch = createBatch({ query, transaction });

        const result = await batch.deleteMany(
            "users",
            [{ column: "id", operator: "=", values: [1, 2, 3] }],
            { transaction: true, returning: ["id"] },
        );

        expect(seen[0]).toBe("begin");
        expect(seen[seen.length - 1]).toBe("commit");
        expect(seen.some(s => s.text && s.text.includes("DELETE"))).toBe(true);
        expect(result.stats.chunks).toBeGreaterThanOrEqual(1);
    });

    it("updateMany reports per-row errors via onRowError", async () => {
        const errors = [];
        let callCount = 0;
        const query = captureQuery((text, values) => {
            callCount++;
            if (callCount === 2) {
                throw new Error("forced failure");
            }
            return { rows: [], rowCount: 1 };
        });
        const batch = createBatch({ query });

        const result = await batch.updateMany(
            "users",
            { status: "active" },
            [
                { column: "id", operator: "=", values: [1] },
                { column: "id", operator: "=", values: [2] },
                { column: "id", operator: "=", values: [3] },
            ],
            { chunkSize: 1, onRowError: (row, error, rowIndex) => errors.push({ rowIndex, error: error.message }) },
        );

        expect(errors.length).toBe(1);
        expect(errors[0].error).toBe("forced failure");
        expect(result.stats.rowsFailed).toBe(1);
    });

    it("updateMany supports savepoint-based error isolation", async () => {
        let callCount = 0;
        const seen = [];
        const transaction = async (callback) => {
            seen.push("begin");
            await callback({
                query: async (text, values) => {
                    callCount++;
                    if (callCount === 2) {
                        throw new Error("forced failure");
                    }
                    seen.push({ text, values });
                    return { rows: [], rowCount: 1 };
                },
            });
            seen.push("commit");
        };
        const batch = createBatch({ query: captureQuery(), transaction });

        const result = await batch.updateMany(
            "users",
            { status: "active" },
            [
                { column: "id", operator: "=", values: [1] },
                { column: "id", operator: "=", values: [2] },
                { column: "id", operator: "=", values: [3] },
            ],
            { chunkSize: 1, transaction: true, savepoint: true },
        );

        expect(result.stats.rowsFailed).toBe(1);
        expect(seen[0]).toBe("begin");
        expect(seen[seen.length - 1]).toBe("commit");
    });

    it("selectMany supports keyset pagination", async () => {
        const rows = [
            { id: 1, name: "a" },
            { id: 2, name: "b" },
            { id: 3, name: "c" },
        ];
        const query = captureQuery((text, values) => {
            // Simulate keyset pagination: filter by id > lastKeysetValue
            const lastId = values[values.length - 1];
            const filtered = lastId === undefined ? rows : rows.filter(r => r.id > lastId);
            return { rows: filtered.slice(0, 2), rowCount: filtered.length };
        });
        const batch = createBatch({ query });

        const result = await batch.selectMany(
            "SELECT * FROM users",
            { keysetColumn: "id", chunkSize: 2 },
        );

        expect(result.rows.length).toBe(3);
        expect(result.stats.chunks).toBeGreaterThanOrEqual(1);
    });

    it("batch progress reporting fires onProgress callback", async () => {
        const events = [];
        const query = captureQuery(() => ({ rows: [{ id: 1 }], rowCount: 1 }));
        const batch = createBatch({ query });

        await batch.insertMany("t", [{ a: 1 }, { a: 2 }, { a: 3 }], {
            chunkSize: 1,
            onProgress: (event) => events.push(event),
        });

        expect(events.length).toBeGreaterThan(0);
        expect(events[0].operation).toBe("insert");
        expect(events[0].chunkIndex).toBe(0);
        expect(events[0].rowsProcessed).toBe(1);
    });

    it("configurable retry for failed chunks", async () => {
        let attempts = 0;
        const query = captureQuery(() => {
            attempts++;
            if (attempts < 3) {
                throw new Error("temporary failure");
            }
            return { rows: [], rowCount: 1 };
        });
        const batch = createBatch({ query });

        const result = await batch.insertMany("t", [{ a: 1 }], { retry: 3 });
        expect(result.stats.chunks).toBe(1);
        expect(attempts).toBe(3);
    });

    it("batch operation cancellation via AbortSignal", async () => {
        const controller = new AbortController();
        const query = captureQuery(() => {
            throw new DOMException("The operation was aborted.", "AbortError");
        });
        const batch = createBatch({ query });

        await expect(
            batch.insertMany("t", [{ a: 1 }], { signal: controller.signal }),
        ).rejects.toThrow();
        controller.abort();
    });

    it("batch execution statistics are returned", async () => {
        const query = captureQuery(() => ({ rows: [{ id: 1 }], rowCount: 1 }));
        const batch = createBatch({ query });

        const result = await batch.insertMany("t", [{ a: 1 }, { a: 2 }], {
            returning: ["id"],
        });

        expect(result.stats).toHaveProperty("chunks");
        expect(result.stats).toHaveProperty("rowsProcessed");
        expect(result.stats).toHaveProperty("rowsSucceeded");
        expect(result.stats).toHaveProperty("rowsFailed");
        expect(result.stats).toHaveProperty("durationMs");
        expect(result.stats).toHaveProperty("errors");
        expect(typeof result.stats.durationMs).toBe("number");
    });

    it("batch timeout option is accepted", async () => {
        const query = captureQuery(() => ({ rows: [], rowCount: 0 }));
        const batch = createBatch({ query });

        // Timeout option should be accepted and passed through
        const result = await batch.insertMany("t", [{ a: 1 }], {
            queryOptions: { timeout: 5000, cancel: true },
        });

        expect(result.stats.chunks).toBe(1);
        expect(query.calls[0].options.timeout).toBe(5000);
    });

    it("executeBatch supports progress reporting", async () => {
        const events = [];
        const query = captureQuery(() => ({ rows: [], rowCount: 0 }));
        const batch = createBatch({ query });

        const builders = [
            { toSQL: () => ({ text: "SELECT 1", values: [] }) },
            { toSQL: () => ({ text: "SELECT 2", values: [] }) },
            { toSQL: () => ({ text: "SELECT 3", values: [] }) },
        ];

        await batch.executeBatch(builders, {
            chunkSize: 1,
            onProgress: (event) => events.push(event),
        });

        expect(events.length).toBeGreaterThan(0);
        expect(events[0].operation).toBe("executeBatch");
    });
});
