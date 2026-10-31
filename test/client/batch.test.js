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

        const rows = await batch.insertMany("users", [{ a: 1, b: 2 }, { a: 3, b: 4 }], {
            returning: ["id"],
        });

        expect(query.calls.length).toBe(1);
        expect(query.calls[0].text).toBe('INSERT INTO "users" ("a", "b") VALUES ($1, $2), ($3, $4) RETURNING "id"');
        expect(query.calls[0].values).toEqual([1, 2, 3, 4]);
        expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
    });

    it("insertMany chunks large batches", async () => {
        const query = captureQuery((text, values) => ({ rows: [{ a: values[0] }], rowCount: 1 }));
        const batch = createBatch({ query });

        await batch.insertMany("t", [{ a: 1 }, { a: 2 }, { a: 3 }], {
            chunkSize: 2,
            returning: ["a"],
        });

        expect(query.calls.length).toBe(2);
        expect(query.calls[0].values).toEqual([1, 2]);
        expect(query.calls[1].values).toEqual([3]);
    });

    it("insertMany returns the row count when no returning clause is used", async () => {
        const query = captureQuery(() => ({ rows: [], rowCount: 1 }));
        const batch = createBatch({ query });

        const count = await batch.insertMany("t", [{ a: 1 }, { a: 2 }]);
        expect(count).toBe(2);
    });

    it("insertMany handles empty input and conflicts", async () => {
        const query = captureQuery();
        const batch = createBatch({ query });

        expect(await batch.insertMany("t", [])).toEqual([]);

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
});
