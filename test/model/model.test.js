import { describe, it, expect } from "vitest";

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
});
