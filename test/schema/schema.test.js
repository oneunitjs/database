import { describe, it, expect } from "vitest";

import { createSchemaManager } from "../../src/index.js";

describe("schema manager", () => {
    it("create.table builds and runs CREATE TABLE", async () => {
        const queries = [];
        const pool = {
            async query(sql) {
                queries.push(sql);
            },
        };

        const manager = createSchemaManager(pool);
        await manager.create.table("users", {
            id: "UUID PRIMARY KEY",
            name: "TEXT NOT NULL",
        });

        expect(queries[0]).toMatch(/CREATE TABLE IF NOT EXISTS users/);
        expect(queries[0]).toMatch(/id UUID PRIMARY KEY/);
        expect(queries[0]).toMatch(/name TEXT NOT NULL/);
    });
});
