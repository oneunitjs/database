import { describe, it, expect } from "vitest";
import { enumColumn } from "../../src/index.js";

describe("column helpers", () => {
    it("enumColumn escapes single quotes in enum values", () => {
        const column = enumColumn({
            columnName: "status",
            values: ["active", "O'Reilly"],
        });
        expect(column.status).toBe("ENUM ('active', 'O''Reilly') NOT NULL");
    });
});
