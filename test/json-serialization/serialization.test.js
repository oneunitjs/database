import { describe, it, expect } from "vitest";

import {
    encode,
    decode,
    encodeToString,
    decodeFromString,
    serializeRow,
    deserializeRow,
    serializeQueryResult,
    deserializeQueryResult,
} from "../../src/index.js";

describe("json serialization", () => {
    it("row serialization round-trips without an external msgpack package", () => {
        const row = { id: 1, email: "a@b.c", nested: { ok: true } };
        const restored = deserializeRow(serializeRow(row));
        expect(restored).toEqual(row);
    });

    it("query result serialization round-trips", () => {
        const result = {
            rows: [{ id: 2 }],
            rowCount: 1,
            fields: [{ name: "id" }],
        };

        expect(deserializeQueryResult(serializeQueryResult(result))).toEqual(result);
    });

    it("string helpers round-trip JSON payloads", () => {
        const value = { hello: "world" };
        expect(decodeFromString(encodeToString(value))).toEqual(value);
        expect(decode(encode(value))).toEqual(value);
    });
});
