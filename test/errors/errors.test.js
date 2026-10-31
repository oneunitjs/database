import { describe, it, expect } from "vitest";

import {
    DatabaseError,
    TimeoutError,
    normalizeError,
    isDatabaseError,
    isTransientError,
    isUniqueViolation,
    isForeignKeyViolation,
    isConnectionError,
    isConstraintViolation,
    isSerializationFailure,
    isDeadlock,
} from "../../src/index.js";

describe("errors", () => {
    it("normalizeError wraps driver errors into a DatabaseError", () => {
        const cause = Object.assign(new Error("duplicate key"), {
            code: "23505",
            constraint: "uq_users_email",
            table: "users",
            detail: "Key (email) already exists.",
        });

        const error = normalizeError(cause);

        expect(error).toBeInstanceOf(DatabaseError);
        expect(error.name).toBe("DatabaseError");
        expect(error.code).toBe("23505");
        expect(error.constraint).toBe("uq_users_email");
        expect(error.table).toBe("users");
        expect(error.cause).toBe(cause);
        expect(isUniqueViolation(error)).toBe(true);
        expect(isConstraintViolation(error)).toBe(true);
    });

    it("normalizeError is idempotent", () => {
        const error = normalizeError(new Error("x"));
        expect(normalizeError(error)).toBe(error);
        expect(isDatabaseError(error)).toBe(true);
    });

    it("isTransientError detects retryable PostgreSQL classes", () => {
        expect(isTransientError({ code: "40001" })).toBe(true);
        expect(isTransientError({ code: "40P01" })).toBe(true);
        expect(isTransientError({ code: "08006" })).toBe(true);
        expect(isTransientError({ code: "57P03" })).toBe(true);
        expect(isTransientError({ code: "ECONNREFUSED" })).toBe(true);
        expect(isTransientError(new TimeoutError())).toBe(true);
        expect(isTransientError({ code: "23505" })).toBe(false);
        expect(isTransientError(null)).toBe(false);
    });

    it("specific classifiers work", () => {
        expect(isForeignKeyViolation({ code: "23503" })).toBe(true);
        expect(isSerializationFailure({ code: "40001" })).toBe(true);
        expect(isDeadlock({ code: "40P01" })).toBe(true);
        expect(isConnectionError({ code: "08001" })).toBe(true);
        expect(isConnectionError({ code: "23505" })).toBe(false);
    });

    it("TimeoutError carries timeout metadata and is transient", () => {
        const error = new TimeoutError("too slow", { timeout: 50 });
        expect(error.timeout).toBe(50);
        expect(error.code).toBe("TIMEOUT");
        expect(error.transient).toBe(true);
    });

    it("DatabaseError serializes without leaking the cause", () => {
        const error = normalizeError(Object.assign(new Error("boom"), { code: "42601" }));
        const json = error.toJSON();
        expect(json.message).toBe("boom");
        expect(json.code).toBe("42601");
        expect(json.cause).toBeUndefined();
    });
});
