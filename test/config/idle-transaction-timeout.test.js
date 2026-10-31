import { describe, it, expect } from "vitest";

import { loadDatabaseConfig } from "../../src/index.js";

describe("idleTransactionTimeout", () => {
    it("enabled with valid timeout sets pg options", () => {
        const config = loadDatabaseConfig({
            idleTransactionTimeout: { enabled: true, timeout: 60_000 },
        });

        expect(config.options).toContain(
            "-c idle_in_transaction_session_timeout=60000",
        );
    });

    it("disabled does not set idle_in_transaction_session_timeout", () => {
        const config = loadDatabaseConfig({
            idleTransactionTimeout: { enabled: false },
        });

        expect(config.options).toBeUndefined();
    });

    it("enabled without timeout throws TypeError", () => {
        expect(() =>
            loadDatabaseConfig({ idleTransactionTimeout: { enabled: true } }),
        ).toThrow(TypeError);
    });

    it("negative timeout throws RangeError", () => {
        expect(() =>
            loadDatabaseConfig({
                idleTransactionTimeout: { enabled: true, timeout: -1000 },
            }),
        ).toThrow(RangeError);
    });

    it("zero timeout throws RangeError when enabled", () => {
        expect(() =>
            loadDatabaseConfig({
                idleTransactionTimeout: { enabled: true, timeout: 0 },
            }),
        ).toThrow(RangeError);
    });

    it("NaN timeout throws RangeError", () => {
        expect(() =>
            loadDatabaseConfig({
                // @ts-expect-error - intentionally testing invalid runtime value
                idleTransactionTimeout: { enabled: true, timeout: NaN },
            }),
        ).toThrow(RangeError);
    });

    it("Infinity timeout throws RangeError", () => {
        expect(() =>
            loadDatabaseConfig({
                idleTransactionTimeout: { enabled: true, timeout: Infinity },
            }),
        ).toThrow(RangeError);
    });

    it("timeout exceeding PostgreSQL max throws RangeError", () => {
        expect(() =>
            loadDatabaseConfig({
                idleTransactionTimeout: {
                    enabled: true,
                    timeout: 2147483648,
                },
            }),
        ).toThrow(RangeError);
    });

    it("timeout at PostgreSQL max is accepted", () => {
        const config = loadDatabaseConfig({
            idleTransactionTimeout: {
                enabled: true,
                timeout: 2147483647,
            },
        });

        expect(config.options).toContain(
            "-c idle_in_transaction_session_timeout=2147483647",
        );
    });

    it("preserves existing options alongside idle transaction timeout", () => {
        const config = loadDatabaseConfig({
            idleTransactionTimeout: { enabled: true, timeout: 30_000 },
            options: "-c statement_timeout=10000",
        });

        expect(config.options).toContain(
            "-c idle_in_transaction_session_timeout=30000",
        );
        expect(config.options).toContain("-c statement_timeout=10000");
    });

    it("PGOPTIONS environment variable is preserved alongside idle transaction timeout", () => {
        const config = loadDatabaseConfig(
            { idleTransactionTimeout: { enabled: true, timeout: 30_000 } },
            { PGOPTIONS: "-c application_name=myapp" },
        );

        expect(config.options).toContain(
            "-c idle_in_transaction_session_timeout=30000",
        );
        expect(config.options).toContain("-c application_name=myapp");
    });

    it("replaces existing idle_in_transaction_session_timeout in options", () => {
        const config = loadDatabaseConfig({
            idleTransactionTimeout: { enabled: true, timeout: 45_000 },
            options: "-c idle_in_transaction_session_timeout=1000",
        });

        expect(config.options).toContain(
            "-c idle_in_transaction_session_timeout=45000",
        );
        expect(config.options).not.toContain("=1000");
    });

    it("omitting idleTransactionTimeout does not set the option", () => {
        const config = loadDatabaseConfig({});

        expect(config.options).toBeUndefined();
    });

    it("enabled with fractional timeout floors to integer", () => {
        const config = loadDatabaseConfig({
            idleTransactionTimeout: { enabled: true, timeout: 60_000.7 },
        });

        expect(config.options).toContain(
            "-c idle_in_transaction_session_timeout=60000",
        );
    });

    it("non-number timeout string throws TypeError", () => {
        expect(() =>
            // @ts-expect-error - intentionally testing invalid runtime value
            loadDatabaseConfig({
                idleTransactionTimeout: { enabled: true, timeout: "60000" },
            }),
        ).toThrow(TypeError);
    });
});
