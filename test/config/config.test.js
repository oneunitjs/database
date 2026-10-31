import { describe, it, expect } from "vitest";

import { loadDatabaseConfig, parseSslConfig } from "../../src/index.js";

describe("config", () => {
    it("loadDatabaseConfig maps the documented DATABASE_* variables", () => {
        const config = loadDatabaseConfig({}, {
            DATABASE_URL: "postgresql://u:p@db:5432/app",
            DATABASE_HOST: "db",
            DATABASE_PORT: "5433",
            DATABASE_NAME: "app",
            DATABASE_USER: "u",
            DATABASE_PASSWORD: "p",
            DB_MAX_CONN: "7",
            DB_MIN_CONN: "2",
            DB_IDLE_TIMEOUT: "1000",
            DB_CONN_TIMEOUT: "2000",
            DB_STATEMENT_TIMEOUT: "3000",
            DB_QUERY_TIMEOUT: "4000",
            DB_APP_NAME: "svc",
            DB_SSL: "false",
        });

        expect(config.connectionString).toBe("postgresql://u:p@db:5432/app");
        expect(config.host).toBe("db");
        expect(config.port).toBe(5433);
        expect(config.database).toBe("app");
        expect(config.user).toBe("u");
        expect(config.password).toBe("p");
        expect(config.max).toBe(7);
        expect(config.min).toBe(2);
        expect(config.idleTimeoutMillis).toBe(1000);
        expect(config.connectionTimeoutMillis).toBe(2000);
        expect(config.statement_timeout).toBe(3000);
        expect(config.query_timeout).toBe(4000);
        expect(config.application_name).toBe("svc");
        expect(config.ssl).toBe(false);
    });

    it("dbConfig parses environment variables", () => {
        const config = loadDatabaseConfig({}, {
            DATABASE_URL: "postgresql://user:pass@localhost:5432/writer",
            DB_MAX_CONN: "5",
            DB_CONN_TIMEOUT: "1000",
            DB_IDLE_TIMEOUT: "2000",
            DB_SSL: "false",
            DB_APP_NAME: "writer-test",
        });

        expect(config.connectionString).toBe("postgresql://user:pass@localhost:5432/writer");
        expect(config.max).toBe(5);
        expect(config.connectionTimeoutMillis).toBe(1000);
        expect(config.idleTimeoutMillis).toBe(2000);
        expect(config.ssl).toBe(false);
        expect(config.application_name).toBe("writer-test");
    });

    it("dbConfig falls back to defaults when env is missing", () => {
        const config = loadDatabaseConfig({}, {});

        expect(config.connectionString).toBeUndefined();
        expect(config.max).toBe(20);
        expect(config.connectionTimeoutMillis).toBe(5000);
        expect(config.idleTimeoutMillis).toBe(30000);
        expect(config.ssl).toBe(false);
        expect(config.application_name).toBe("Unknown App");
    });

    it("explicit overrides win over environment values", () => {
        const config = loadDatabaseConfig(
            { host: "override", max: 99, ssl: { rejectUnauthorized: true } },
            { DATABASE_HOST: "env", DB_MAX_CONN: "1" },
        );

        expect(config.host).toBe("override");
        expect(config.max).toBe(99);
        expect(config.ssl).toEqual({ rejectUnauthorized: true });
    });

    it("undefined overrides are ignored rather than clearing env values", () => {
        const config = loadDatabaseConfig(
            { host: undefined, max: undefined },
            { DATABASE_HOST: "env", DB_MAX_CONN: "3" },
        );

        expect(config.host).toBe("env");
        expect(config.max).toBe(3);
    });

    it("DB_POOL_SIZE is accepted as an alias for pool size", () => {
        const config = loadDatabaseConfig({}, { DB_POOL_SIZE: "12" });
        expect(config.max).toBe(12);
    });

    it("DATABASE_POOL_* and timeout aliases are accepted", () => {
        const config = loadDatabaseConfig({}, {
            DATABASE_POOL_MAX: "9",
            DATABASE_POOL_MIN: "1",
            DATABASE_CONNECTION_TIMEOUT: "1500",
            DATABASE_QUERY_TIMEOUT: "2500",
            DATABASE_STATEMENT_TIMEOUT: "3500",
            DATABASE_SSL: "false",
        });

        expect(config.max).toBe(9);
        expect(config.min).toBe(1);
        expect(config.connectionTimeoutMillis).toBe(1500);
        expect(config.query_timeout).toBe(2500);
        expect(config.statement_timeout).toBe(3500);
    });

    it("dbConfig enables ssl without verification when CA is missing", () => {
        const ssl = parseSslConfig({ DB_SSL: "true", DB_SSL_CA: "/nonexistent-ca.pem" });
        expect(ssl).toEqual({ rejectUnauthorized: false });
    });

    it("parseSslConfig returns unverified SSL when no CA is specified", () => {
        const ssl = parseSslConfig({ DB_SSL: "true" });
        expect(ssl).toEqual({ rejectUnauthorized: false });
    });
});
