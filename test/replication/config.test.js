import { describe, it, expect } from "vitest";
import { validateReplicationConfig } from "../../src/replication/replication-manager.js";
import { ReplicationConfigError } from "../../src/replication/errors.js";

function baseConfig(overrides = {}) {
    return {
        enabled: true,
        mode: "replica-preferred",
        primary: { id: "primary", connection: { connectionString: "postgresql://localhost:5432/db" } },
        replicas: [],
        ...overrides,
    };
}

describe("validateReplicationConfig", () => {
    it("accepts a valid primary-only config", () => {
        expect(() =>
            validateReplicationConfig(baseConfig({ mode: "primary", replicas: [] })),
        ).not.toThrow();
    });

    it("accepts replica-preferred with replicas", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    mode: "replica-preferred",
                    replicas: [{ id: "r0", connection: { connectionString: "postgresql://localhost:5432/r0" } }],
                }),
            ),
        ).not.toThrow();
    });

    it("rejects an invalid mode", () => {
        expect(() =>
            validateReplicationConfig(baseConfig({ mode: "invalid" })),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects duplicate endpoint ids", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    replicas: [
                        { id: "r0", connection: { connectionString: "postgresql://localhost:5432/r0" } },
                        { id: "r0", connection: { connectionString: "postgresql://localhost:5432/r0-dup" } },
                    ],
                }),
            ),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects a replica whose id collides with the primary id", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    primary: { id: "primary", connection: { connectionString: "postgresql://localhost:5432/db" } },
                    replicas: [
                        { id: "primary", connection: { connectionString: "postgresql://localhost:5432/r0" } },
                    ],
                }),
            ),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects a replica with an empty id", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    replicas: [{ id: "", connection: { connectionString: "postgresql://localhost:5432/r0" } }],
                }),
            ),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects a replica missing a connection", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    replicas: [{ id: "r0", connection: undefined }],
                }),
            ),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects a replica connection without connectionString or host", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    replicas: [{ id: "r0", connection: { database: "x" } }],
                }),
            ),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects negative pool max", () => {
        expect(() =>
            validateReplicationConfig(
                baseConfig({
                    replicas: [
                        { id: "r0", connection: { connectionString: "postgresql://localhost:5432/r0", max: -1 } },
                    ],
                }),
            ),
        ).toThrow(ReplicationConfigError);
    });

    it("rejects a negative lagThresholdMs", () => {
        expect(() =>
            validateReplicationConfig(baseConfig({ lagThresholdMs: -1 })),
        ).toThrow(ReplicationConfigError);
    });

    it("returns normally when replication is disabled", () => {
        expect(() => validateReplicationConfig({ ...baseConfig(), enabled: false })).not.toThrow();
    });
});
