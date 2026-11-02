import { describe, it, expect } from "vitest";
import { SchemaValidator } from "../../src/resharding/schema-validator.ts";
import { ChecksumVerifier, ChecksumCalculator } from "../../src/resharding/checksum-verifier.ts";
import { CutoverReadinessValidator } from "../../src/resharding/cutover-validator.ts";

function col(name, dataType, { nullable = true, defaultValue = null, ordinalPosition = 1 } = {}) {
    return { name, dataType, nullable, defaultValue, ordinalPosition };
}

function table(name, columns, extras = {}) {
    return {
        name,
        columns,
        constraints: extras.constraints ?? [],
        indexes: extras.indexes ?? [],
    };
}

function status(overrides = {}) {
    return {
        migrationId: "m1",
        status: "ready_for_cutover",
        version: 1,
        sourceRoutingVersion: 1,
        targetRoutingVersion: 2,
        createdAt: "t",
        updatedAt: "t",
        unitsTotal: 2,
        unitsCompleted: 2,
        unitsFailed: 0,
        rowsCopied: 20,
        rowsValidated: 20,
        changeLogLag: 0,
        cutoverIntent: false,
        cutoverCommitted: false,
        ...overrides,
    };
}

describe("destination schema compatibility", () => {
    const validator = new SchemaValidator();

    it("accepts identical schemas and safe type widening", () => {
        const source = table("orders", [
            col("id", "int4", { nullable: false, ordinalPosition: 1 }),
            col("note", "varchar", { ordinalPosition: 2 }),
        ]);
        const dest = table("orders", [
            col("id", "int8", { nullable: false, ordinalPosition: 1 }),
            col("note", "text", { ordinalPosition: 2 }),
        ]);
        const report = validator.validate([source], [dest]);
        expect(report.compatible).toBe(true);
        expect(report.tablesCompared).toBe(1);
    });

    it("rejects missing tables, missing columns, and unsafe narrowing", () => {
        const source = table("orders", [
            col("id", "int8", { nullable: false }),
            col("amount", "numeric"),
        ]);
        const dest = table("orders", [col("id", "int4", { nullable: false })]);
        const report = validator.validate([source, table("items", [col("id", "int4")])], [dest]);
        const codes = report.issues.map((i) => i.code);
        expect(report.compatible).toBe(false);
        expect(codes).toContain("MISSING_TABLE");
        expect(codes).toContain("MISSING_COLUMN");
        expect(codes).toContain("INCOMPATIBLE_TYPE");
    });

    it("rejects nullable source columns that are NOT NULL on destination", () => {
        const source = table("t", [col("name", "text", { nullable: true })]);
        const dest = table("t", [col("name", "text", { nullable: false })]);
        expect(validator.validate([source], [dest]).compatible).toBe(false);
    });

    it("allows extra destination columns that are nullable or have a default", () => {
        const source = table("t", [col("id", "int4", { nullable: false })]);
        const dest = table("t", [
            col("id", "int4", { nullable: false, ordinalPosition: 1 }),
            col("note", "text", { nullable: true, ordinalPosition: 2 }),
            col("flag", "bool", { nullable: false, defaultValue: "false", ordinalPosition: 3 }),
        ]);
        expect(validator.validate([source], [dest]).compatible).toBe(true);
    });

    it("rejects extra destination NOT NULL columns without a default", () => {
        const source = table("t", [col("id", "int4", { nullable: false })]);
        const dest = table("t", [
            col("id", "int4", { nullable: false, ordinalPosition: 1 }),
            col("flag", "bool", { nullable: false, defaultValue: null, ordinalPosition: 2 }),
        ]);
        expect(validator.validate([source], [dest]).issues.some((i) => i.code === "EXTRA_NOT_NULL_COLUMN")).toBe(
            true,
        );
    });

    it("treats missing unique indexes as warnings, not blockers", () => {
        const source = table(
            "t",
            [col("email", "text")],
            { indexes: [{ name: "t_email_uq", columns: ["email"], unique: true }] },
        );
        const dest = table("t", [col("email", "text")]);
        const report = validator.validate([source], [dest]);
        expect(report.compatible).toBe(true);
        expect(report.issues.some((i) => i.code === "MISSING_UNIQUE_INDEX")).toBe(true);
    });

    it("rejects primary key column mismatch", () => {
        const source = table(
            "t",
            [col("id", "int4"), col("tenant", "text")],
            { constraints: [{ name: "t_pk", type: "primary_key", columns: ["id"] }] },
        );
        const dest = table(
            "t",
            [col("id", "int4"), col("tenant", "text")],
            { constraints: [{ name: "t_pk", type: "primary_key", columns: ["id", "tenant"] }] },
        );
        expect(validator.validate([source], [dest]).compatible).toBe(false);
    });
});

describe("checksum verification", () => {
    it("matches rows regardless of object key order", () => {
        const verifier = new ChecksumVerifier();
        const comparison = verifier.compare([{ b: 2, a: 1 }], [{ a: 1, b: 2 }]);
        expect(comparison.match).toBe(true);
        expect(comparison.source.rowCount).toBe(1);
    });

    it("treats null and undefined as the same serialized value", () => {
        const calc = new ChecksumCalculator("sha256");
        expect(calc.hashRows([{ x: null }]).hash).toBe(calc.hashRows([{ x: undefined }]).hash);
    });

    it("does not match when row order or count differs", () => {
        const verifier = new ChecksumVerifier();
        expect(verifier.compare([{ id: 1 }, { id: 2 }], [{ id: 2 }, { id: 1 }]).match).toBe(false);
        expect(verifier.compare([{ id: 1 }], [{ id: 1 }, { id: 1 }]).match).toBe(false);
    });

    it("hashes bigint values stably across batches", () => {
        const calc = new ChecksumCalculator("md5");
        const direct = calc.hashRows([{ n: 10n }, { n: 11n }]);
        const batched = calc.hashBatches([[{ n: 10n }], [{ n: 11n }]]);
        expect(direct.hash).toBe(batched.hash);
        expect(direct.rowCount).toBe(2);
        expect(calc.getBatchSize()).toBe(1000);
    });
});

describe("cutover readiness", () => {
    const validator = new CutoverReadinessValidator();

    it("is ready when copy, validation, and change log are drained", () => {
        const report = validator.validate({
            status: status(),
            writeFenceActive: true,
            sourceShardsHealthy: true,
            targetShardsHealthy: true,
        });
        expect(report.ready).toBe(true);
        expect(report.blockers).toEqual([]);
        expect(report.confidence).toBe(1);
    });

    it("blocks cutover while still copying or while units have failed", () => {
        const copying = validator.validate({ status: status({ status: "copying", unitsCompleted: 1 }) });
        expect(copying.ready).toBe(false);
        expect(copying.blockers.some((b) => b.name === "status")).toBe(true);
        expect(copying.blockers.some((b) => b.name === "data_copy")).toBe(true);

        const failed = validator.validate({ status: status({ unitsFailed: 1 }) });
        expect(failed.blockers.some((b) => b.name === "data_copy")).toBe(true);
    });

    it("treats a missing write fence as a warning unless strict", () => {
        const lenient = validator.validate({ status: status(), writeFenceActive: false });
        expect(lenient.ready).toBe(true);
        expect(lenient.warnings.some((w) => w.name === "write_fence")).toBe(true);

        const strict = validator.validate({ status: status(), writeFenceActive: false }, { strict: true });
        expect(strict.ready).toBe(false);
        expect(strict.blockers.some((b) => b.name === "write_fence")).toBe(true);
    });

    it("blocks when checksums or schema are incompatible", () => {
        const report = validator.validate({
            status: status(),
            checksumMatch: false,
            schemaReport: { compatible: false, issues: [], tablesCompared: 1 },
        });
        expect(report.ready).toBe(false);
        expect(report.blockers.some((b) => b.message.includes("Checksum"))).toBe(true);
        expect(report.blockers.some((b) => b.message.includes("schema"))).toBe(true);
    });

    it("requires validation reports in strict mode", () => {
        const report = validator.validate({ status: status(), validationReports: [] }, { strict: true });
        expect(report.blockers.some((b) => b.name === "validation")).toBe(true);
    });
});
