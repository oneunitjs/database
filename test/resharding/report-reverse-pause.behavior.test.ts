import { describe, it, expect } from "vitest";
import { ReshardingReporter } from "../../src/resharding/resharding-reporter.ts";
import { ReverseMigrationPlanner } from "../../src/resharding/reverse-migration.ts";
import { ReshardingCopier } from "../../src/resharding/resharding-copier.ts";
import { ReshardingChangeCapture } from "../../src/resharding/resharding-change-capture.ts";

function status(overrides = {}) {
    return {
        migrationId: "m1",
        status: "paused",
        version: 1,
        sourceRoutingVersion: 1,
        targetRoutingVersion: 2,
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:01:00.000Z",
        unitsTotal: 2,
        unitsCompleted: 1,
        unitsFailed: 1,
        rowsCopied: 10,
        rowsValidated: 0,
        changeLogLag: 3,
        errorMessage: "copy timed out",
        cutoverIntent: false,
        cutoverCommitted: false,
        ...overrides,
    };
}

function topology(routingVersion, shardId) {
    return {
        shards: [
            {
                id: shardId,
                version: routingVersion,
                connection: { host: "localhost", port: 5432, database: "db", user: "u" },
            },
        ],
        strategy: "hash",
        strategyConfig: {},
        routingVersion,
        capturedAt: "t",
    };
}

describe("resharding reporter", () => {
    const reporter = new ReshardingReporter();

    it("surfaces errors, lag warnings, and pause recommendations", () => {
        const report = reporter.generate(status(), {
            cutover: {
                ready: false,
                blockers: [
                    {
                        name: "data_copy",
                        passed: false,
                        blocking: true,
                        message: "Copy incomplete",
                    },
                ],
                warnings: [
                    {
                        name: "write_fence",
                        passed: false,
                        blocking: false,
                        message: "Write fence is not active",
                    },
                ],
                checks: [],
                confidence: 0,
            },
        });
        expect(report.issues.errors).toEqual(
            expect.arrayContaining(["copy timed out", "1 unit(s) failed", "Cutover blocked: Copy incomplete"]),
        );
        expect(report.issues.warnings).toEqual(
            expect.arrayContaining(["Change log lag is 3", "Write fence is not active"]),
        );
        expect(report.recommendations).toEqual(
            expect.arrayContaining([
                "Resume the migration when ready.",
                "Resolve cutover blockers before attempting cutover.",
                "Drain remaining change-log events before cutover.",
            ]),
        );
    });

    it("renders markdown and json without dropping issue text", () => {
        const report = reporter.generate(status({ status: "ready_for_cutover", unitsFailed: 0, errorMessage: undefined, changeLogLag: 0 }));
        const md = reporter.format(report, "markdown");
        expect(md).toContain("# Resharding report: m1");
        expect(md).toContain("ready_for_cutover");
        expect(md).toContain("Migration is ready for cutover.");
        const json = JSON.parse(reporter.format(report, "json"));
        expect(json.metadata.migrationId).toBe("m1");
        expect(json.summary.status).toBe("ready_for_cutover");
    });
});

describe("reverse migration plan", () => {
    it("swaps source and destination without copying progress", () => {
        const planner = new ReverseMigrationPlanner();
        const original = {
            migrationId: "mig-1",
            createdAt: "t",
            sourceRoutingVersion: 1,
            targetRoutingVersion: 2,
            sourceTopology: topology(1, "s0"),
            targetTopology: topology(2, "s1"),
            strategy: "hash",
            strategyConfig: {},
            units: [
                {
                    unitId: "u1",
                    unitType: "bucket",
                    sourceShardId: "s0",
                    destinationShardId: "s1",
                    estimatedRows: 10,
                    status: "completed",
                    rowsCopied: 10,
                    lastCheckpoint: { kind: "keyset", columns: ["id"], lastValues: [9], batchNo: 1, updatedAt: "t" },
                    updatedAt: "t",
                },
            ],
            estimatedTotalRows: 10,
            affectedShardIds: ["s0", "s1"],
            dryRun: false,
            version: 1,
        };
        const reverse = planner.plan(original, { reverseMigrationId: "rev-custom" });
        expect(reverse.reverseMigrationId).toBe("rev-custom");
        expect(reverse.sourceTopology.shards[0].id).toBe("s1");
        expect(reverse.targetTopology.shards[0].id).toBe("s0");
        expect(reverse.units[0]).toMatchObject({
            unitId: "rev-u1",
            sourceShardId: "s1",
            destinationShardId: "s0",
            status: "planned",
            rowsCopied: 0,
        });
        expect(reverse.units[0].lastCheckpoint).toBeUndefined();
        expect(reverse.risks.some((r) => r.code === "DATA_LOSS" && r.severity === "high")).toBe(true);
        expect(reverse.steps.map((s) => s.name)).toEqual([
            "validate",
            "copy",
            "catch_up",
            "verify",
            "cutover",
            "cleanup",
        ]);
        expect(reverse.estimatedDurationMs).toBe(475_000);
    });
});

describe("change capture pause", () => {
    it("skips draining while paused and resumes afterward", async () => {
        const capture = new ReshardingChangeCapture({
            coordination: {},
            getShardPool: () => undefined,
        });
        capture.pause();
        expect(capture.isPaused()).toBe(true);
        await expect(capture.drainChanges("m1", "u1", 10, async () => {})).resolves.toEqual({ applied: 0 });
        capture.resume();
        expect(capture.isPaused()).toBe(false);
    });
});

describe("copier pause/resume", () => {
    it("remembers pause reason and clears it on resume", () => {
        const copier = new ReshardingCopier(
            {},
            undefined,
            new ReshardingChangeCapture({
                coordination: {},
                getShardPool: () => undefined,
            }),
            {
                batchSize: 10,
                maxConcurrency: 1,
                maxRetries: 0,
                baseRetryDelayMs: 1,
                maxRetryDelayMs: 1,
                throttleMs: 0,
                batchTimeoutMs: 1_000,
                idleTimeoutMs: 1_000,
            },
        );
        expect(copier.isPaused()).toBe(false);
        copier.pause("operator-drain");
        expect(copier.isPaused()).toBe(true);
        expect(copier.getPauseReason()).toBe("operator-drain");
        copier.resume();
        expect(copier.isPaused()).toBe(false);
        expect(copier.getPauseReason()).toBeNull();
    });
});
