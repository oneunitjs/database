import type { MigrationUnit, ReshardingPlan, TopologySnapshot } from "./resharding-types.js";

export interface ReverseMigrationRisk {
    code: string;
    severity: "low" | "medium" | "high";
    description: string;
    mitigation: string;
}

export interface ReverseMigrationStep {
    order: number;
    name: string;
    description: string;
    estimatedDurationMs: number;
}

export interface ReverseMigrationPlan {
    originalMigrationId: string;
    reverseMigrationId: string;
    sourceTopology: TopologySnapshot;
    targetTopology: TopologySnapshot;
    units: MigrationUnit[];
    risks: ReverseMigrationRisk[];
    steps: ReverseMigrationStep[];
    dependencies: string[];
    estimatedDurationMs: number;
}

function reverseUnit(unit: MigrationUnit): MigrationUnit {
    return {
        ...unit,
        unitId: `rev-${unit.unitId}`,
        sourceShardId: unit.destinationShardId,
        destinationShardId: unit.sourceShardId,
        status: "planned",
        rowsCopied: 0,
        lastCheckpoint: undefined,
        updatedAt: new Date().toISOString(),
    };
}

export class ReverseMigrationPlanner {
    plan(original: ReshardingPlan, options: { reverseMigrationId?: string } = {}): ReverseMigrationPlan {
        const reverseMigrationId = options.reverseMigrationId ?? `rev-${original.migrationId}`;
        const units = original.units.map(reverseUnit);
        const risks: ReverseMigrationRisk[] = [
            {
                code: "DATA_LOSS",
                severity: "high",
                description: "Writes committed on the new topology may not exist on the original shards.",
                mitigation: "Replay change-log events and verify checksums before reversing.",
            },
            {
                code: "DOWNTIME",
                severity: "medium",
                description: "Cutover back to the original topology requires a write fence window.",
                mitigation: "Schedule a maintenance window and drain in-flight transactions.",
            },
            {
                code: "SCHEMA_DRIFT",
                severity: "medium",
                description: "Schema changes applied after the original migration may not exist on source shards.",
                mitigation: "Run schema compatibility validation before reverse copy.",
            },
            {
                code: "CONCURRENT_MIGRATION",
                severity: "high",
                description: "Another in-flight migration can conflict with reverse routing versions.",
                mitigation: "Ensure no other migration is active and lock the coordination row.",
            },
        ];
        const steps: ReverseMigrationStep[] = [
            { order: 1, name: "validate", description: "Validate reverse topology and schema compatibility", estimatedDurationMs: 30_000 },
            { order: 2, name: "copy", description: "Copy units from current shards back to original shards", estimatedDurationMs: 300_000 },
            { order: 3, name: "catch_up", description: "Drain change-log events generated during reverse copy", estimatedDurationMs: 60_000 },
            { order: 4, name: "verify", description: "Checksum and count validation", estimatedDurationMs: 60_000 },
            { order: 5, name: "cutover", description: "Publish original routing version", estimatedDurationMs: 15_000 },
            { order: 6, name: "cleanup", description: "Release fences and finalize", estimatedDurationMs: 10_000 },
        ];
        return {
            originalMigrationId: original.migrationId,
            reverseMigrationId,
            sourceTopology: original.targetTopology,
            targetTopology: original.sourceTopology,
            units,
            risks,
            steps,
            dependencies: original.affectedShardIds,
            estimatedDurationMs: steps.reduce((sum, s) => sum + s.estimatedDurationMs, 0),
        };
    }
}
