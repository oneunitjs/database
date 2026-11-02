import type { ReshardingStatus, ReshardingStatusInfo, ValidationReport } from "./resharding-types.js";
import type { SchemaCompatibilityReport } from "./schema-validator.js";

export type CutoverCheckName =
    | "status"
    | "data_copy"
    | "validation"
    | "change_log"
    | "shard_health"
    | "write_fence"
    | "active_transactions";

export interface CutoverCheckResult {
    name: CutoverCheckName;
    passed: boolean;
    blocking: boolean;
    message: string;
    metrics?: Record<string, number | string | boolean>;
}

export interface CutoverReadinessReport {
    ready: boolean;
    blockers: CutoverCheckResult[];
    warnings: CutoverCheckResult[];
    checks: CutoverCheckResult[];
    confidence: number;
}

export interface CutoverReadinessInput {
    status: ReshardingStatusInfo;
    validationReports?: ValidationReport[];
    schemaReport?: SchemaCompatibilityReport;
    sourceShardsHealthy?: boolean;
    targetShardsHealthy?: boolean;
    writeFenceActive?: boolean;
    activeTransactions?: number;
    checksumMatch?: boolean;
}

const READY_STATUSES: ReshardingStatus[] = ["ready_for_cutover", "validating"];

export class CutoverReadinessValidator {
    validate(input: CutoverReadinessInput, options: { strict?: boolean } = {}): CutoverReadinessReport {
        const checks: CutoverCheckResult[] = [];
        const statusOk = READY_STATUSES.includes(input.status.status);
        checks.push({
            name: "status",
            passed: statusOk,
            blocking: true,
            message: statusOk
                ? `Migration is in ${input.status.status}`
                : `Migration status "${input.status.status}" is not ready for cutover`,
            metrics: { status: input.status.status },
        });

        const unitsDone = input.status.unitsTotal === 0 || input.status.unitsCompleted >= input.status.unitsTotal;
        const failedUnits = input.status.unitsFailed === 0;
        checks.push({
            name: "data_copy",
            passed: unitsDone && failedUnits,
            blocking: true,
            message:
                unitsDone && failedUnits
                    ? "All units copied"
                    : `Copy incomplete (completed=${input.status.unitsCompleted}/${input.status.unitsTotal}, failed=${input.status.unitsFailed})`,
            metrics: {
                unitsCompleted: input.status.unitsCompleted,
                unitsTotal: input.status.unitsTotal,
                unitsFailed: input.status.unitsFailed,
                rowsCopied: input.status.rowsCopied,
            },
        });

        const reports = input.validationReports ?? [];
        const validationPassed = reports.length === 0 ? !options.strict : reports.every((r) => r.passed);
        checks.push({
            name: "validation",
            passed: validationPassed,
            blocking: true,
            message: validationPassed ? "Validation passed" : "Validation reported failures",
            metrics: { reports: reports.length },
        });

        const lagOk = input.status.changeLogLag <= 0;
        checks.push({
            name: "change_log",
            passed: lagOk,
            blocking: true,
            message: lagOk ? "Change log drained" : `Change log lag is ${input.status.changeLogLag}`,
            metrics: { changeLogLag: input.status.changeLogLag },
        });

        const shardsHealthy = input.sourceShardsHealthy !== false && input.targetShardsHealthy !== false;
        checks.push({
            name: "shard_health",
            passed: shardsHealthy,
            blocking: true,
            message: shardsHealthy ? "Source and target shards healthy" : "One or more shards are unhealthy",
        });

        const fenceOk = input.writeFenceActive === true;
        checks.push({
            name: "write_fence",
            passed: fenceOk,
            blocking: options.strict === true,
            message: fenceOk ? "Write fence is active" : "Write fence is not active",
        });

        const activeTx = input.activeTransactions ?? 0;
        checks.push({
            name: "active_transactions",
            passed: activeTx === 0,
            blocking: options.strict === true,
            message: activeTx === 0 ? "No active source transactions" : `${activeTx} active transaction(s) on source`,
            metrics: { activeTransactions: activeTx },
        });

        if (input.schemaReport && !input.schemaReport.compatible) {
            checks.push({
                name: "validation",
                passed: false,
                blocking: true,
                message: "Destination schema is incompatible",
            });
        }

        if (input.checksumMatch === false) {
            checks.push({
                name: "validation",
                passed: false,
                blocking: true,
                message: "Checksum verification failed",
            });
        }

        const blockers = checks.filter((c) => !c.passed && c.blocking);
        const warnings = checks.filter((c) => !c.passed && !c.blocking);
        const passedCount = checks.filter((c) => c.passed).length;
        return {
            ready: blockers.length === 0,
            blockers,
            warnings,
            checks,
            confidence: checks.length === 0 ? 0 : passedCount / checks.length,
        };
    }
}
