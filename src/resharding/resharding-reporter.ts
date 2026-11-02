import type { ReshardingProgress } from "./resharding-tracker.js";
import type { CopyMetrics } from "./resharding-metrics.js";
import type { CutoverReadinessReport } from "./cutover-validator.js";
import type { SchemaCompatibilityReport } from "./schema-validator.js";
import type { ReshardingStatusInfo, ValidationReport } from "./resharding-types.js";

export type ReshardingReportFormat = "json" | "markdown";

export interface ReshardingReport {
    metadata: { migrationId: string; generatedAt: string; format: ReshardingReportFormat; version: number };
    summary: {
        status: string;
        overallPercentage: number;
        estimatedRemainingMs: number | null;
        createdAt: string;
        updatedAt: string;
        startedAt?: string;
        completedAt?: string;
    };
    progress: ReshardingProgress | null;
    performance: CopyMetrics | null;
    issues: { errors: string[]; warnings: string[] };
    recommendations: string[];
    validation?: ValidationReport[];
    schema?: SchemaCompatibilityReport;
    cutover?: CutoverReadinessReport;
}

export class ReshardingReporter {
    generate(
        status: ReshardingStatusInfo,
        extras: {
            progress?: ReshardingProgress;
            metrics?: CopyMetrics;
            validation?: ValidationReport[];
            schema?: SchemaCompatibilityReport;
            cutover?: CutoverReadinessReport;
        } = {},
    ): ReshardingReport {
        const errors: string[] = [];
        const warnings: string[] = [];
        if (status.errorMessage) {
            errors.push(status.errorMessage);
        }
        if (status.unitsFailed > 0) {
            errors.push(`${status.unitsFailed} unit(s) failed`);
        }
        if (status.changeLogLag > 0) {
            warnings.push(`Change log lag is ${status.changeLogLag}`);
        }
        for (const report of extras.validation ?? []) {
            for (const failure of report.failures) {
                if (failure.severity === "error") {
                    errors.push(`${failure.table}: ${failure.message}`);
                } else {
                    warnings.push(`${failure.table}: ${failure.message}`);
                }
            }
        }
        for (const issue of extras.schema?.issues ?? []) {
            if (issue.severity === "error") {
                errors.push(`${issue.table}: ${issue.message}`);
            } else if (issue.severity === "warning") {
                warnings.push(`${issue.table}: ${issue.message}`);
            }
        }
        for (const blocker of extras.cutover?.blockers ?? []) {
            errors.push(`Cutover blocked: ${blocker.message}`);
        }
        for (const warning of extras.cutover?.warnings ?? []) {
            warnings.push(warning.message);
        }

        const recommendations: string[] = [];
        if (status.status === "paused") {
            recommendations.push("Resume the migration when ready.");
        }
        if (status.status === "failed") {
            recommendations.push("Inspect error_code/error_message and retry from planned after fixing the cause.");
        }
        if (extras.cutover && !extras.cutover.ready) {
            recommendations.push("Resolve cutover blockers before attempting cutover.");
        }
        if (status.changeLogLag > 0) {
            recommendations.push("Drain remaining change-log events before cutover.");
        }
        if (recommendations.length === 0 && status.status === "ready_for_cutover") {
            recommendations.push("Migration is ready for cutover.");
        }

        return {
            metadata: {
                migrationId: status.migrationId,
                generatedAt: new Date().toISOString(),
                format: "json",
                version: 1,
            },
            summary: {
                status: status.status,
                overallPercentage: extras.progress?.overallPercentage ?? 0,
                estimatedRemainingMs: extras.progress?.estimatedRemainingMs ?? null,
                createdAt: status.createdAt,
                updatedAt: status.updatedAt,
                startedAt: status.startedAt,
                completedAt: status.completedAt,
            },
            progress: extras.progress ?? null,
            performance: extras.metrics ?? null,
            issues: { errors, warnings },
            recommendations,
            validation: extras.validation,
            schema: extras.schema,
            cutover: extras.cutover,
        };
    }

    format(report: ReshardingReport, format: ReshardingReportFormat = "markdown"): string {
        if (format === "json") {
            return JSON.stringify(report, null, 2);
        }
        const lines = [
            `# Resharding report: ${report.metadata.migrationId}`,
            "",
            `- Status: ${report.summary.status}`,
            `- Progress: ${report.summary.overallPercentage}%`,
            `- Generated: ${report.metadata.generatedAt}`,
            "",
            "## Issues",
        ];
        if (report.issues.errors.length === 0 && report.issues.warnings.length === 0) {
            lines.push("- None");
        }
        for (const error of report.issues.errors) {
            lines.push(`- Error: ${error}`);
        }
        for (const warning of report.issues.warnings) {
            lines.push(`- Warning: ${warning}`);
        }
        lines.push("", "## Recommendations");
        if (report.recommendations.length === 0) {
            lines.push("- None");
        }
        for (const rec of report.recommendations) {
            lines.push(`- ${rec}`);
        }
        return lines.join("\n");
    }
}
