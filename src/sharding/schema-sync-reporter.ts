import type { SchemaSyncService } from "./schema-sync-service.js";

interface SummaryObject {
    status: string;
    successfulShards: number;
    failedShards: number;
    totalShards: number;
    shardResults: any[];
    progressPercent: number;
    progressPercentage: number;
    durationMs: number;
    duration: number;
    errors: string[];
    message: string;
    summary: string;
}

export class SchemaSyncReporter {
    constructor(private readonly syncService?: any, private readonly logger?: any) {}

    private normalize(input: any): any {
        if (!input) return {};
        if (input.status && (input.operationId !== undefined || input.shardResults !== undefined || input.successfulShards !== undefined)) {
            return input;
        }
        if (typeof input === "string") {
            try { return JSON.parse(input); } catch { return { message: input }; }
        }
        return input;
    }

    private build(input: any): SummaryObject & { toString(): string } {
        const normalized = this.normalize(input);
        const total = Number(
            normalized.totalShards ?? normalized.shardResults?.length ?? 0,
        );
        const successful = Number(
            normalized.successfulShards ?? normalized.shardResults?.filter((r: any) => r.success).length ?? 0,
        );
        const failed = Number(
            normalized.failedShards ?? Math.max(0, total - successful),
        );
        const progress = total > 0 ? (successful / total) * 100 : 0;
        const duration = Number(normalized.durationMs ?? normalized.duration ?? 0);
        const status = String(normalized.status ?? (failed > 0 ? "failed" : "completed"));
        const errors = Array.isArray(normalized.errors)
            ? normalized.errors
            : normalized.error
              ? [String(normalized.error?.message ?? normalized.error)]
              : [];
        const message = `${status}: ${successful}/${total} shards successful (${progress.toFixed(1)}%) in ${duration}ms`;
        const obj: any = {
            status,
            successfulShards: successful,
            failedShards: failed,
            totalShards: total,
            shardResults: normalized.shardResults ?? [],
            progressPercent: progress,
            progressPercentage: progress,
            durationMs: duration,
            duration,
            errors,
            message,
            summary: message,
        };
        const str = new String(message) as any;
        Object.assign(str, obj);
        str.toString = () => message;
        return str;
    }

    report(input?: any): any { return this.build(input); }
    reportResult(input?: any): any { return this.build(input); }
    reportOperation(...args: any[]): any {
        const last = args[args.length - 1];
        return this.build(last && (last.status || last.shardResults || last.operationId) ? last : args[0]);
    }
    summarize(input?: any): any { return this.build(input); }
    summary(input?: any): any { return this.build(input); }
    format(input?: any, format?: string): any {
        const s = this.build(input);
        if (format === "json") return JSON.stringify(s);
        return s.toString();
    }
    formatResult(input?: any): any { return this.format(input); }
    generateReport(input?: any): any { return this.build(input); }

    async status(operationId?: string): Promise<any> {
        return this.syncService?.getStatus?.(operationId);
    }
    async history(limit?: number): Promise<any> {
        return this.syncService?.getHistory?.(limit);
    }
}
