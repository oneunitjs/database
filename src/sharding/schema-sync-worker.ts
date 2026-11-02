import type { SchemaSyncService } from "./schema-sync-service.js";

export interface SchemaSyncWorkerOptions {
    intervalMs?: number;
    enabled?: boolean;
    autoRepair?: boolean;
    logger?: any;
}

export class SchemaSyncWorker {
    private timer?: ReturnType<typeof setInterval>;
    private running = false;

    constructor(
        private readonly syncService: SchemaSyncService,
        private readonly shardManager?: any,
        private readonly options: SchemaSyncWorkerOptions = {},
    ) {}

    start(): void {
        if (this.options.enabled === false) return;
        if (this.running) return;
        const intervalMs = this.options.intervalMs ?? 300_000;
        this.timer = setInterval(() => {
            void this.checkConsistency();
        }, intervalMs);
        this.running = true;
        this.options.logger?.info?.(`Schema sync worker started with interval ${intervalMs}ms`);
    }

    stop(): void {
        if (!this.running) return;
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = undefined;
        }
        this.running = false;
        this.options.logger?.info?.("Schema sync worker stopped");
    }

    isRunning(): boolean { return this.running; }

    private async checkConsistency(): Promise<void> {
        try {
            const service: any = this.syncService;
            const validate = service.validateConsistency ?? service.validate ?? service.validateSchema;
            const report = await validate?.call(service);
            if (report && !report.isConsistent) {
                this.options.logger?.warn?.(
                    `Schema inconsistency detected: ${(report.inconsistentShards?.length ?? 0)} inconsistent`,
                );
                if (this.options.autoRepair) {
                    try {
                        await service.repair?.();
                    } catch (error) {
                        this.options.logger?.error?.(`Schema auto-repair failed: ${error}`);
                    }
                }
            }
        } catch (error) {
            this.options.logger?.error?.(`Background consistency check failed: ${error}`);
        }
    }
}
