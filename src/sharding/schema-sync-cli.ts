import type { SchemaSyncService } from "./schema-sync-service.js";

export class SchemaSyncCLI {
    constructor(private db: any, private logger: Console = console) {}

    async sync(options: any = {}): Promise<void> {
        await this.db?.schemaSync?.sync?.(undefined, options);
    }
    async validate(options: any = {}): Promise<void> {
        await this.db?.schemaSync?.validate?.(options);
    }
    async rollback(version: string, options: any = {}): Promise<void> {
        await this.db?.schemaSync?.rollback?.({ id: version }, options);
    }
    async status(operationId?: string): Promise<void> {
        const op = operationId ? await this.db?.schemaSync?.getStatus?.(operationId) : null;
        this.logger.log(op);
    }
    async history(limit?: number): Promise<void> {
        const list = await this.db?.schemaSync?.getHistory?.(limit);
        this.logger.log(list);
    }
    async repair(shardId?: string, options: any = {}): Promise<void> {
        this.logger.log("repair", shardId, options);
    }
}
