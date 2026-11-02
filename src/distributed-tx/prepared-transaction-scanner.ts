import type { Pool } from "pg";

export interface PreparedTransactionFilter {
    transactionId?: string;
}

export interface PreparedTransactionInfo {
    transactionId: string;
    preparedAt: Date;
    preparedBy: string;
    shardId?: string;
    owner: string;
    status: "prepared";
    ageMs: number;
    isOrphaned: boolean;
    canCleanup: boolean;
}

export interface PreparedTransactionScannerOptions {
    maxAgeMs?: number;
    dryRun?: boolean;
    batchSize?: number;
}

export const DEFAULT_PREPARED_TRANSACTION_SCANNER_OPTIONS: Required<PreparedTransactionScannerOptions> = {
    maxAgeMs: 3_600_000,
    dryRun: false,
    batchSize: 100,
};

export class PreparedTransactionScanner {
    constructor(
        public readonly pool: Pool,
        private readonly logger: PreparedTransactionScannerLogger,
        private readonly options: Required<PreparedTransactionScannerOptions> = DEFAULT_PREPARED_TRANSACTION_SCANNER_OPTIONS,
    ) {}

    async scanPreparedTransactions(filter?: PreparedTransactionFilter): Promise<PreparedTransactionInfo[]> {
        const maxAgeMs = this.options.maxAgeMs;
        const query =
            "SELECT gid, prepared, owner, database FROM pg_prepared_xacts" +
            (filter?.transactionId ? " WHERE gid = $1" : "");
        const params = filter?.transactionId ? [filter.transactionId] : [];
        const result = await this.pool.query(query, params);
        const now = new Date();
        return result.rows.map((row: any) => ({
            transactionId: row.gid,
            preparedAt: new Date(row.prepared),
            preparedBy: row.owner ?? "unknown",
            shardId: this.extractShardId(row.gid),
            owner: row.owner ?? "unknown",
            status: "prepared" as const,
            ageMs: now.getTime() - new Date(row.prepared as any).getTime(),
            isOrphaned: !row.owner || this.isOwnerInvalidSync(row.owner ?? ""),
            canCleanup: !row.owner || now.getTime() - new Date(row.prepared as any).getTime() > maxAgeMs,
        }));
    }

    async isOwnerInvalid(owner: string): Promise<boolean> {
        const result = await this.pool.query("SELECT 1 FROM pg_stat_activity WHERE usename = $1 LIMIT 1", [owner]);
        return result.rows.length === 0;
    }

    private isOwnerInvalidSync(owner: string | undefined): boolean {
        return !owner;
    }

    private extractShardId(gid: string): string | undefined {
        const match = gid.match(/^pg_(.+?)_(.+)$/);
        return match ? match[2] : undefined;
    }
}

export type PreparedTransactionScannerLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

export const defaultPreparedTransactionScannerLogger: PreparedTransactionScannerLogger = {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
};
