import type { Pool, PoolClient } from "pg";
import { PreparedTransactionScanner } from "./prepared-transaction-scanner.js";
import type { PreparedTransactionFilter } from "./prepared-transaction-scanner.js";

export interface CleanupDiagnosticsOptions {
    maxAgeMs?: number;
    dryRun?: boolean;
    batchSize?: number;
    autoCleanup?: boolean;
}

export const DEFAULT_CLEANUP_DIAGNOSTICS_OPTIONS: Required<CleanupDiagnosticsOptions> = {
    maxAgeMs: 3_600_000,
    dryRun: false,
    batchSize: 100,
    autoCleanup: false,
};

export interface CleanupResult {
    scanned: number;
    found: number;
    orphaned: number;
    cleanedUp: number;
    failed: number;
    errors: string[];
    recommendations: string[];
}

export interface PreparedTransactionCleanupManagerOptions {
    pool: Pool;
    logger: CleanupDiagnosticsLogger;
    options?: CleanupDiagnosticsOptions;
}

export class TransactionCleanupManager {
    private readonly scanner: PreparedTransactionScanner;
    private readonly logger: CleanupDiagnosticsLogger;
    private readonly options: Required<CleanupDiagnosticsOptions>;

    constructor(
        pool: Pool,
        logger: CleanupDiagnosticsLogger,
        options: CleanupDiagnosticsOptions = DEFAULT_CLEANUP_DIAGNOSTICS_OPTIONS,
    ) {
        this.scanner = new PreparedTransactionScanner(pool, logger);
        this.logger = logger;
        this.options = { ...DEFAULT_CLEANUP_DIAGNOSTICS_OPTIONS, ...options };
    }

    async diagnose(filter?: PreparedTransactionFilter): Promise<CleanupResult> {
        const result: CleanupResult = {
            scanned: 0,
            found: 0,
            orphaned: 0,
            cleanedUp: 0,
            failed: 0,
            errors: [],
            recommendations: [],
        };
        const transactions = await this.scanner.scanPreparedTransactions(filter);
        result.scanned = result.found = transactions.length;
        const orphaned = transactions.filter((t) => t.isOrphaned);
        const old = transactions.filter((t) => t.ageMs > this.options.maxAgeMs);
        result.orphaned = orphaned.length;
        if (orphaned.length > 0) {
            result.recommendations.push(
                "Found " + orphaned.length + " orphaned transactions. Clean up with cleanupOrphanedTransactions().",
            );
        }
        if (old.length > 0) {
            result.recommendations.push(
                "Found " + old.length + " old transactions. Clean up with cleanupOldTransactions().",
            );
        }
        if (this.options.autoCleanup) {
            const cleanup = await this.cleanupOrphanedTransactions();
            result.cleanedUp += cleanup.cleanedUp;
            result.failed += cleanup.failed;
            result.errors.push(...cleanup.errors);
        }
        return result;
    }

    async cleanupTransaction(transactionId: string): Promise<boolean> {
        try {
            const existing = await this.scanner.scanPreparedTransactions({ transactionId });
            if (existing.length === 0) {
                return true;
            }
            if (!this.options.dryRun) {
                await this.rollbackPreparedTransaction(transactionId);
            }
            this.logger.info({ transactionId }, "Cleaned up");
            return true;
        } catch (error) {
            this.logger.error(
                { transactionId, error: error instanceof Error ? error.message : String(error) },
                "Cleanup failed",
            );
            return false;
        }
    }

    async cleanupOrphanedTransactions(batchSize?: number): Promise<CleanupResult> {
        const limit = batchSize ?? this.options.batchSize;
        const result: CleanupResult = {
            scanned: 0,
            found: 0,
            orphaned: 0,
            cleanedUp: 0,
            failed: 0,
            errors: [],
            recommendations: [],
        };
        const transactions = await this.scanner.scanPreparedTransactions();
        const orphaned = transactions.filter((t) => t.isOrphaned);
        result.scanned = result.found = orphaned.length;
        result.orphaned = orphaned.length;
        const toProcess = orphaned.slice(0, limit);
        for (const tx of toProcess) {
            if (await this.cleanupTransaction(tx.transactionId)) {
                result.cleanedUp++;
            } else {
                result.failed++;
            }
        }
        return result;
    }

    async cleanupOldTransactions(maxAgeMs?: number): Promise<CleanupResult> {
        const ageThreshold = maxAgeMs ?? this.options.maxAgeMs;
        const result: CleanupResult = {
            scanned: 0,
            found: 0,
            orphaned: 0,
            cleanedUp: 0,
            failed: 0,
            errors: [],
            recommendations: [],
        };
        const transactions = await this.scanner.scanPreparedTransactions();
        const old = transactions.filter((t) => t.ageMs > ageThreshold);
        result.scanned = result.found = old.length;
        const toProcess = old.slice(0, this.options.batchSize);
        for (const tx of toProcess) {
            if (await this.cleanupTransaction(tx.transactionId)) {
                result.cleanedUp++;
            } else {
                result.failed++;
            }
        }
        return result;
    }

    private async rollbackPreparedTransaction(transactionId: string): Promise<void> {
        const client = await this.scanner.pool.connect();
        try {
            await client.query("BEGIN");
            await client.query("ROLLBACK PREPARED '" + transactionId + "'");
            await client.query("COMMIT");
        } catch (error) {
            try {
                await client.query("ROLLBACK");
            } catch {}
            throw error;
        } finally {
            client.release();
        }
    }
}

export type CleanupDiagnosticsLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

export const defaultCleanupDiagnosticsLogger: CleanupDiagnosticsLogger = {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
};
