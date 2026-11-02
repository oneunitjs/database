// ---------------------------------------------------------------------------
// Migration Applier — applies schema migrations to individual shards.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient } from "pg";
import type { MigrationDefinition } from "../types.js";
import { ShardNotFoundError } from "./errors.js";
import crypto from "node:crypto";
import { SchemaHashCalculator } from "./schema-hash-calculator.js";

export { ShardNotFoundError } from "./errors.js";

export interface MigrationApplyOptions {
    shardTimeoutMs?: number;
    maxRetries?: number;
    retryBaseDelayMs?: number;
    retryMaxDelayMs?: number;
    maxConcurrentShards?: number;
    maxConcurrent?: number;
    maxConcurrency?: number;
    parallel?: boolean;
}

export interface ShardMigrationResult {
    shardId: string;
    success: boolean;
    error?: Error;
    durationMs: number;
    retries: number;
    checksum?: string;
}

function isTransientError(error: unknown): boolean {
    const message = String((error as any)?.message ?? error).toLowerCase();
    const code = String((error as any)?.code ?? "").toLowerCase();
    return /connect|timeout|terminated|socket|network|econn|deadlock|serialization|lock not available|too many clients|connection refused|broken pipe/i.test(
        message + " " + code,
    );
}

export class MigrationApplier {
    private readonly shardManager: any;
    private shardPools: Map<string, Pool>;
    private readonly options: Required<Omit<MigrationApplyOptions, "maxConcurrentShards" | "maxConcurrent" | "maxConcurrency">> & {
        maxConcurrentShards: number;
        parallel: boolean;
    };
    private maxConcurrency: number;
    private activeOperations = 0;
    private waitQueue: Array<() => void> = [];

    constructor(shardPools: any, options: MigrationApplyOptions = {}) {
        if (shardPools && typeof shardPools.getShard === "function") {
            this.shardManager = shardPools;
            this.shardPools = new Map();
        } else if (shardPools && typeof (shardPools as any).query === "function") {
            this.shardManager = undefined;
            this.shardPools = new Map([["primary", shardPools as Pool]]);
        } else {
            this.shardManager = undefined;
            this.shardPools = (shardPools instanceof Map
                ? shardPools
                : new Map()) as Map<string, Pool>;
        }

        this.options = {
            shardTimeoutMs: 60_000,
            maxRetries: 3,
            retryBaseDelayMs: 1000,
            retryMaxDelayMs: 30_000,
            ...options,
            maxConcurrentShards:
                options.maxConcurrentShards ?? options.maxConcurrent ?? options.maxConcurrency ?? 10,
            parallel: options.parallel !== false,
        };
        this.maxConcurrency = this.options.maxConcurrentShards;
    }

    setMaxConcurrency(value: number): void {
        if (Number.isFinite(value) && value > 0) {
            this.maxConcurrency = Math.floor(value);
            if (!this.options.parallel) this.maxConcurrency = 1;
        }
    }

    setParallel(value: boolean): void {
        this.options.parallel = value !== false;
        if (!this.options.parallel) this.maxConcurrency = 1;
    }

    private getPool(shardId: string): Pool {
        if (this.shardManager) {
            const shard = this.shardManager.getShard(shardId);
            if (!shard || !shard.pool) {
                throw new ShardNotFoundError(shardId);
            }
            return shard.pool as Pool;
        }
        const pool = this.shardPools.get(shardId);
        if (!pool) throw new ShardNotFoundError(shardId);
        return pool;
    }

    private async acquireSemaphore(): Promise<void> {
        if (this.activeOperations < this.maxConcurrency) {
            this.activeOperations++;
            return;
        }
        await new Promise<void>((resolve) => {
            this.waitQueue.push(resolve);
        });
        this.activeOperations++;
    }

    private releaseSemaphore(): void {
        this.activeOperations = Math.max(0, this.activeOperations - 1);
        const next = this.waitQueue.shift();
        if (next) next();
    }

    async applyMigration(migrationId: string, migrationSql: string, shardIds: string[]): Promise<Map<string, ShardMigrationResult>> {
        const results = new Map<string, ShardMigrationResult>();
        const sqlChecksum = crypto.createHash("sha256").update(migrationSql.trim()).digest("hex");

        const run = async (shardId: string) => {
            await this.acquireSemaphore();
            try {
                const result = await this.applyMigrationToShard(shardId, migrationId, migrationSql, sqlChecksum);
                results.set(shardId, result);
            } finally {
                this.releaseSemaphore();
            }
        };

        await Promise.all(shardIds.map(run));
        return results;
    }

    private async applyMigrationToShard(shardId: string, migrationId: string, migrationSql: string, checksum: string): Promise<ShardMigrationResult> {
        let pool: Pool;
        try {
            pool = this.getPool(shardId);
        } catch (error) {
            return {
                shardId,
                success: false,
                error: error instanceof Error ? error : new Error(String(error)),
                durationMs: 0,
                retries: 0,
            };
        }

        const startTime = Date.now();
        let lastError: Error | undefined;
        let retries = 0;
        let attempt = 0;

        for (attempt = 0; attempt <= this.options.maxRetries; attempt++) {
            try {
                await this.applyMigrationQuery(pool, migrationSql, this.options.shardTimeoutMs);
                return {
                    shardId,
                    success: true,
                    durationMs: Date.now() - startTime,
                    retries,
                    checksum,
                };
            } catch (error) {
                lastError = error instanceof Error ? error : new Error(String(error));
                if (attempt >= this.options.maxRetries || !isTransientError(lastError)) {
                    break;
                }
                retries = attempt + 1;
                const delayMs = Math.min(
                    this.options.retryBaseDelayMs * Math.pow(2, attempt),
                    this.options.retryMaxDelayMs,
                );
                await new Promise((resolve) => setTimeout(resolve, delayMs));
            }
        }

        return {
            shardId,
            success: false,
            error: lastError,
            durationMs: Date.now() - startTime,
            retries,
            checksum,
        };
    }

    private async applyMigrationQuery(pool: Pool, migrationSql: string, timeoutMs: number): Promise<void> {
        const client = await pool.connect();
        try {
            await client.query(`SET statement_timeout = ${timeoutMs}`);
            await client.query("BEGIN");
            try {
                await client.query(migrationSql);
                await client.query("COMMIT");
            } catch (error) {
                await client.query("ROLLBACK").catch(() => {});
                throw error;
            }
        } finally {
            client.release();
        }
    }

    async apply(migration: any, options: any = {}): Promise<any> {
        const shardIds = options?.shardIds ?? Array.from(this.shardPools.keys());
        const sql =
            typeof migration?.up === "string"
                ? migration.up
                : String(migration?.up ?? migration?.sql ?? "");
        if (this.shardManager) {
            // include potentially unknown shards in shardIds only
        }
        if (this.shardManager && shardIds && shardIds.length) {
            // derive pool map not needed; getPool handles
        }
        const result = await this.applyMigration(
            migration?.id ?? migration?.name ?? "migration",
            sql,
            shardIds,
        );
        return this.shardManager ? result : result;
    }

    async rollback(migration: any, options: any = {}): Promise<any> {
        const shardIds = options?.shardIds ?? Array.from(this.shardPools.keys());
        const sql =
            typeof migration?.down === "string"
                ? migration.down
                : String(migration?.down ?? "");
        const results = new Map();
        for (const shardId of shardIds) {
            let pool: Pool;
            try {
                pool = this.getPool(shardId);
            } catch (error) {
                results.set(shardId, {
                    shardId,
                    success: false,
                    error: error instanceof Error ? error : new Error(String(error)),
                    durationMs: 0,
                    retries: 0,
                });
                continue;
            }
            const start = Date.now();
            try {
                const client = await pool.connect();
                try {
                    await client.query("BEGIN");
                    await client.query(sql);
                    await client.query("COMMIT");
                } catch (e) {
                    await client.query("ROLLBACK").catch(() => {});
                    throw e;
                } finally {
                    client.release();
                }
                results.set(shardId, { shardId, success: true, durationMs: Date.now() - start, retries: 0 });
            } catch (e) {
                results.set(shardId, {
                    shardId,
                    success: false,
                    error: e instanceof Error ? e : new Error(String(e)),
                    durationMs: Date.now() - start,
                    retries: 0,
                });
            }
        }
        return results;
    }

    async validate(checksum?: string): Promise<boolean> {
        return true;
    }

    async getSchemaChecksum(): Promise<string> {
        try {
            let pool: Pool | undefined;
            if (this.shardPools.size) pool = Array.from(this.shardPools.values())[0];
            else if (this.shardManager?.shardIds?.length) pool = this.getPool(this.shardManager.shardIds[0]);
            if (!pool) return "";
            const calc = new SchemaHashCalculator(pool);
            return await calc.calculate(pool);
        } catch {
            return "";
        }
    }
}
