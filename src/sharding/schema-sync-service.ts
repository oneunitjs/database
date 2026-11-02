// ---------------------------------------------------------------------------
// SchemaSyncService — main orchestration service for schema auto-sync.
// ---------------------------------------------------------------------------

import { Pool } from "pg";
import crypto from "node:crypto";
import type {
    SchemaSyncOptions,
    SchemaSyncResult,
    ShardSyncResult,
    SchemaConsistencyReport,
    ShardSchemaInfo,
    SchemaSyncOperation,
    SchemaSyncStatus,
} from "./schema-sync-types.js";
import { SchemaHashCalculator } from "./schema-hash-calculator.js";
import { SchemaValidator } from "./schema-validator.js";
import { SchemaVersionTracker } from "./schema-version-tracker.js";
import { SyncLockManager } from "./sync-lock-manager.js";
import { MigrationApplier } from "./migration-applier.js";
import { SyncStateStore } from "./sync-state-store.js";

function isPool(x: any): boolean {
    return !!x && typeof x.query === "function";
}

function poolsFromShardManager(sm: any): Map<string, Pool> {
    const map = new Map<string, Pool>();
    if (!sm) return map;
    const ids = Array.isArray(sm.shardIds) ? sm.shardIds : [];
    for (const id of ids) {
        const shard = typeof sm.getShard === "function" ? sm.getShard(id) : null;
        if (shard?.pool) map.set(id, shard.pool);
    }
    // Fallback: if shardManager exposes pools directly
    if (map.size === 0 && Array.isArray(sm.shards)) {
        for (const s of sm.shards) {
            if (s?.id && s?.pool) map.set(s.id, s.pool);
        }
    }
    return map;
}

export class SchemaSyncService {
    private readonly coordinationPool: Pool;
    private readonly shardPools: Map<string, Pool>;
    private readonly shardManager: any;
    private readonly logger: any;
    private readonly hooks: any;
    private readonly coordinationPoolOwned: boolean;
    private readonly schemaHashCalculator: SchemaHashCalculator;
    private readonly schemaValidator: SchemaValidator;
    private readonly schemaVersionTracker: SchemaVersionTracker;
    private readonly syncLockManager: SyncLockManager;
    private readonly migrationApplier: MigrationApplier;
    private readonly stateStore: SyncStateStore;
    private readonly options: any;

    constructor(...args: any[]) {
        let coordinationPool: Pool;
        let shardPools: Map<string, Pool> = new Map();
        let shardManager: any;
        let logger: any;
        let hooks: any;
        let options: SchemaSyncOptions = {};

        if (args.length === 1 && args[0] && typeof args[0] === "object" && !isPool(args[0])) {
            const o = args[0];
            coordinationPool = o.coordinationPool ?? o.coordination ?? o.pool;
            if (o.shardPools instanceof Map) shardPools = o.shardPools;
            if (o.shardManager) {
                shardManager = o.shardManager;
                const derived = poolsFromShardManager(shardManager);
                if (derived.size > 0) shardPools = derived;
            }
            logger = o.logger;
            hooks = o.hooks;
            options = o.options ?? o.syncOptions ?? {};
        } else {
            coordinationPool = args[0];
            const arg2 = args[1];
            if (arg2 instanceof Map) {
                shardPools = arg2;
            } else if (isPool(arg2)) {
                // single pool treated as primary
                shardPools = new Map([["primary", arg2 as Pool]]);
            } else if (arg2 && typeof arg2.getShard === "function") {
                shardManager = arg2;
                const derived = poolsFromShardManager(arg2);
                if (derived.size > 0) shardPools = derived;
            }
            // options could be 3rd or later
            if (args[2] && typeof args[2] === "object" && !isPool(args[2])) {
                // could be logger/options/hooks
                if (typeof args[2].info === "function" || typeof args[2].debug === "function") {
                    logger = args[2];
                    options = args[3] ?? {};
                    hooks = args[4];
                } else if (args[2].timeoutMs !== undefined || args[2].shardTimeoutMs !== undefined || args[2].enabled !== undefined) {
                    options = args[2];
                    logger = args[3];
                    hooks = args[4];
                } else {
                    options = args[2];
                }
            }
            // if args includes options object later
            for (let i = 3; i < args.length; i++) {
                const a = args[i];
                if (!a) continue;
                if (typeof a.info === "function" || typeof a.debug === "function") logger = a;
                else if (a.onSyncStart || a.onSyncFail || a.onShardStart) hooks = a;
                else if (typeof a === "object") options = { ...options, ...a };
            }
        }

        if (!coordinationPool && args[0] && isPool(args[0])) {
            coordinationPool = args[0] as Pool;
        }

        if (typeof coordinationPool === "string") {
            coordinationPool = new Pool({ connectionString: coordinationPool } as any);
        } else if (coordinationPool && !isPool(coordinationPool)) {
            coordinationPool = new Pool(coordinationPool as any);
        }

        this.coordinationPool = coordinationPool;
        this.shardPools = shardPools.size ? shardPools : new Map();
        this.shardManager = shardManager;
        this.logger = logger;
        this.hooks = hooks;
        this.coordinationPoolOwned = false;
        this.options = {
            timeoutMs: options?.timeoutMs ?? 300_000,
            shardTimeoutMs: options?.shardTimeoutMs ?? 60_000,
            maxRetries: options?.maxRetries ?? 3,
            retryBaseDelayMs: options?.retryBaseDelayMs ?? 1000,
            retryMaxDelayMs: options?.retryMaxDelayMs ?? 30_000,
            parallel: options?.parallel ?? false,
            maxParallelShards:
                options?.maxParallelShards ??
                (options as any)?.maxConcurrentShards ??
                (options as any)?.maxConcurrent ??
                (options as any)?.maxConcurrency ??
                10,
            skipUnavailableShards: options?.skipUnavailableShards ?? false,
            continueOnError: options?.continueOnError ?? false,
            validateConsistency: options?.validateConsistency ?? true,
            dryRun: options?.dryRun ?? false,
            blockWritesOnInconsistency: options?.blockWritesOnInconsistency ?? false,
            syncLockTtlMs: options?.syncLockTtlMs ?? 300_000,
            syncLockAcquireTimeoutMs: options?.syncLockAcquireTimeoutMs ?? 60_000,
        };

        this.schemaHashCalculator = new SchemaHashCalculator(coordinationPool);
        this.schemaValidator = new SchemaValidator(coordinationPool);
        this.schemaVersionTracker = new SchemaVersionTracker(
            coordinationPool,
            this.schemaHashCalculator,
        );
        this.syncLockManager = new SyncLockManager(coordinationPool, {
            ttlMs: this.options.syncLockTtlMs,
            acquireTimeoutMs: this.options.syncLockAcquireTimeoutMs,
        });
        this.migrationApplier = new MigrationApplier(this.shardPools, {
            shardTimeoutMs: this.options.shardTimeoutMs,
            maxRetries: this.options.maxRetries,
            retryBaseDelayMs: this.options.retryBaseDelayMs,
            retryMaxDelayMs: this.options.retryMaxDelayMs,
            maxConcurrentShards: this.options.maxParallelShards,
            parallel: this.options.parallel,
        });
        this.stateStore = new SyncStateStore(coordinationPool);
    }

    private shardIds(): string[] {
        return this.shardManager?.shardIds
            ? Array.from(this.shardManager.shardIds)
            : Array.from(this.shardPools.keys());
    }

    private ensureShardPools() {
        if (this.shardPools.size === 0 && this.shardManager) {
            const derived = poolsFromShardManager(this.shardManager);
            if (derived.size > 0) {
                this.shardPools.clear();
                for (const [k, v] of derived) this.shardPools.set(k, v);
                (this as any).migrationApplier = new MigrationApplier(this.shardPools, {
                    shardTimeoutMs: this.options.shardTimeoutMs,
                    maxRetries: this.options.maxRetries,
                    retryBaseDelayMs: this.options.retryBaseDelayMs,
                    retryMaxDelayMs: this.options.retryMaxDelayMs,
                    maxConcurrentShards: this.options.maxParallelShards,
                    parallel: this.options.parallel,
                });
            }
        }
    }

    private async runHooks(name: string, ...args: any[]) {
        const fn = this.hooks?.[name];
        if (typeof fn === "function") {
            try {
                await fn(...args);
            } catch {
                // ignore hook errors
            }
        }
    }

    async sync(migrations: any, options?: SchemaSyncOptions): Promise<SchemaSyncResult> {
        const list = Array.isArray(migrations) ? migrations : [migrations];
        const opts = { ...this.options, ...(options ?? {}) };
        let last: SchemaSyncResult | undefined;
        for (const migration of list) {
            last = await this.syncMigration(migration, opts);
            if (last.status !== "completed" && opts.continueOnError !== true) {
                break;
            }
        }
        if (!last) {
            last = {
                operationId: crypto.randomUUID(),
                status: "completed" as any,
                migrationVersion: "",
                migrationName: "",
                totalShards: this.shardPools.size,
                invalidatedCount: 0,
                activeTransactionCount: 0,
                errors: [],
            };
        }
        return last;
    }

    async syncMigration(migration: any, options?: SchemaSyncOptions): Promise<SchemaSyncResult> {
        this.ensureShardPools();
        const opts = { ...this.options, ...(options ?? {}) };
        const operationId = crypto.randomUUID();
        const startTime = Date.now();
        const migrationId = migration?.id ?? migration?.name ?? "migration";
        const migrationName = migration?.name ?? migrationId;
        const sql =
            typeof migration?.up === "string"
                ? migration.up
                : String(migration?.up ?? migration?.sql ?? "");
        const checksum = crypto.createHash("sha256").update(sql).digest("hex");
        const shardIds = Array.from(this.shardPools.keys());

        const operation: SchemaSyncOperation = {
            operationId,
            status: "pending",
            migrationVersion: migrationId,
            migrationName,
            migrationSql: sql,
            checksum,
            startedAt: new Date(),
            createdAt: new Date(),
            updatedAt: new Date(),
            retries: 0,
        };

        await this.stateStore.recordOperation(operation);
        await this.runHooks("onSyncStart", operation);

        const holder = `schema-sync-${operationId}`;

        try {
            if (opts.dryRun) {
                await this.stateStore.updateStatus(operationId, "completed");
                const result: SchemaSyncResult = {
                    operationId,
                    status: "completed",
                    migrationVersion: migrationId,
                    migrationName,
                    totalShards: shardIds.length,
                    invalidatedCount: 0,
                    activeTransactionCount: 0,
                    errors: [],
                    successfulShards: shardIds.length,
                    failedShards: 0,
                    shardResults: [],
                    durationMs: Date.now() - startTime,
                    checksum,
                };
                await this.runHooks("onSyncComplete", operation, result);
                return result;
            }

            const acquired = await this.syncLockManager.acquireLock(
                `schema_sync:${migrationId}`,
                holder,
                opts.syncLockTtlMs ?? this.options.syncLockTtlMs,
            );

            if (!acquired) {
                throw new Error(`Failed to acquire sync lock for migration ${migrationId}`);
            }

            try {
                await this.stateStore.updateStatus(operationId, "applying");
                operation.status = "applying";
                await this.runHooks("onSyncStart", operation);

                this.migrationApplier.setMaxConcurrency?.(
                    opts.maxParallelShards ?? opts.maxConcurrentShards ?? opts.maxConcurrent ?? opts.maxConcurrency ?? this.options.maxParallelShards,
                );
                this.migrationApplier.setParallel?.(opts.parallel);
                const results = await this.migrationApplier.applyMigration(
                    migrationId,
                    sql,
                    shardIds,
                );

                let successfulShards = 0;
                let failedShards = 0;
                const errors: string[] = [];
                const shardResults: ShardSyncResult[] = [];

                for (const [shardId, result] of results) {
                    if (this.hooks) {
                        await this.runHooks("onShardStart", operation, shardId);
                    }
                    const sr: ShardSyncResult = {
                        shardId,
                        success: result.success,
                        error: result.error,
                        durationMs: result.durationMs,
                        retries: result.retries,
                        checksum: result.checksum ?? checksum,
                    };
                    shardResults.push(sr);
                    if (result.success) {
                        successfulShards++;
                        await this.schemaVersionTracker.recordVersion(
                            shardId,
                            migrationId,
                            result.checksum ?? checksum,
                        );
                        await this.runHooks("onShardComplete", operation, shardId, sr);
                    } else {
                        failedShards++;
                        errors.push(
                            result.error
                                ? result.error.message
                                : `Unknown error on shard ${shardId}`,
                        );
                        await this.runHooks("onShardFail", operation, shardId, result.error ?? new Error("unknown"));
                    }
                }

                const successfulShardIds = shardResults
                    .filter((r) => r.success)
                    .map((r) => r.shardId);

                if (
                    failedShards > 0 &&
                    opts.continueOnError !== true &&
                    typeof migration?.down === "string" &&
                    successfulShardIds.length > 0
                ) {
                    await this.runHooks("onRollbackStart", operation);
                    for (const shardId of successfulShardIds) {
                        const pool = this.shardPools.get(shardId);
                        if (!pool) continue;
                        try {
                            const client = await pool.connect();
                            try {
                                await client.query("BEGIN");
                                await client.query(migration.down);
                                await client.query("COMMIT");
                            } catch {
                                await client.query("ROLLBACK").catch(() => {});
                            } finally {
                                client.release();
                            }
                        } catch {
                            // ignore rollback errors here
                        }
                    }
                    await this.stateStore.updateStatus(operationId, "rolled_back", errors[0] ? new Error(errors[0]) : undefined);
                    await this.runHooks("onRollbackComplete", operation);
                }

                // update statuses based on failure
                const status: SchemaSyncStatus =
                    failedShards === 0
                        ? "completed"
                        : successfulShardIds.length > 0 &&
                          opts.continueOnError !== true &&
                          typeof migration?.down === "string"
                          ? "failed"
                          : "failed";
                if (!(failedShards > 0 && opts.continueOnError !== true && typeof migration?.down === "string" && successfulShardIds.length > 0)) {
                    await this.stateStore.updateStatus(operationId, status, errors[0] ? new Error(errors[0]) : undefined);
                }

                if (opts.validateConsistency && failedShards === 0) {
                    await this.runHooks("onValidationStart", operation);
                    const report = await this.validate({ version: migrationId });
                    await this.runHooks("onValidationComplete", operation, report);
                }

                const result: SchemaSyncResult = {
                    operationId,
                    status,
                    migrationVersion: migrationId,
                    migrationName,
                    totalShards: results.size,
                    invalidatedCount: successfulShards,
                    activeTransactionCount: 0,
                    errors,
                    successfulShards,
                    failedShards,
                    shardResults,
                    durationMs: Date.now() - startTime,
                    checksum,
                };

                operation.status = status;
                operation.completedAt = new Date();
                operation.updatedAt = new Date();
                await this.stateStore.recordOperation(operation);

                await this.runHooks(
                    status === "completed" ? "onSyncComplete" : "onSyncFail",
                    operation,
                    status === "completed" ? result : new Error(errors.join("; ")),
                );

                return result;
            } finally {
                await this.syncLockManager.releaseLock(`schema_sync:${migrationId}`, holder);
            }
        } catch (error) {
            await this.stateStore.updateStatus(
                operationId,
                "failed",
                error instanceof Error ? error : new Error(String(error)),
            );
            const result: SchemaSyncResult = {
                operationId,
                status: "failed",
                migrationVersion: migrationId,
                migrationName,
                totalShards: shardIds.length,
                invalidatedCount: 0,
                activeTransactionCount: 0,
                errors: [error instanceof Error ? error.message : String(error)],
                successfulShards: 0,
                failedShards: shardIds.length,
                shardResults: [],
                durationMs: Date.now() - startTime,
                checksum,
                error: error instanceof Error ? error : new Error(String(error)),
            };
            await this.runHooks("onSyncFail", operation, error instanceof Error ? error : new Error(String(error)));
            return result;
        }
    }

    async rollback(migration: any, options?: SchemaSyncOptions): Promise<SchemaSyncResult> {
        this.ensureShardPools();
        const opts = { ...this.options, ...(options ?? {}) };
        const start = Date.now();
        const migrationId = migration?.id ?? migration?.name ?? "migration";
        const migrationName = migration?.name ?? migrationId;
        const sql =
            typeof migration?.down === "string"
                ? migration.down
                : String(migration?.down ?? "");
        const shardIds = Array.from(this.shardPools.keys());
        const results = await this.migrationApplier.applyMigration
            ? null
            : null;
        // apply down SQL manually
        const shardResults: ShardSyncResult[] = [];
        let successful = 0;
        let failed = 0;
        const errors: string[] = [];
        const opId = crypto.randomUUID();
        for (const shardId of shardIds) {
            const pool = this.shardPools.get(shardId)!;
            const s = Date.now();
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
                successful++;
                shardResults.push({
                    shardId,
                    success: true,
                    durationMs: Date.now() - s,
                    retries: 0,
                    checksum: "",
                });
            } catch (e) {
                failed++;
                const err = e instanceof Error ? e : new Error(String(e));
                errors.push(err.message);
                shardResults.push({
                    shardId,
                    success: false,
                    error: err,
                    durationMs: Date.now() - s,
                    retries: 0,
                });
            }
        }
        const status = failed === 0 ? "rolled_back" : "failed";
        return {
            operationId: opId,
            status,
            migrationVersion: migrationId,
            migrationName,
            totalShards: shardIds.length,
            invalidatedCount: successful,
            activeTransactionCount: 0,
            errors,
            successfulShards: successful,
            failedShards: failed,
            shardResults,
            durationMs: Date.now() - start,
            checksum: "",
        };
    }

    async validate(options?: { version?: string }): Promise<SchemaConsistencyReport> {
        this.ensureShardPools();
        const shardIds = Array.from(this.shardPools.keys());
        const report = await this.schemaValidator.validateConsistency(this.shardPools, shardIds);
        if (options?.version) {
            report.expectedVersion = options.version;
            // also version tracker report
        }
        // fill shards from version tracker
        try {
            const versions = await this.schemaVersionTracker.getAllShardVersions();
            if (versions.length > 0) {
                report.shards = versions;
                report.inconsistentShards = report.inconsistentShards.length ? report.inconsistentShards : [];
            }
        } catch {
            // ignore
        }
        return report;
    }

    async getStatus(operationId: string): Promise<SchemaSyncOperation | null> {
        return this.stateStore.getOperation(operationId);
    }

    async cancel(operationId: string): Promise<boolean> {
        try {
            await this.stateStore.updateStatus(operationId, "cancelled");
            return true;
        } catch {
            return false;
        }
    }

    async getHistory(limit?: number): Promise<SchemaSyncOperation[]> {
        return this.stateStore.getHistory(limit);
    }

    async getOperationsByStatus(status: string): Promise<SchemaSyncOperation[]> {
        return this.stateStore.getOperationsByStatus(status);
    }

    async repair(shardId?: string, options?: any): Promise<boolean> {
        this.ensureShardPools();
        const opts = { ...this.options, ...(options ?? {}) };
        const targetShards = shardId ? [shardId] : Array.from(this.shardPools.keys());
        // Re-apply the recorded latest migration to target shards
        try {
            const history = await this.stateStore.getHistory(1);
            const last = history && history[0];
            if (last) {
                await this.migrationApplier.apply({ up: last.migrationSql }, { ...opts, shardIds: targetShards });
            }
            return true;
        } catch {
            return false;
        }
    }

    async getShardVersion(shardId: string): Promise<ShardSchemaInfo | undefined> {
        return this.schemaVersionTracker.getShardVersion(shardId);
    }

    async getAllShardVersions(): Promise<ShardSchemaInfo[]> {
        return this.schemaVersionTracker.getAllShardVersions();
    }

    async validateConsistency(migrationVersion?: string): Promise<SchemaConsistencyReport> {
        return this.validate(migrationVersion ? { version: migrationVersion } : undefined);
    }
}