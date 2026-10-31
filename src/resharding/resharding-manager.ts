// ---------------------------------------------------------------------------
// ReshardingManager — durable, crash-recoverable online resharding lifecycle.
//
// Coordination authority: a single PostgreSQL database (the `coordination`
// pool) owns migration lifecycle state, units, and published routing versions.
// Cross-shard transactions are NOT atomic; per-shard databases own their own
// change log (transactional outbox) and batch checkpoints.
//
// Legal state transitions are enforced with optimistic concurrency control
// (version column) plus an advisory worker lock, so multiple workers/
// processes and crash restarts cannot corrupt migration state.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient, type PoolConfig } from "pg";
import type {
    MigrationDescriptor,
    ProgressEvent,
    ReshardingApi,
    ReshardingOptions,
    ReshardingStatus,
    ReshardingStatusInfo,
    TopologySnapshot,
    ChangeEvent,
    PlanOptions,
    ReshardingPlan,
} from "./resharding-types.js";
import type { MigrationUnit } from "./resharding-types.js";
import {
    ReshardingError,
    InvalidStateTransitionError,
    MigrationNotFoundError,
    MigrationAlreadyExistsError,
    StalePlanError,
    UnrecoverableCutoverError,
    WriteFencedError,
} from "./resharding-errors.js";
import { createReshardingSchema, type ReshardingSchema } from "./resharding-schema.js";
import { planResharding } from "./resharding-planner.js";
import { ReshardingStore } from "./resharding-store.js";
import { ReshardingCopier } from "./resharding-copier.js";
import { ReshardingChangeCapture } from "./resharding-change-capture.js";
import { ReshardingCutover } from "./resharding-cutover.js";
import type { ShardManager } from "../sharding/shard-manager.js";

const LEGAL_TRANSITIONS: Record<ReshardingStatus, Set<ReshardingStatus>> = {
    planned: new Set(["copying", "paused", "cancelled", "failed"]),
    copying: new Set(["catching_up", "paused", "failed", "cancelled"]),
    catching_up: new Set(["validating", "paused", "failed", "cancelled"]),
    validating: new Set(["ready_for_cutover", "paused", "failed", "cancelled", "rollback_required"]),
    ready_for_cutover: new Set(["cutting_over", "paused", "failed", "cancelled", "rollback_required"]),
    cutting_over: new Set(["completed", "failed", "rollback_required"]),
    completed: new Set(["finalized"]),
    paused: new Set(["copying", "catching_up", "validating", "cancelled", "failed"]),
    failed: new Set(["cancelled", "rollback_required", "planned"]),
    rollback_required: new Set(["cancelled", "planned"]),
    cancelled: new Set([]),
    finalized: new Set([]),
};

function nowIso(): string {
    return new Date().toISOString();
}

export class ReshardingManager implements ReshardingApi {
    private readonly coordination: Pool;
    private readonly ownsCoordination: boolean;
    private readonly schema: ReshardingSchema;
    private readonly store: ReshardingStore;
    private readonly changeCapture: ReshardingChangeCapture;
    private readonly copier: ReshardingCopier;
    private readonly cutoverEngine: ReshardingCutover;
    private readonly logger?: ReshardingOptions["logger"];
    private readonly hooks?: ReshardingOptions["hooks"];
    private readonly bulkCopy: Required<ReshardingOptions["bulkCopy"]>;
    private readonly validation: Required<ReshardingOptions["validation"]>;
    private readonly cutoverOpts: Required<ReshardingOptions["cutover"]>;
    private closed = false;

    private descriptors = new Map<string, MigrationDescriptor>();
    private activeMigrationId: string | null = null;
    private coordinationInitialized = false;
    private coordinationInitPromise: Promise<void> | null = null;

    constructor(
        coordination: ReshardingOptions["coordination"],
        options: ReshardingOptions = {},
        private readonly manager?: ShardManager,
    ) {
        this.schema = createReshardingSchema();
        this.logger = options.logger;
        this.hooks = options.hooks;

        if (typeof coordination === "object" && "connect" in coordination) {
            this.coordination = coordination as Pool;
            this.ownsCoordination = false;
        } else {
            this.coordination = new Pool(coordination as PoolConfig);
            this.ownsCoordination = true;
        }

        const defaults = {
            batchSize: 1000,
            maxConcurrency: 4,
            maxRetries: 5,
            baseRetryDelayMs: 100,
            maxRetryDelayMs: 10000,
            throttleMs: 0,
            batchTimeoutMs: 30000,
            idleTimeoutMs: 300000,
        } as const;
        const vDefaults = {
            enabled: true,
            checkCounts: true,
            checkDuplicates: true,
            checkChecksums: true,
            checkDestinationPlacement: true,
        } as const;
        const cDefaults = {
            drainTimeoutMs: 60000,
            writeFenceTimeoutMs: 30000,
            requireValidation: true,
        } as const;

        this.bulkCopy = { ...defaults, ...(options.bulkCopy ?? {}) };
        this.validation = { ...vDefaults, ...(options.validation ?? {}) };
        this.cutoverOpts = { ...cDefaults, ...(options.cutover ?? {}) };

        this.store = new ReshardingStore(this.coordination, this.logger, this.hooks);
        this.changeCapture = new ReshardingChangeCapture({
            coordination: this.coordination,
            getShardPool: (shardId: string) => this.manager?.getShard(shardId)?.pool,
        });
        this.copier = new ReshardingCopier(
            this.coordination,
            this.manager,
            this.changeCapture,
            this.bulkCopy,
            this.logger,
            this.hooks,
            this.initShard.bind(this),
        );
        this.cutoverEngine = new ReshardingCutover(
            this.coordination,
            this.manager,
            this.store,
            this.changeCapture,
            this.copier,
            this.cutoverOpts,
            this.logger,
            this.hooks,
        );
    }

    get migrationId(): string | null {
        return this.activeMigrationId;
    }

    async init(): Promise<void> {
        await this.ensureCoordinationInitialized();
    }

    private async ensureCoordinationInitialized(): Promise<void> {
        if (this.coordinationInitialized) return;
        if (this.coordinationInitPromise) {
            await this.coordinationInitPromise;
            return;
        }
        this.coordinationInitPromise = (async () => {
            const client = await this.coordination.connect();
            try {
                await this.schema.initCoordination(client);
            } finally {
                client.release();
            }
            this.coordinationInitialized = true;
        })();
        await this.coordinationInitPromise;
    }

    async initShard(shardId: string, pool: Pool): Promise<void> {
        const client = await pool.connect();
        try {
            await this.schema.initShard(client, shardId);
        } finally {
            client.release();
        }
    }

    async plan(
        descriptor: MigrationDescriptor,
        targetTopology: TopologySnapshot,
        options: PlanOptions = {},
    ): Promise<ReshardingPlan> {
        await this.ensureCoordinationInitialized();
        if (this.closed) throw new ReshardingError("Resharding manager is shut down");

        const migrationId = descriptor.migrationId;
        const existing = await this.store.getMigration(migrationId);
        if (existing) throw new MigrationAlreadyExistsError(migrationId);

        const sourceTopology = await this.captureSourceTopology(options.expectedRoutingVersion);

        const plan = await planResharding(sourceTopology, targetTopology, migrationId, options);

        if (options.dryRun) return plan;

        await this.store.insertMigration(plan, descriptor);
        this.descriptors.set(migrationId, descriptor);
        this.activeMigrationId = migrationId;

        this.emitProgress({
            type: "phase-changed",
            migrationId,
            timestamp: nowIso(),
            detail: { phase: "planned", units: plan.units.length },
        });

        return plan;
    }

    /**
     * Reattach to an existing migration after a process restart. Recovers the
     * active migration id from durable coordination state so the migration can
     * be resumed, paused, cancelled, validated, cut over, or finalized.
     */
    async load(migrationId: string): Promise<ReshardingStatusInfo | undefined> {
        await this.ensureCoordinationInitialized();
        if (this.closed) throw new ReshardingError("Resharding manager is shut down");

        const row = await this.store.getMigration(migrationId);
        if (!row) return undefined;

        this.activeMigrationId = migrationId;
        // Rebind the descriptor if previously planned by this process; otherwise
        // the caller must re-supply it before copy/validate/cutover.
        return this.buildStatus(migrationId);
    }

    async start(): Promise<ReshardingStatusInfo> {
        await this.ensureCoordinationInitialized();
        if (this.closed) throw new ReshardingError("Resharding manager is shut down");
        const migrationId = this.requireActive();
        const row = await this.store.getMigration(migrationId);
        if (!row) throw new MigrationNotFoundError(migrationId);

        await this.store.claimWorkerLock(migrationId);
        await this.store.transition(migrationId, row.status, "copying", row.version);
        this.activeMigrationId = migrationId;

        this.emitProgress({ type: "phase-changed", migrationId, timestamp: nowIso(), detail: { phase: "copying" } });
        return this.buildStatus(migrationId);
    }

    async getStatus(): Promise<ReshardingStatusInfo | undefined> {
        await this.ensureCoordinationInitialized();
        if (!this.activeMigrationId) return undefined;
        return this.buildStatus(this.activeMigrationId);
    }

    async pause(): Promise<ReshardingStatusInfo> {
        const migrationId = this.requireActive();
        const row = await this.store.requireMigration(migrationId);
        const allowed: ReshardingStatus[] = ["copying", "catching_up", "validating", "ready_for_cutover"];
        if (!allowed.includes(row.status as ReshardingStatus)) {
            throw new InvalidStateTransitionError(migrationId, row.status, "paused");
        }
        await this.store.transition(migrationId, row.status, "paused", row.version);
        this.emitProgress({ type: "phase-changed", migrationId, timestamp: nowIso(), detail: { phase: "paused" } });
        return this.buildStatus(migrationId);
    }

    async resume(): Promise<ReshardingStatusInfo> {
        const migrationId = this.requireActive();
        const row = await this.store.requireMigration(migrationId);
        if (row.status !== "paused") {
            throw new InvalidStateTransitionError(migrationId, row.status, "copying/catching_up/validating");
        }
        await this.store.claimWorkerLock(migrationId);
        await this.store.transition(migrationId, "paused", "copying", row.version);
        return this.buildStatus(migrationId);
    }

    async cancel(): Promise<ReshardingStatusInfo> {
        const migrationId = this.requireActive();
        const row = await this.store.requireMigration(migrationId);
        const terminal = ["completed", "finalized", "cancelled"];
        if (terminal.includes(row.status as ReshardingStatus)) {
            throw new InvalidStateTransitionError(migrationId, row.status, "cancelled");
        }
        await this.store.releaseWorkerLock(migrationId);
        await this.store.transition(migrationId, row.status, "cancelled", row.version);
        // Unblock writes fenced during the (cancelled) cutover.
        for (const sid of new Set((row.units ?? []).map((u: { sourceShardId: string }) => u.sourceShardId))) {
            const shard = this.manager?.getShard(sid);
            if (shard?.pool) await this.store.setWriteGate(shard.pool, migrationId, false).catch(() => {});
        }
        this.emitProgress({ type: "phase-changed", migrationId, timestamp: nowIso(), detail: { phase: "cancelled" } });
        return this.buildStatus(migrationId);
    }

    async validate(descriptor?: MigrationDescriptor): Promise<{ report: import("./resharding-types.js").ValidationReport[]; passed: boolean }> {
        await this.ensureCoordinationInitialized();
        const migrationId = this.requireActive();
        const row = await this.store.requireMigration(migrationId);
        const from = row.status;
        if (from !== "catching_up" && from !== "validating" && from !== "ready_for_cutover") {
            throw new InvalidStateTransitionError(migrationId, from, "validating");
        }

        const desc = descriptor ?? this.descriptors.get(migrationId);
        if (!desc) throw new MigrationNotFoundError(migrationId);

        // Replay all changes from the capture boundary before validating.
        // This ensures the destination includes every change committed at or
        // after snapshot time, so validation compares against a complete
        // destination rather than a partial snapshot copy.
        for (const unit of row.units) {
            if (unit.captureTxid !== undefined) {
                await this.changeCapture.drainChanges(
                    migrationId,
                    unit.unitId,
                    Number.MAX_SAFE_INTEGER,
                    async (events: ChangeEvent[]) => {
                        await this.copier.applyChangeEvents(migrationId, unit, events);
                    },
                    unit.captureTxid,
                );
            }
        }

        await this.store.transition(migrationId, from, "validating", row.version);

        const { report, passed } = await this.copier.validateAll(migrationId, desc, row);

        await this.store.transition(
            migrationId,
            "validating",
            passed ? "ready_for_cutover" : "rollback_required",
            row.version + 1,
        );

        return { report, passed };
    }

    async cutover(descriptor?: MigrationDescriptor): Promise<ReshardingStatusInfo> {
        await this.ensureCoordinationInitialized();
        const migrationId = this.requireActive();
        const row = await this.store.requireMigration(migrationId);

        // Recovery: if already in cutting_over state, check if routing version
        // was already published (crash after barrier commit but before routing
        // publication, or after routing publication but before completed
        // transition).
        if (row.status === "cutting_over") {
            const currentVersion = await this.store.currentRoutingVersion();
            if (currentVersion >= row.targetRoutingVersion) {
                // Routing version already published — safe to complete cutover.
                await this.store.transition(migrationId, "cutting_over", "completed", row.version);
                this.emitProgress({
                    type: "cutover-committed",
                    migrationId,
                    timestamp: nowIso(),
                    detail: { routingVersion: row.targetRoutingVersion, recovered: true },
                });
                return this.buildStatus(migrationId);
            }
            // Routing version not yet published — re-attempt cutover. The
            // write gate may still be fenced on source shards, which is fine;
            // acquireWriteBarrier will re-acquire the advisory lock and
            // re-persist fenced=TRUE (idempotent).
        } else if (row.status !== "ready_for_cutover" && row.status !== "cutting_over") {
            throw new InvalidStateTransitionError(migrationId, row.status, "cutting_over");
        }

        const desc = descriptor ?? this.descriptors.get(migrationId);
        if (!desc) throw new MigrationNotFoundError(migrationId);

        try {
            await this.hooks?.onBeforeCutover?.(migrationId, row as unknown as ReshardingPlan);
        } catch (error) {
            await this.store.transition(migrationId, row.status, "rollback_required", row.version);
            throw error;
        }

        if (row.status !== "cutting_over") {
            await this.store.transition(migrationId, row.status, "cutting_over", row.version);
            // Keep the in-memory row version in sync with the cutting_over
            // transition so later transitions use the correct optimistic
            // concurrency version after a process restart as well.
            row.version = row.version + 1;
            row.status = "cutting_over";
        }

        try {
            await this.cutoverEngine.commit(migrationId, desc, row);
            await this.store.transition(migrationId, "cutting_over", "completed", row.version);
            this.emitProgress({
                type: "cutover-committed",
                migrationId,
                timestamp: nowIso(),
                detail: { routingVersion: row.targetRoutingVersion },
            });
            return this.buildStatus(migrationId);
        } catch (error) {
            await this.store.transition(migrationId, "cutting_over", "rollback_required", row.version);
            for (const sid of new Set(row.units.map((u: { sourceShardId: string }) => u.sourceShardId))) {
                const s = this.manager?.getShard(sid);
                if (s?.pool) await this.store.setWriteGate(s.pool, migrationId, false).catch(() => {});
            }
            throw new UnrecoverableCutoverError(
                migrationId,
                error instanceof Error ? error.message : String(error),
            );
        }
    }

    async finalize(): Promise<ReshardingStatusInfo> {
        const migrationId = this.requireActive();
        const row = await this.store.requireMigration(migrationId);
        if (row.status !== "completed") {
            throw new InvalidStateTransitionError(migrationId, row.status, "finalized");
        }
        await this.store.transition(migrationId, row.status, "finalized", row.version);
        await this.store.releaseWorkerLock(migrationId);
        this.emitProgress({ type: "phase-changed", migrationId, timestamp: nowIso(), detail: { phase: "finalized" } });
        return this.buildStatus(migrationId);
    }

    async recordChange(
        client: PoolClient,
        event: Omit<ChangeEvent, "id" | "sequence" | "createdAt">,
    ): Promise<ChangeEvent> {
        await this.ensureCoordinationInitialized();
        return this.changeCapture.recordChange(client, event);
    }

    /**
     * Execute application mutations and CDC change capture atomically in a
     * single transaction on the source shard. The callback receives the
     * transaction client and a `recordChange` function that writes change
     * events through the same client, so mutations and their CDC records
     * commit or roll back together.
     */
    async instrumentedTransaction<T>(
        shardId: string,
        callback: (
            client: PoolClient,
            recordChange: (
                event: Omit<ChangeEvent, "id" | "sequence" | "createdAt">,
            ) => Promise<ChangeEvent>,
        ) => Promise<T> | T,
        _txOptions?: { isolation?: string; readOnly?: boolean; deferrable?: boolean; timeout?: number; retry?: unknown },
    ): Promise<T> {
        await this.ensureCoordinationInitialized();

        // Participate in existing write fencing: reject if the shard is fenced.
        const fenced = await this.isShardWriteFenced(shardId);
        if (fenced) {
            throw new WriteFencedError(shardId, this.activeMigrationId ?? "unknown");
        }

        const shard = this.manager?.getShard(shardId);
        if (!shard?.pool) {
            throw new ReshardingError(
                `Source shard pool unavailable for instrumented transaction: ${shardId}`,
                { code: "SHARD_UNAVAILABLE", transient: true },
            );
        }

        // Register the operation in distributed drain state so removeShard()
        // can see in-flight CDC transactions from this instance.
        const mgr = this.manager;
        if (!mgr) {
            throw new ReshardingError(
                `Shard manager unavailable for instrumented transaction: ${shardId}`,
                { code: "SHARD_UNAVAILABLE", transient: true },
            );
        }
        await mgr.acquireShard(shardId);

        const client = await shard.pool.connect();
        try {
            await client.query("BEGIN");

            const recordChange = async (
                event: Omit<ChangeEvent, "id" | "sequence" | "createdAt">,
            ): Promise<ChangeEvent> => {
                return this.changeCapture.recordChange(client, event);
            };

            try {
                const result = await callback(client, recordChange);
                await client.query("COMMIT");
                return result as T;
            } catch (error) {
                await client.query("ROLLBACK").catch(() => {});
                throw error;
            }
        } finally {
            client.release();
            await mgr.releaseShard(shardId).catch(() => {});
        }
    }

    async drainChanges(migrationId: string, unitId: string, uptoSequence: number, apply?: (events: ChangeEvent[]) => Promise<void>, afterTxid?: number, shardId?: string): Promise<{ applied: number }> {
        await this.ensureCoordinationInitialized();
        const mId = this.requireActive();
        return this.changeCapture.drainChanges(mId, unitId, uptoSequence, apply ?? (async () => {}), afterTxid ?? 0, shardId);
    }

    async refreshRoutingVersion(): Promise<number> {
        return this.store.currentRoutingVersion();
    }

    async isWriteFenced(_shardKey: string): Promise<boolean> {
        const migrationId = this.activeMigrationId;
        if (!migrationId) return false;
        const row = await this.store.getMigration(migrationId);
        if (!row) return false;
        return row.cutoverIntent || row.status === "cutting_over" || row.status === "completed";
    }

    /**
     * Check whether a shard's write gate is fenced. This is the durable
     * enforcement point: the write path consults this before executing any
     * write against a shard. Works across independent application instances
     * because the gate is stored shard-locally in PostgreSQL.
     *
     * Fails closed: if the fencing state cannot be verified (gate table
     * missing, connection error, etc.), the error is propagated so writers
     * cannot silently bypass a required safety barrier. Returns false only
     * when initShard() has not yet run on this shard (no migration active).
     */
    async isShardWriteFenced(shardId: string): Promise<boolean> {
        const shard = this.manager?.getShard(shardId);
        if (!shard?.pool) return false;
        try {
            const result = await this.store.isWriteGateFenced(shard.pool);
            return result.fenced;
        } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            if (/relation "resharding_write_gate" does not exist/.test(msg)) {
                // Gate table doesn't exist yet. If a migration is active,
                // we cannot verify the fence state — fail closed. If no
                // migration is active, initShard() simply hasn't run yet,
                // so allow writes (bootstrap phase).
                if (this.activeMigrationId) {
                    throw new ReshardingError(
                        `Unable to verify write fence state for shard "${shardId}"`,
                        { code: "FENCE_VERIFICATION_FAILED", transient: true },
                    );
                }
                return false;
            }
            throw new ReshardingError(
                `Unable to verify write fence state for shard "${shardId}"`,
                { code: "FENCE_VERIFICATION_FAILED", transient: true },
            );
        }
    }

    async shutdown(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        if (this.activeMigrationId) await this.store.releaseWorkerLock(this.activeMigrationId);
        if (this.ownsCoordination) await this.coordination.end();
    }

    get isClosed(): boolean {
        return this.closed;
    }

    // ------------------------------------------------------------------
    // Bulk copy entry point (called by an external worker loop)
    // ------------------------------------------------------------------

    async copyUnits(descriptor: MigrationDescriptor): Promise<void> {
        await this.ensureCoordinationInitialized();
        const migrationId = this.requireActive();
        const row = await this.store.getMigration(migrationId);
        if (!row) throw new MigrationNotFoundError(migrationId);

        // Capture the initial transaction snapshot boundary per source shard
        // BEFORE starting the snapshot copy.
        //
        // We use txid_current_snapshot() in a REPEATABLE READ transaction on
        // each source shard. The snapshot's xmin is the oldest transaction
        // that could still be active at capture time. All changes with
        // txid < xmin are committed before the snapshot and are included in
        // the copy; all changes with txid >= xmin must be replayed during
        // catching_up. This correctly handles transactions that were in
        // flight at capture time but commit afterward (they have txid >=
        // xmin and will be replayed).
        //
        // txid is per-database in PostgreSQL, so we capture per-shard and
        // store per-unit (keyed by sourceShardId).
        const captureSnapshots = new Map<string, { xmin: number; xmax: number; xip: number[] }>();
        const sourceShardIds = new Set(row.units.map((u) => u.sourceShardId));
        for (const shardId of sourceShardIds) {
            const shard = this.manager?.getShard(shardId);
            if (shard?.pool) {
                const client = await shard.pool.connect();
                try {
                    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
                    const result = await client.query(
                        `SELECT txid_current_snapshot() AS snapshot`,
                    );
                    const row0 = result.rows[0];
                    if (!row0 || !row0.snapshot) {
                        throw new Error(`txid_current_snapshot() returned no value for shard "${shardId}"`);
                    }
                    const snapshot = String(row0.snapshot);
                    // Snapshot format: "xmin:xmax:xip1,xip2,..." (colon-separated)
                    const parts = snapshot.split(":");
                    const xmin = Number(parts[0]);
                    const xmax = Number(parts[1]);
                    const xip = (parts[2] ?? "").split(",").filter(Boolean).map(Number);
                    if (!Number.isFinite(xmin) || !Number.isFinite(xmax)) {
                        throw new Error(`Invalid snapshot for shard "${shardId}": "${snapshot}"`);
                    }
                    captureSnapshots.set(shardId, { xmin, xmax, xip });
                    await client.query("COMMIT");
                } catch (error) {
                    await client.query("ROLLBACK").catch(() => {});
                    throw error;
                } finally {
                    client.release();
                }
            }
        }

        // Persist capture snapshot per unit so replay knows where to start.
        for (const unit of row.units) {
            const snap = captureSnapshots.get(unit.sourceShardId);
            if (snap) {
                await this.store.updateUnitCaptureSnapshot(
                    migrationId,
                    unit.unitId,
                    snap.xmin,
                    snap.xmax,
                    snap.xip,
                );
            }
        }

        await this.copier.copyAll(migrationId, descriptor);
        // Bulk copy complete -> catching up on change-log events.
        if (row.status === "copying") {
            await this.store.transition(migrationId, "copying", "catching_up", row.version);
        }
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private requireActive(): string {
        if (!this.activeMigrationId) throw new MigrationNotFoundError("no active migration");
        return this.activeMigrationId;
    }

    private async buildStatus(migrationId: string): Promise<ReshardingStatusInfo> {
        const row = await this.store.getMigration(migrationId);
        if (!row) throw new MigrationNotFoundError(migrationId);
        return {
            migrationId: row.migrationId,
            status: row.status as ReshardingStatus,
            version: row.version,
            sourceRoutingVersion: row.sourceRoutingVersion,
            targetRoutingVersion: row.targetRoutingVersion,
            createdAt: row.createdAt,
            updatedAt: row.updatedAt,
            startedAt: row.startedAt,
            completedAt: row.completedAt,
            finalizedAt: row.finalizedAt,
            pausedAt: row.pausedAt,
            unitsTotal: row.unitsTotal,
            unitsCompleted: row.unitsCompleted,
            unitsFailed: row.unitsFailed,
            rowsCopied: row.rowsCopied,
            rowsValidated: row.rowsValidated,
            changeLogLag: row.changeLogLag,
            errorCode: row.errorCode,
            errorMessage: row.errorMessage,
            cutoverIntent: row.cutoverIntent,
            cutoverCommitted: row.cutoverCommitted,
        };
    }

    private async captureSourceTopology(expectedVersion?: number): Promise<TopologySnapshot> {
        if (!this.manager) {
            throw new ReshardingError("ShardManager is required to capture the source topology");
        }
        const mgr = this.manager;
        if (!mgr) throw new ReshardingError("ShardManager is required to capture the source topology");
        const liveVersion = mgr.routingVersion;
        if (expectedVersion !== undefined && expectedVersion !== liveVersion) {
            throw new StalePlanError(
                "planning",
                `Expected routing version ${expectedVersion} but live topology is at version ${liveVersion}`,
            );
        }
        const shards = mgr.shardIds.map((id) => {
            const shard = mgr.getShard(id);
            return {
                id,
                version: mgr.shardVersions.get(id) ?? 1,
                connection: { host: undefined, port: undefined, database: undefined, user: undefined },
                addedAt: undefined,
            };
        });
        return {
            shards,
            strategy: "hash",
            strategyConfig: {},
            routingVersion: liveVersion,
            capturedAt: nowIso(),
        };
    }

    private emitProgress(event: ProgressEvent): void {
        try {
            this.hooks?.onProgress?.(event);
        } catch {
            // Advisory hooks must not compromise consistency.
        }
    }
}

export function createReshardingManager(
    coordination: ReshardingOptions["coordination"],
    options?: ReshardingOptions,
    manager?: ShardManager,
): ReshardingManager {
    return new ReshardingManager(coordination, options ?? {}, manager);
}
