import type { Pool, PoolClient } from "pg";
import type {
    DistributedTxStatus,
    ParticipantStatus,
    DistributedTxParticipantRecord,
    DistributedTxRecord,
    DistributedTxOptions,
} from "../failover/failover-types.js";
import { DatabaseError } from "../client/errors.js";
import { DISTRIBUTED_TX_SCHEMA } from "./coordinator-schema.js";
import { DistributedTxHookRunner, defaultDistributedTxHookLogger } from "./distributed-tx-hooks.js";
import type { DistributedTxHookContext } from "./distributed-tx-hooks.js";

export class DistributedTransactionError extends DatabaseError {
    constructor(message: string, public readonly transactionId?: string, public readonly cause?: Error) {
        super(message, { cause });
        this.name = "DistributedTransactionError";
        this.transient = false;
    }
}

export class DistributedTxTimeoutError extends DistributedTransactionError {
    constructor(
        message: string,
        public readonly transactionId?: string,
        public readonly cause?: Error,
        public readonly phase?: "prepare" | "commit" | "abort" | "global",
        public readonly timeoutMs?: number,
        public readonly shardId?: string,
    ) {
        super(message, transactionId, cause);
        this.name = "DistributedTxTimeoutError";
        this.transient = true;
    }
}

import { RetryPolicy } from "./retry-policy.js";

export interface DistributedTxTimeoutConfig {
    prepareTimeoutMs: number;
    commitTimeoutMs: number;
    abortTimeoutMs: number;
    globalTimeoutMs: number;
}

export const DEFAULT_DISTRIBUTED_TX_TIMEOUTS: Required<DistributedTxTimeoutConfig> = {
    prepareTimeoutMs: 30_000,
    commitTimeoutMs: 30_000,
    abortTimeoutMs: 30_000,
    globalTimeoutMs: 300_000,
};

export class DistributedTxNotFoundError extends DistributedTransactionError {
    constructor(transactionId: string) {
        super(`Distributed transaction not found: ${transactionId}`, transactionId);
        this.name = "DistributedTxNotFoundError";
    }
}

export class DistributedTxAlreadyResolvedError extends DistributedTransactionError {
    constructor(transactionId: string, status: DistributedTxStatus) {
        super(`Distributed transaction ${transactionId} already resolved with status: ${status}`, transactionId);
        this.name = "DistributedTxAlreadyResolvedError";
    }
}

export class MaxPreparedTransactionsError extends DistributedTransactionError {
    constructor() {
        super(
            "max_prepared_transactions is 0 — distributed transactions require PostgreSQL prepared transactions. " +
            "Set max_prepared_transactions > 0 in postgresql.conf and restart PostgreSQL.",
        );
        this.name = "MaxPreparedTransactionsError";
    }
}

export interface ParticipantContext {
    shardId: string;
    client: PoolClient;
    pool: Pool;
    preparedTransactionId?: string;
}

export type RequiredDistributedTxLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

export interface DistributedTxCoordinatorOptions {
    coordinationPool: Pool;
    coordinatorId?: string;
    maxPreparedTransactionAgeMs?: number;
    timeouts?: Partial<DistributedTxTimeoutConfig>;
    retryPolicy?: RetryPolicy;
    hookRunner?: DistributedTxHookRunner;
    logger: RequiredDistributedTxLogger;
}

export class DistributedTransactionCoordinator {
    private readonly coordinationPool: Pool;
    private readonly coordinatorId: string;
    private readonly maxPreparedTransactionAgeMs: number;
    private readonly timeouts: Required<DistributedTxTimeoutConfig>;
    private readonly retryPolicy: RetryPolicy;
    private readonly hookRunner: DistributedTxHookRunner;
    private readonly logger: RequiredDistributedTxLogger;
    private readonly participantPools = new Map<string, Pool>();
    private readonly activeTransactions = new Map<string, {
        status: DistributedTxStatus;
        participants: Map<string, ParticipantContext>;
        globalDecision: "COMMIT" | "ABORT" | undefined;
        startTime: number;
    }>();
    private recoveryTimer?: ReturnType<typeof setInterval>;
    private closed = false;
    private maxPreparedTransactionsChecked = false;

    constructor(options: DistributedTxCoordinatorOptions) {
        this.coordinationPool = options.coordinationPool;
        this.coordinatorId = options.coordinatorId ?? `coord-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
        this.maxPreparedTransactionAgeMs = options.maxPreparedTransactionAgeMs ?? 300000;
        this.timeouts = { ...DEFAULT_DISTRIBUTED_TX_TIMEOUTS, ...options.timeouts };
        this.retryPolicy = options.retryPolicy ?? new RetryPolicy();
        this.hookRunner = options.hookRunner ?? new DistributedTxHookRunner(defaultDistributedTxHookLogger);
        this.logger = options.logger ?? {
            debug: () => {},
            info: () => {},
            warn: () => {},
            error: () => {},
        };
    }

    private async withTimeout<T>(
        promise: () => Promise<T>,
        ms: number,
        phase: "prepare" | "commit" | "abort",
        transactionId?: string,
        shardId?: string,
    ): Promise<T> {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeoutReject = new Promise<never>((_, reject) => {
            timer = setTimeout(() => {
                const message =
                    phase +
                    " timed out after " +
                    ms +
                    "ms" +
                    (shardId ? " on shard " + shardId : "");
                reject(
                    new DistributedTxTimeoutError(
                        message,
                        transactionId,
                        undefined,
                        phase,
                        ms,
                        shardId,
                    ),
                );
            }, ms);
        });
        try {
            return await Promise.race([promise(), timeoutReject]);
        } finally {
            if (timer !== undefined) {
                clearTimeout(timer);
            }
        }
    }

    private assertGlobalTimeout(startTime: number, transactionId: string): void {
        const elapsed = Date.now() - startTime;
        if (elapsed >= this.timeouts.globalTimeoutMs) {
            throw new DistributedTxTimeoutError(
                "Global transaction timeout exceeded (" +
                    elapsed +
                    "ms >= " +
                    this.timeouts.globalTimeoutMs +
                    "ms)",
                transactionId,
                undefined,
                "global",
                this.timeouts.globalTimeoutMs,
            );
        }
    }

    /**
     * Register a participant pool for recovery use. When the coordinator
     * restarts after a crash, it needs pools to issue COMMIT PREPARED /
     * ROLLBACK PREPARED on behalf of prepared participants.
     */
    registerParticipantPool(shardId: string, pool: Pool): void {
        this.participantPools.set(shardId, pool);
    }

    /**
     * Initialize the coordinator: create schema tables, check PostgreSQL
     * prepared transaction support, and start the recovery worker.
     * Throws if max_prepared_transactions is 0.
     */
    async initialize(): Promise<void> {
        for (const stmt of DISTRIBUTED_TX_SCHEMA) {
            await this.coordinationPool.query(stmt);
        }

        const result = await this.coordinationPool.query("SHOW max_prepared_transactions");
        const maxPrepared = Number(result.rows[0]?.max_prepared_transactions ?? 0);
        this.maxPreparedTransactionsChecked = true;

        if (maxPrepared === 0) {
            throw new MaxPreparedTransactionsError();
        }

        this.logger.info(
            { coordinatorId: this.coordinatorId, maxPreparedTransactions: maxPrepared },
            "Distributed transaction coordinator initialized",
        );

        this.startRecoveryWorker();
    }

    private startRecoveryWorker(): void {
        const intervalMs = this.maxPreparedTransactionAgeMs;
        this.recoveryTimer = setInterval(() => {
            void this.recover().catch((error) => {
                this.logger.error(
                    { coordinatorId: this.coordinatorId, err: error },
                    "Recovery worker failed",
                );
            });
        }, intervalMs);
        if (this.recoveryTimer.unref) {
            this.recoveryTimer.unref();
        }
    }

    private generateTransactionId(): string {
        return `tx-${Date.now().toString(36)}-${this.coordinatorId}-${Math.random().toString(36).slice(2, 14)}`;
    }

    /**
     * Create a new distributed transaction record durably.
     * Participants are added dynamically via addParticipant().
     */
    async begin(options: DistributedTxOptions = {}): Promise<string> {
        const transactionId = this.generateTransactionId();
        const startTime = Date.now();

        await this.coordinationPool.query(
            `INSERT INTO distributed_transactions (transaction_id, status, created_at, updated_at, coordinator_id, participants)
             VALUES ($1, 'active', NOW(), NOW(), $2, $3)`,
            [transactionId, this.coordinatorId, []],
        );

        this.activeTransactions.set(transactionId, {
            status: "active",
            participants: new Map(),
            globalDecision: undefined,
            startTime,
        });

        this.logger.debug(
            { transactionId, coordinatorId: this.coordinatorId },
            "Distributed transaction begun",
        );

        return transactionId;
    }

    /**
     * Register a participant in the distributed transaction.
     * Called when the first operation targets a shard.
     * Begins a local transaction on the participant.
     */
    async addParticipant(
        transactionId: string,
        shardId: string,
        pool: Pool,
        options: DistributedTxOptions = {},
    ): Promise<PoolClient> {
        const txState = this.activeTransactions.get(transactionId);
        if (!txState) {
            throw new DistributedTxNotFoundError(transactionId);
        }
        if (txState.status !== "active") {
            throw new DistributedTxAlreadyResolvedError(transactionId, txState.status);
        }
        if (txState.participants.has(shardId)) {
            return txState.participants.get(shardId)!.client;
        }

        const client = await pool.connect();
        client.on("error", (error) => {
            this.logger.warn(
                { transactionId, shardId, err: error },
                "Participant client connection error",
            );
        });
        try {
            await client.query("BEGIN");
            if (options.isolation) {
                await client.query(`SET TRANSACTION ISOLATION LEVEL ${options.isolation}`);
            }

            await this.coordinationPool.query(
                `INSERT INTO distributed_transaction_participants (transaction_id, shard_id, participant_status)
                 VALUES ($1, $2, 'active')
                 ON CONFLICT (transaction_id, shard_id) DO NOTHING`,
                [transactionId, shardId],
            );

            const ctx: ParticipantContext = { shardId, client, pool };
            txState.participants.set(shardId, ctx);
            this.participantPools.set(shardId, pool);

            await this.coordinationPool.query(
                `UPDATE distributed_transactions SET participants = array_append(participants, $2), updated_at = NOW() WHERE transaction_id = $1 AND NOT participants @> ARRAY[$2]`,
                [transactionId, shardId],
            );

            this.logger.debug(
                { transactionId, shardId },
                "Participant added to distributed transaction",
            );

            return client;
        } catch (error) {
            try { await client.query("ROLLBACK").catch(() => {}); } catch {}
            try { await client.release(); } catch {}

            throw new DistributedTransactionError(
                `Failed to begin transaction on participant "${shardId}": ${error instanceof Error ? error.message : String(error)}`,
                transactionId,
                error instanceof Error ? error : undefined,
            );
        }
    }

    /**
     * Phase 1: Prepare all participants.
     * Issues PREPARE TRANSACTION on each participant and records the prepared
     * state durably.
     */
    async prepare(transactionId: string): Promise<void> {
        const txState = this.activeTransactions.get(transactionId);
        if (!txState) {
            const durable = await this.getDurableTransaction(transactionId);
            if (!durable) {
                throw new DistributedTxNotFoundError(transactionId);
            }
            if (durable.status !== "active" && durable.status !== "preparing") {
                throw new DistributedTxAlreadyResolvedError(transactionId, durable.status);
            }
        } else {
            if (txState.status !== "active") {
                throw new DistributedTxAlreadyResolvedError(transactionId, txState.status);
            }
        }

        this.assertGlobalTimeout(txState?.startTime ?? Date.now(), transactionId);

        const participants = txState?.participants ??
            new Map((await this.getDurableParticipants(transactionId)).map(
                (p) => [p.shardId, { shardId: p.shardId, client: null as any, pool: null as any, preparedTransactionId: p.preparedTransactionId }],
            ));

        await this.hookRunner.invoke("beforePrepare", {
            transactionId,
            coordinator: {
                getCurrentState: () => this.currentState,
                getCurrentTransactionId: () => transactionId,
                getParticipantCount: () => participants.size,
            },
            data: { participants: Array.from(participants.keys()) },
        });

        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = 'preparing', updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId],
        );

        const prepareErrors: Array<{ shardId: string; error: Error }> = [];

        for (const [shardId, ctx] of participants) {
            const client = ctx.client;
            if (!client) {
                prepareErrors.push({ shardId, error: new Error(`No client for participant ${shardId}`) });
                continue;
            }

            try {
                // Sanitize preparedId to prevent SQL injection in PREPARE TRANSACTION
                // (PREPARE TRANSACTION does not support parameterized identifiers)
                const safePreparedId = `pg_${transactionId}_${shardId}`.replace(/'/g, "''");

                await this.retryPolicy.execute(async () => {
                    await this.withTimeout(
                        () => client.query(`PREPARE TRANSACTION '${safePreparedId}'`),
                        this.timeouts.prepareTimeoutMs,
                        "prepare",
                        transactionId,
                        shardId,
                    );
                });

                await this.coordinationPool.query(
                    `INSERT INTO distributed_transaction_participants (transaction_id, shard_id, prepared_transaction_id, participant_status, prepared_at)
                     VALUES ($1, $2, $3, 'prepared', NOW())
                     ON CONFLICT (transaction_id, shard_id)
                     DO UPDATE SET prepared_transaction_id = $3, participant_status = 'prepared', prepared_at = NOW()`,
                    [transactionId, shardId, safePreparedId],
                );

                ctx.preparedTransactionId = safePreparedId;
                this.logger.debug(
                    { transactionId, shardId, preparedTransactionId: safePreparedId },
                    "Participant prepared",
                );
            } catch (error) {
                if (error instanceof DistributedTxTimeoutError) {
                    prepareErrors.push({
                        shardId,
                        error,
                    });
                    this.logger.error(
                        { transactionId, shardId, err: error },
                        "Participant prepare timed out",
                    );
                    continue;
                }
                prepareErrors.push({
                    shardId,
                    error: error instanceof Error ? error : new Error(String(error)),
                });
                this.logger.error(
                    { transactionId, shardId, err: error },
                    "Participant prepare failed",
                );
            }
        }

        if (prepareErrors.length > 0) {
            await this.coordinationPool.query(
                `UPDATE distributed_transactions SET status = 'aborting', updated_at = NOW() WHERE transaction_id = $1`,
                [transactionId],
            );

            for (const [shardId, ctx] of participants) {
                if (!ctx.preparedTransactionId) {
                    try {
                        await ctx.client?.query("ROLLBACK").catch(() => {});
                        await ctx.client?.release();
                    } catch {}
                }
            }

            for (const [shardId, ctx] of participants) {
                if (ctx.preparedTransactionId && ctx.client) {
                    try {
                        await ctx.client.query(`ROLLBACK PREPARED '${ctx.preparedTransactionId}'`);
                    } catch {}
                }
            }

            await this.coordinationPool.query(
                `UPDATE distributed_transactions SET status = 'aborted', global_decision = 'ABORT', decision_persisted_at = NOW(), updated_at = NOW() WHERE transaction_id = $1`,
                [transactionId],
            );

            const firstError = prepareErrors[0];
            await this.hookRunner.invoke("prepareFailed", {
                transactionId,
                coordinator: {
                    getCurrentState: () => "preparing",
                    getCurrentTransactionId: () => transactionId,
                    getParticipantCount: () => participants.size,
                },
                data: {
                    error: firstError.error,
                    preparedCount: participants.size - prepareErrors.length,
                    failedCount: prepareErrors.length,
                },
            });
            throw new DistributedTransactionError(
                `Prepare failed on ${prepareErrors.length} participant(s). First error on "${firstError.shardId}": ${firstError.error.message}`,
                transactionId,
                firstError.error,
            );
        }

        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = 'prepared', updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId],
        );

        if (txState) {
            txState.status = "prepared";
        }

        this.logger.info(
            { transactionId, participantCount: participants.size, coordinatorId: this.coordinatorId },
            "All participants prepared — ready for global commit decision",
        );

        const preparedIds = Array.from(participants.keys()).filter(
            (shardId) => txState?.participants.get(shardId)?.preparedTransactionId != null,
        );
        await this.hookRunner.invoke("afterPrepare", {
            transactionId,
            coordinator: {
                getCurrentState: () => "prepared",
                getCurrentTransactionId: () => transactionId,
                getParticipantCount: () => participants.size,
            },
            data: { preparedTransactionIds: preparedIds, durationMs: Date.now() - (txState?.startTime ?? 0) },
        });
    }

    /**
     * Phase 2: Commit all participants.
     * Persists the global COMMIT decision durably BEFORE issuing COMMIT PREPARED.
     */
    async commit(transactionId: string): Promise<void> {
        const txState = this.activeTransactions.get(transactionId);
        const durable = await this.getDurableTransaction(transactionId);

        if (!durable) {
            throw new DistributedTxNotFoundError(transactionId);
        }
        if (durable.status !== "prepared") {
            throw new DistributedTxAlreadyResolvedError(transactionId, durable.status);
        }
        if (durable.globalDecision) {
            throw new DistributedTxAlreadyResolvedError(transactionId, durable.status);
        }

        this.assertGlobalTimeout(
            txState?.startTime ?? new Date(durable.createdAt).getTime(),
            transactionId,
        );

        const participantRecords = await this.getDurableParticipants(transactionId);

        await this.hookRunner.invoke("beforeCommit", {
            transactionId,
            coordinator: {
                getCurrentState: () => "prepared",
                getCurrentTransactionId: () => transactionId,
                getParticipantCount: () => participantRecords.length,
            },
            data: { participantCount: participantRecords.length },
        });

        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = 'committing', global_decision = 'COMMIT', decision_persisted_at = NOW(), updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId],
        );

        if (txState) {
            txState.status = "committing";
            txState.globalDecision = "COMMIT";
        }

        const commitErrors: Array<{ shardId: string; error: Error }> = [];

        for (const record of participantRecords) {
            if (record.participantStatus !== "prepared") {
                continue;
            }

            if (!record.preparedTransactionId) {
                commitErrors.push({
                    shardId: record.shardId,
                    error: new Error(`No prepared transaction ID for participant ${record.shardId}`),
                });
                continue;
            }

            let client: PoolClient | null = null;
            let shouldRelease = false;
            const pool = this.participantPools.get(record.shardId)
                ?? txState?.participants.get(record.shardId)?.pool;

            try {
                if (pool) {
                    try {
                        client = await pool.connect();
                        shouldRelease = true;
                    } catch (error) {
                        commitErrors.push({
                            shardId: record.shardId,
                            error: error instanceof Error ? error : new Error(String(error)),
                        });
                        continue;
                    }
                } else if (txState?.participants.has(record.shardId)) {
                    client = txState.participants.get(record.shardId)!.client;
                }
                if (!client) {
                    commitErrors.push({
                        shardId: record.shardId,
                        error: new Error(`No connection available for participant ${record.shardId}`),
                    });
                    continue;
                }
                const clientRef = client;
                await this.retryPolicy.execute(async () => {
                    await this.withTimeout(
                        () => clientRef.query(`COMMIT PREPARED '${record.preparedTransactionId}'`),
                        this.timeouts.commitTimeoutMs,
                        "commit",
                        transactionId,
                        record.shardId,
                    );
                });

                await this.coordinationPool.query(
                    `UPDATE distributed_transaction_participants SET participant_status = 'committed', completed_at = NOW() WHERE transaction_id = $1 AND shard_id = $2`,
                    [transactionId, record.shardId],
                );

                this.logger.debug(
                    { transactionId, shardId: record.shardId },
                    "Participant committed",
                );
            } catch (error) {
                if (error instanceof DistributedTxTimeoutError) {
                    throw error;
                }
                commitErrors.push({
                    shardId: record.shardId,
                    error: error instanceof Error ? error : new Error(String(error)),
                });
                this.logger.error(
                    { transactionId, shardId: record.shardId, err: error },
                    "Participant commit failed",
                );
            } finally {
                if (shouldRelease && client) {
                    try { await client.release(); } catch {}
                }
            }
        }

        for (const [, ctx] of txState?.participants ?? []) {
            try { await ctx.client?.release(); } catch {}
        }
        txState?.participants.clear();

        const finalStatus = commitErrors.length > 0 ? "committing" : "committed";
        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = $2, updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId, finalStatus],
        );

        if (txState) {
            txState.status = finalStatus as DistributedTxStatus;
        }

        if (commitErrors.length > 0) {
            const firstError = commitErrors[0];
            await this.hookRunner.invoke("commitFailed", {
                transactionId,
                coordinator: {
                    getCurrentState: () => "committing",
                    getCurrentTransactionId: () => transactionId,
                    getParticipantCount: () => txState?.participants.size ?? 0,
                },
                data: {
                    error: firstError.error,
                    committedCount: participantRecords.length - commitErrors.length,
                    failedCount: commitErrors.length,
                },
            });
            throw new DistributedTransactionError(
                `Global COMMIT decision persisted but ${commitErrors.length} participant(s) failed to commit. First error on "${firstError.shardId}": ${firstError.error.message}. Recovery will retry.`,
                transactionId,
                firstError.error,
            );
        }

        this.logger.info(
            { transactionId, coordinatorId: this.coordinatorId },
            "Distributed transaction committed",
        );

        const committedCount = participantRecords.filter(
            (r) => r.participantStatus === "committed",
        ).length;
        await this.hookRunner.invoke("afterCommit", {
            transactionId,
            coordinator: {
                getCurrentState: () => "committed",
                getCurrentTransactionId: () => transactionId,
                getParticipantCount: () => participantRecords.length,
            },
            data: { committedCount, durationMs: Date.now() - (txState?.startTime ?? 0) },
        });
    }

    /**
     * Abort the distributed transaction.
     */
    async abort(transactionId: string): Promise<void> {
        const txState = this.activeTransactions.get(transactionId);
        const durable = await this.getDurableTransaction(transactionId);

        if (!durable) {
            throw new DistributedTxNotFoundError(transactionId);
        }
        if (durable.globalDecision === "COMMIT") {
            throw new DistributedTransactionError(
                `Cannot abort transaction ${transactionId} — global COMMIT decision is durable`,
                transactionId,
            );
        }
        if (durable.status === "aborted") {
            throw new DistributedTxAlreadyResolvedError(transactionId, durable.status);
        }

        this.assertGlobalTimeout(
            txState?.startTime ?? new Date(durable.createdAt).getTime(),
            transactionId,
        );

        const participantRecords = await this.getDurableParticipants(transactionId);

        await this.hookRunner.invoke("beforeAbort", {
            transactionId,
            coordinator: {
                getCurrentState: () => durable.status,
                getCurrentTransactionId: () => transactionId,
                getParticipantCount: () => participantRecords.length,
            },
            data: { participantCount: participantRecords.length },
        });

        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = 'aborting', global_decision = 'ABORT', decision_persisted_at = NOW(), updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId],
        );

        if (txState) {
            txState.status = "aborting";
            txState.globalDecision = "ABORT";
        }

        const abortErrors: Array<{ shardId: string; error: Error }> = [];

        for (const record of participantRecords) {
            if (record.participantStatus === "aborted") {
                continue;
            }

            let client: PoolClient | null = null;
            let shouldRelease = false;
            const pool = this.participantPools.get(record.shardId)
                ?? txState?.participants.get(record.shardId)?.pool;

            try {
                if (pool) {
                    try {
                        client = await pool.connect();
                        shouldRelease = true;
                    } catch (error) {
                        abortErrors.push({
                            shardId: record.shardId,
                            error: error instanceof Error ? error : new Error(String(error)),
                        });
                        continue;
                    }
                } else if (txState?.participants.has(record.shardId)) {
                    client = txState.participants.get(record.shardId)!.client;
                }
                if (!client) {
                    abortErrors.push({
                        shardId: record.shardId,
                        error: new Error(`No connection available for participant ${record.shardId}`),
                    });
                    continue;
                }
                const clientRef = client;
                if (record.preparedTransactionId) {
                    await this.retryPolicy.execute(async () => {
                        await this.withTimeout(
                            () => clientRef.query(`ROLLBACK PREPARED '${record.preparedTransactionId}'`),
                            this.timeouts.abortTimeoutMs,
                            "abort",
                            transactionId,
                            record.shardId,
                        );
                    });
                } else {
                    await this.retryPolicy.execute(async () => {
                        await this.withTimeout(
                            () => clientRef.query("ROLLBACK"),
                            this.timeouts.abortTimeoutMs,
                            "abort",
                            transactionId,
                            record.shardId,
                        );
                    });
                }

                await this.coordinationPool.query(
                    `UPDATE distributed_transaction_participants SET participant_status = 'aborted', completed_at = NOW() WHERE transaction_id = $1 AND shard_id = $2`,
                    [transactionId, record.shardId],
                );
            } catch (error) {
                if (error instanceof DistributedTxTimeoutError) {
                    throw error;
                }
                abortErrors.push({
                    shardId: record.shardId,
                    error: error instanceof Error ? error : new Error(String(error)),
                });
            } finally {
                if (shouldRelease && client) {
                    try { await client.release(); } catch {}
                }
            }
        }

        for (const [, ctx] of txState?.participants ?? []) {
            try { await ctx.client?.release(); } catch {}
        }
        txState?.participants.clear();

        const finalAbortStatus = abortErrors.length > 0 ? "aborting" : "aborted";
        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = $2, updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId, finalAbortStatus],
        );

        if (txState) {
            txState.status = finalAbortStatus as DistributedTxStatus;
        }

        if (abortErrors.length > 0) {
            const firstError = abortErrors[0];
            await this.hookRunner.invoke("abortFailed", {
                transactionId,
                coordinator: {
                    getCurrentState: () => "aborting",
                    getCurrentTransactionId: () => transactionId,
                    getParticipantCount: () => txState?.participants.size ?? 0,
                },
                data: {
                    error: firstError.error,
                    abortedCount: participantRecords.length - abortErrors.length,
                    failedCount: abortErrors.length,
                },
            });
            throw new DistributedTransactionError(
                `Global ABORT decision persisted but ${abortErrors.length} participant(s) failed to abort. First error on "${firstError.shardId}": ${firstError.error.message}. Recovery will retry.`,
                transactionId,
                firstError.error,
            );
        }

        this.logger.info(
            { transactionId, coordinatorId: this.coordinatorId },
            "Distributed transaction aborted",
        );

        const abortedCount = participantRecords.filter(
            (r) => r.participantStatus === "aborted",
        ).length;
        await this.hookRunner.invoke("afterAbort", {
            transactionId,
            coordinator: {
                getCurrentState: () => "aborted",
                getCurrentTransactionId: () => transactionId,
                getParticipantCount: () => participantRecords.length,
            },
            data: { abortedCount, durationMs: Date.now() - (txState?.startTime ?? 0) },
        });
    }

    /**
     * Recovery: scan for incomplete distributed transactions and resolve them.
     * Idempotent — safe to run concurrently.
     *
     * Also reconciles pg_prepared_xacts against durable coordinator metadata
     * to detect prepared transactions whose coordinator acknowledgement was
     * never recorded (crash between PREPARE and durable record).
     */
    async recover(): Promise<{
        recovered: number;
        decisions: Array<{ transactionId: string; decision: "COMMIT" | "ABORT" }>;
        unknownPrepared: Array<{ preparedTransactionId: string; shardId?: string }>;
    }> {
        const decisions: Array<{ transactionId: string; decision: "COMMIT" | "ABORT" }> = [];
        const unknownPrepared: Array<{ preparedTransactionId: string; shardId?: string }> = [];
        let recovered = 0;

        const incomplete = await this.coordinationPool.query(
            `SELECT transaction_id, status, global_decision
             FROM distributed_transactions
             WHERE status NOT IN ('committed', 'aborted')
               AND (updated_at < NOW() - ($1 || ' milliseconds')::interval
                    OR global_decision IS NOT NULL)
             ORDER BY created_at ASC`,
            [String(this.maxPreparedTransactionAgeMs)],
        );

        for (const row of incomplete.rows) {
            const transactionId = row.transaction_id as string;
            const globalDecision = row.global_decision as "COMMIT" | "ABORT" | null;

            try {
                if (globalDecision === "COMMIT") {
                    await this.recoverCommit(transactionId);
                    decisions.push({ transactionId, decision: "COMMIT" });
                } else if (globalDecision === "ABORT") {
                    await this.recoverAbort(transactionId);
                    decisions.push({ transactionId, decision: "ABORT" });
                } else {
                    this.logger.warn(
                        { transactionId, status: row.status },
                        "Transaction has no global decision — waiting for coordinator",
                    );
                }
                recovered++;
            } catch (error) {
                this.logger.error(
                    { transactionId, err: error },
                    "Recovery failed for transaction",
                );
            }
        }

        // Reconcile pg_prepared_xacts against durable coordinator metadata.
        // Prepared transactions found here but not in the coordinator's
        // durable records indicate a crash between PREPARE and durable record.
        // These must NEVER be automatically committed — they require operator
        // intervention or coordinator-driven recovery.
        const poolsToScan: Array<{ shardId?: string; pool: Pool }> = [
            { pool: this.coordinationPool },
        ];
        for (const [shardId, pool] of this.participantPools) {
            poolsToScan.push({ shardId, pool });
        }
        const seenGids = new Set<string>();
        for (const { shardId, pool } of poolsToScan) {
            try {
                const preparedXacts = await pool.query(
                    `SELECT transaction, gid, owner, database FROM pg_prepared_xacts`,
                );
                for (const row of preparedXacts.rows) {
                    const preparedId = row.gid as string;
                    if (seenGids.has(preparedId)) continue;
                    seenGids.add(preparedId);
                    const tracked = await this.coordinationPool.query(
                        `SELECT transaction_id, shard_id, participant_status
                         FROM distributed_transaction_participants
                         WHERE prepared_transaction_id = $1`,
                        [preparedId],
                    );
                    if (tracked.rows.length === 0) {
                        unknownPrepared.push({
                            preparedTransactionId: preparedId,
                            shardId,
                        });
                        this.logger.warn(
                            { preparedTransactionId: preparedId, database: row.database, shardId },
                            "Unknown prepared transaction found in pg_prepared_xacts — not managed by coordinator. Manual intervention required.",
                        );
                    } else {
                        for (const rec of tracked.rows) {
                            if (rec.participant_status === "prepared") {
                                this.logger.warn(
                                    { transactionId: rec.transaction_id, shardId: rec.shard_id, preparedTransactionId: preparedId },
                                    "Prepared transaction found in pg_prepared_xacts — recovery will reconcile",
                                );
                            }
                        }
                    }
                }
            } catch (error) {
                this.logger.warn(
                    { err: error, shardId },
                    "Failed to query pg_prepared_xacts — prepared transaction reconciliation skipped for this pool",
                );
            }
        }

        return { recovered, decisions, unknownPrepared };
    }

    private async recoverCommit(transactionId: string): Promise<void> {
        const participantRecords = await this.getDurableParticipants(transactionId);
        let pending = 0;

        for (const record of participantRecords) {
            if (record.participantStatus === "committed") continue;
            if (!record.preparedTransactionId) continue;

            try {
                const pool = this.participantPools.get(record.shardId);
                if (!pool) {
                    pending++;
                    this.logger.warn(
                        { transactionId, shardId: record.shardId },
                        "Recovery: no participant pool registered — will retry",
                    );
                    continue;
                }
                const client = await pool.connect();
                try {
                await this.retryPolicy.execute(async () => {
                    await this.withTimeout(
                        () => client.query(`COMMIT PREPARED '${record.preparedTransactionId}'`),
                        this.timeouts.commitTimeoutMs,
                        "commit",
                        transactionId,
                        record.shardId,
                    );
                });
                } finally {
                    client.release();
                }

                await this.coordinationPool.query(
                    `UPDATE distributed_transaction_participants SET participant_status = 'committed', completed_at = NOW() WHERE transaction_id = $1 AND shard_id = $2`,
                    [transactionId, record.shardId],
                );
            } catch (error: any) {
                // If the prepared transaction no longer exists, it was already
                // committed or rolled back by another recovery attempt or process.
                // Treat as success — idempotent recovery.
                if (error.message && error.message.includes("does not exist")) {
                    await this.coordinationPool.query(
                        `UPDATE distributed_transaction_participants SET participant_status = 'committed', completed_at = NOW() WHERE transaction_id = $1 AND shard_id = $2`,
                        [transactionId, record.shardId],
                    );
                    continue;
                }
                pending++;
                this.logger.warn(
                    { transactionId, shardId: record.shardId, err: error },
                    "Recovery: failed to commit prepared participant — will retry",
                );
            }
        }

        if (pending > 0) {
            await this.coordinationPool.query(
                `UPDATE distributed_transactions SET status = 'committing', updated_at = NOW() WHERE transaction_id = $1`,
                [transactionId],
            );
            return;
        }

        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = 'committed', updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId],
        );
    }

    private async recoverAbort(transactionId: string): Promise<void> {
        const participantRecords = await this.getDurableParticipants(transactionId);
        let pending = 0;

        for (const record of participantRecords) {
            if (record.participantStatus === "aborted") continue;
            if (!record.preparedTransactionId) continue;

            try {
                const pool = this.participantPools.get(record.shardId);
                if (!pool) {
                    pending++;
                    this.logger.warn(
                        { transactionId, shardId: record.shardId },
                        "Recovery: no participant pool registered — will retry",
                    );
                    continue;
                }
                const client = await pool.connect();
                try {
                    await client.query(`ROLLBACK PREPARED '${record.preparedTransactionId}'`);
                } finally {
                    client.release();
                }

                await this.coordinationPool.query(
                    `UPDATE distributed_transaction_participants SET participant_status = 'aborted', completed_at = NOW() WHERE transaction_id = $1 AND shard_id = $2`,
                    [transactionId, record.shardId],
                );
            } catch (error: any) {
                if (error.message && error.message.includes("does not exist")) {
                    await this.coordinationPool.query(
                        `UPDATE distributed_transaction_participants SET participant_status = 'aborted', completed_at = NOW() WHERE transaction_id = $1 AND shard_id = $2`,
                        [transactionId, record.shardId],
                    );
                    continue;
                }
                pending++;
                this.logger.warn(
                    { transactionId, shardId: record.shardId, err: error },
                    "Recovery: failed to abort prepared participant — will retry",
                );
            }
        }

        if (pending > 0) {
            await this.coordinationPool.query(
                `UPDATE distributed_transactions SET status = 'aborting', updated_at = NOW() WHERE transaction_id = $1`,
                [transactionId],
            );
            return;
        }

        await this.coordinationPool.query(
            `UPDATE distributed_transactions SET status = 'aborted', updated_at = NOW() WHERE transaction_id = $1`,
            [transactionId],
        );
    }

    private async getDurableTransaction(transactionId: string): Promise<DistributedTxRecord | null> {
        const result = await this.coordinationPool.query(
            `SELECT * FROM distributed_transactions WHERE transaction_id = $1`,
            [transactionId],
        );
        if (result.rows.length === 0) return null;
        const row = result.rows[0];
        return {
            transactionId: row.transaction_id,
            status: row.status as DistributedTxStatus,
            createdAt: row.created_at,
            updatedAt: row.updated_at,
            coordinatorId: row.coordinator_id,
            participants: row.participants ?? [],
            globalDecision: row.global_decision as "COMMIT" | "ABORT" | undefined,
            decisionPersistedAt: row.decision_persisted_at,
            recoveryMetadata: row.recovery_metadata,
        };
    }

    private async getDurableParticipants(transactionId: string): Promise<DistributedTxParticipantRecord[]> {
        const result = await this.coordinationPool.query(
            `SELECT * FROM distributed_transaction_participants WHERE transaction_id = $1 ORDER BY shard_id`,
            [transactionId],
        );
        return result.rows.map((row) => ({
            transactionId: row.transaction_id,
            shardId: row.shard_id,
            preparedTransactionId: row.prepared_transaction_id,
            participantStatus: row.participant_status as ParticipantStatus,
            preparedAt: row.prepared_at,
            completedAt: row.completed_at,
            errorDetails: row.error_details,
        }));
    }

    async getStatus(transactionId: string): Promise<DistributedTxRecord | null> {
        return this.getDurableTransaction(transactionId);
    }

    async getMetrics(): Promise<{
        active: number;
        preparing: number;
        prepared: number;
        committing: number;
        committed: number;
        aborting: number;
        aborted: number;
        oldestActiveAgeMs: number;
    }> {
        const result = await this.coordinationPool.query(
            `SELECT status, COUNT(*) as cnt, MIN(created_at) as oldest
             FROM distributed_transactions
             GROUP BY status`,
        );

        const metrics: Record<string, { count: number; oldest?: Date }> = {};
        for (const row of result.rows) {
            metrics[row.status] = {
                count: Number(row.cnt),
                oldest: row.oldest ? new Date(row.oldest) : undefined,
            };
        }

        const now = Date.now();
        let oldestActiveAgeMs = 0;
        if (metrics.active?.oldest) {
            oldestActiveAgeMs = now - metrics.active.oldest.getTime();
        }

        return {
            active: metrics.active?.count ?? 0,
            preparing: metrics.preparing?.count ?? 0,
            prepared: metrics.prepared?.count ?? 0,
            committing: metrics.committing?.count ?? 0,
            committed: metrics.committed?.count ?? 0,
            aborting: metrics.aborting?.count ?? 0,
            aborted: metrics.aborted?.count ?? 0,
            oldestActiveAgeMs,
        };
    }

    get currentState(): string {
        for (const [, txState] of this.activeTransactions) {
            return txState.status;
        }
        return "active";
    }

    async shutdown(): Promise<void> {
        this.closed = true;
        if (this.recoveryTimer) {
            clearInterval(this.recoveryTimer);
            this.recoveryTimer = undefined;
        }
    }
}
