// ---------------------------------------------------------------------------
// Stage 3 — Real PostgreSQL 2PC Crash Recovery Tests
//
// Exercises the DistributedTransactionCoordinator's crash-recovery behavior
// against REAL PostgreSQL instances with prepared transactions enabled.
//
// Scenarios (per Stage 3 / Section 5.2 crash-injection matrix):
//   Case C: all participants prepared, no global decision — uncertain
//           transaction is never silently committed.
//   Case D: durable COMMIT persisted, participant down during commit —
//           recovery must commit every participant once reachable.
//   Participant failure after PREPARE with restart — prepared transaction
//           survives restart; recovery resolves it.
//   Unknown prepared transaction — never auto-committed by recovery.
//   Multiple recovery workers idempotency — concurrent recover() calls do not
//           produce contradictory decisions.
//
// Fault injection: pg_ctl stop/start of real PostgreSQL instances.
//
// Notes on harness:
//   * All scenarios run inside ONE test so they execute sequentially (vitest
//     lacks describe.sequential), required because they manipulate shared
//     instance up/down state.
//   * Prepared transactions hold row locks on modified rows, so SELECT count(*)
//     against those rows blocks until the prepared transaction resolves.
//     State is therefore verified through pg_prepared_xacts and the durable
//     coordinator decision rather than row counts while a transaction is
//     prepared.
//   * Fresh pg pools are used per query; pool teardown is bounded so a slow
//     client release cannot hang a test hook.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { DistributedTransactionCoordinator } from "../../../../src/distributed-tx/index.js";
import { Pool } from "pg";
import {
    stopPostgres,
    startPostgres,
    waitForReady,
    assertReady,
    ensureAllUp,
    PG_INSTANCES,
    instanceUri,
} from "../helpers/fault-injection.js";

const SHARD_IDS = ["shard-0", "shard-1", "shard-2"] as const;
const COORD_ID = "coordination-db";

function shardUri(id: string) {
    return instanceUri(PG_INSTANCES.find((x) => x.id === id)!);
}

function coordUri() {
    return instanceUri(PG_INSTANCES.find((x) => x.id === COORD_ID)!);
}

// Fresh helper pool per query (create/end). Teardown is bounded so a slow
// client release cannot hang a test hook.
async function execSql(uri: string, sql: string, params?: unknown[]) {
    const inst = uri === coordUri()
        ? PG_INSTANCES.find((x) => x.id === COORD_ID)!
        : PG_INSTANCES.find((x) => instanceUri(x) === uri)!;
    const pool = new Pool({ ...inst, max: 1, statement_timeout: 8000, connectionTimeoutMillis: 3000 });
    try {
        return await pool.query(sql, params ?? []);
    } finally {
        await Promise.race([
            pool.end(),
            new Promise<void>((resolve) => setTimeout(resolve, 5000)),
        ]).catch(() => {});
    }
}

async function countByOwner(uri: string, owner: string): Promise<number> {
    const r = await execSql(uri, "SELECT count(*) AS c FROM accounts WHERE owner = $1", [owner]);
    return Number(r.rows[0]?.c ?? 0);
}

// Participant status as recorded durably in the coordination database.
// Preferred over pg_prepared_xacts on the shards because the latter can block
// on row locks held by a prepared transaction under test.
async function participantStatus(txId: string, shardId: string): Promise<string> {
    const r = await execSql(coordUri(), "SELECT participant_status FROM distributed_transaction_participants WHERE transaction_id = $1 AND shard_id = $2", [txId, shardId]);
    return r.rows[0]?.participant_status ?? "unknown";
}

async function preparedParticipantCount(txId: string): Promise<number> {
    const r = await execSql(coordUri(), "SELECT count(*) AS c FROM distributed_transaction_participants WHERE transaction_id = $1 AND participant_status = 'prepared'", [txId]);
    return Number(r.rows[0]?.c ?? 0);
}

async function committedParticipantCount(txId: string): Promise<number> {
    const r = await execSql(coordUri(), "SELECT count(*) AS c FROM distributed_transaction_participants WHERE transaction_id = $1 AND participant_status = 'committed'", [txId]);
    return Number(r.rows[0]?.c ?? 0);
}

async function preparedXacts(uri: string): Promise<string[]> {
    const r = await execSql(uri, "SELECT gid FROM pg_prepared_xacts ORDER BY gid");
    return r.rows.map((row) => (row as { gid: string }).gid);
}

let instancesReady = false;

async function ensureSchema() {
    for (const id of SHARD_IDS) {
        await execSql(shardUri(id), `
            CREATE TABLE IF NOT EXISTS accounts (
                id BIGSERIAL PRIMARY KEY,
                owner TEXT NOT NULL,
                balance BIGINT NOT NULL DEFAULT 0
            )
        `);
    }
    try {
        await execSql(coordUri(), `
            CREATE TABLE IF NOT EXISTS distributed_transactions (
                transaction_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                coordinator_id TEXT NOT NULL,
                participants TEXT[] NOT NULL DEFAULT '{}',
                global_decision TEXT,
                decision_persisted_at TIMESTAMPTZ,
                recovery_metadata JSONB NOT NULL DEFAULT '{}'
            )
        `);
        await execSql(coordUri(), `
            CREATE TABLE IF NOT EXISTS distributed_transaction_participants (
                transaction_id TEXT NOT NULL,
                shard_id TEXT NOT NULL,
                prepared_transaction_id TEXT,
                participant_status TEXT NOT NULL DEFAULT 'active',
                prepared_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                error_details TEXT,
                PRIMARY KEY (transaction_id, shard_id),
                CONSTRAINT fk_distributed_tx_participants_tx
                    FOREIGN KEY (transaction_id) REFERENCES distributed_transactions(transaction_id)
            )
        `);
    } catch { /* tables may already exist */ }
}

async function cleanupAll() {
    for (const id of SHARD_IDS) {
        try { await execSql(shardUri(id), `DELETE FROM accounts`); } catch { /* ignore */ }
        const gids = await preparedXacts(shardUri(id));
        for (const gid of gids) {
            try { await execSql(shardUri(id), `ROLLBACK PREPARED '${gid}'`); } catch { /* ignore */ }
        }
    }
    try {
        await execSql(coordUri(), `DELETE FROM distributed_transaction_participants`);
        await execSql(coordUri(), `DELETE FROM distributed_transactions`);
    } catch { /* ignore */ }
}

interface Handle {
    coord: DistributedTransactionCoordinator;
    pools: Pool[];
}

async function newCoordinator(): Promise<Handle> {
    const inst = PG_INSTANCES.find((x) => x.id === COORD_ID)!;
    const cp = new Pool({ ...inst, max: 5, statement_timeout: 8000, connectionTimeoutMillis: 3000 });
    cp.on("error", () => {});
    const coord = new DistributedTransactionCoordinator({
        coordinationPool: cp,
        coordinatorId: `crash-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
        maxPreparedTransactionAgeMs: 5000,
        logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
    });
    await coord.initialize();
    const pools: Pool[] = [cp];
    for (const id of SHARD_IDS) {
        const inst = PG_INSTANCES.find((x) => x.id === id)!;
        const p = new Pool({ ...inst, max: 5, statement_timeout: 8000, connectionTimeoutMillis: 3000 });
        p.on("error", () => {});
        pools.push(p);
        coord.registerParticipantPool(id, p);
    }
    return { coord, pools };
}

async function closeHandle(h: Handle) {
    try { await h.coord.shutdown(); } catch { /* ignore */ }
    await Promise.all(h.pools.map((p) => Promise.race([
        p.end().catch(() => {}),
        new Promise<void>((resolve) => setTimeout(resolve, 5000)),
    ])));
}

async function deleteOwner(owner: string) {
    for (const id of SHARD_IDS) {
        try { await execSql(shardUri(id), `DELETE FROM accounts WHERE owner = $1`, [owner]); } catch { /* ignore */ }
    }
}

describe("Stage 3 — Real PostgreSQL 2PC crash recovery", () => {
    beforeAll(async () => {
        for (const id of SHARD_IDS) {
            await assertReady(id);
            const r = await execSql(shardUri(id), "SHOW max_prepared_transactions");
            const v = Number(Object.values(r.rows[0] ?? {})[0] ?? 0);
            if (v <= 0) throw new Error(`${id} max_prepared_transactions = ${v}`);
        }
        await assertReady(COORD_ID);
        instancesReady = true;
        await cleanupAll();
        await ensureSchema();
    }, 60000);

    afterAll(async () => {
        await ensureAllUp();
        if (instancesReady) await cleanupAll();
    }, 60000);

    it("crash recovery: Case C / Case D / participant failure / unknown prepared / multiple workers", async () => {
        if (!instancesReady) return;

        // ------------------------------------------------------------------
        // Case C: all prepared, no global decision — uncertain never committed
        // ------------------------------------------------------------------
        {
            const owner = "caseC";
            const h = await newCoordinator();
            try {
                const txId = await h.coord.begin();
                const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
                await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-a`, 1]);
                const c1 = await h.coord.addParticipant(txId, "shard-1", h.pools[2]);
                await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-b`, 2]);

                await h.coord.prepare(txId);
                // Both participants durably recorded as prepared.
                expect(await participantStatus(txId, "shard-0")).toBe("prepared");
                expect(await participantStatus(txId, "shard-1")).toBe("prepared");

                const before = await h.coord.recover();
                // No global decision: recovery must not commit or abort.
                expect(before.decisions).toEqual([]);
                // Both participants still recorded as prepared (not resolved).
                expect(await participantStatus(txId, "shard-0")).toBe("prepared");
                expect(await participantStatus(txId, "shard-1")).toBe("prepared");

                // Repeated recovery still does not resolve.
                const again = await h.coord.recover();
                expect(again.decisions).toEqual([]);

                // Clean up via abort (coordinator-driven, has full state).
                await h.coord.abort(txId);
                // Both participants recorded as aborted; no prepared participants remain.
                expect(await participantStatus(txId, "shard-0")).toBe("aborted");
                expect(await participantStatus(txId, "shard-1")).toBe("aborted");
                expect(await preparedParticipantCount(txId)).toBe(0);
            } finally {
                await closeHandle(h);
                await deleteOwner(owner);
            }
        }

        // ------------------------------------------------------------------
        // Case D: durable COMMIT, participant down during commit — recovery
        // commits every participant once reachable.
        // ------------------------------------------------------------------
        {
            const owner = "caseD";
            const h1 = await newCoordinator();
            try {
                const txId = await h1.coord.begin();
                const c0 = await h1.coord.addParticipant(txId, "shard-0", h1.pools[1]);
                await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-a`, 1]);
                const c1 = await h1.coord.addParticipant(txId, "shard-1", h1.pools[2]);
                await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-b`, 2]);

                await h1.coord.prepare(txId); // both prepared

                // Participant shard-1 goes down AFTER prepare.
                await stopPostgres("shard-1");

                // commit() persists the COMMIT decision, then fails on shard-1.
                await expect(h1.coord.commit(txId)).rejects.toThrow();
                const durable = await h1.coord.getStatus(txId);
                expect(durable!.globalDecision).toBe("COMMIT");

                // shard-0 committed; shard-1's prepared transaction survives and
                // holds the uncommitted changes (row-locked, so do not count it).
                expect(await countByOwner(shardUri("shard-0"), `${owner}-a`)).toBe(1);
                expect(await participantStatus(txId, "shard-1")).toBe("prepared");

                // Coordinator "crashes": drop h1, create h2 with no in-memory state.
                // shard-1 is still down.
                await closeHandle(h1);

                // New coordinator recovers from durable state; shard-1 is still
                // down, so recoverCommit cannot commit it yet.
                const h2 = await newCoordinator();
                try {
                    const rec = await h2.coord.recover();
                    expect(rec.decisions).toEqual([{ transactionId: txId, decision: "COMMIT" }]);
                    // shard-1 still prepared (down), not committed yet.
                    expect(await participantStatus(txId, "shard-1")).toBe("prepared");

                    // Restart the participant -> now reachable -> recover commits it.
                    await startPostgres("shard-1");
                    await waitForReady("shard-1");

                    const rec2 = await h2.coord.recover();
                    expect(rec2.decisions).toEqual([{ transactionId: txId, decision: "COMMIT" }]);
                    expect(await countByOwner(shardUri("shard-1"), `${owner}-b`)).toBe(1);
                    expect(await countByOwner(shardUri("shard-0"), `${owner}-a`)).toBe(1);
                    expect(await preparedXacts(shardUri("shard-0"))).toEqual([]);
                    expect(await preparedXacts(shardUri("shard-1"))).toEqual([]);
                } finally {
                    await closeHandle(h2);
                }
            } finally {
                await closeHandle(h1).catch(() => {});
                await deleteOwner(owner);
            }
        }

        // ------------------------------------------------------------------
        // Participant failure after PREPARE — restart then recovery resolves
        // ------------------------------------------------------------------
        {
            const owner = "partfail";
            const h = await newCoordinator();
            try {
                const txId = await h.coord.begin();
                const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
                await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-a`, 1]);
                const c1 = await h.coord.addParticipant(txId, "shard-1", h.pools[2]);
                await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-b`, 2]);

                await h.coord.prepare(txId);
                await stopPostgres("shard-1"); // prepared transaction survives restart
                await startPostgres("shard-1");
                await waitForReady("shard-1");

                // commit() with shard-1 now reachable: both commit.
                await h.coord.commit(txId);
                expect(await countByOwner(shardUri("shard-0"), `${owner}-a`)).toBe(1);
                expect(await countByOwner(shardUri("shard-1"), `${owner}-b`)).toBe(1);
                expect(await preparedXacts(shardUri("shard-0"))).toEqual([]);
                expect(await preparedXacts(shardUri("shard-1"))).toEqual([]);
            } finally {
                await closeHandle(h);
                await deleteOwner(owner);
            }
        }

        // ------------------------------------------------------------------
        // Unknown prepared transaction — recovery never auto-commits it.
        // ------------------------------------------------------------------
        {
            const gid = "unknown-prepared-gid-test";
            {
                const inst0 = PG_INSTANCES.find((x) => x.id === "shard-0")!;
                const p = new Pool({ ...inst0, max: 1, statement_timeout: 8000, connectionTimeoutMillis: 3000 });
                const client = await p.connect();
                try {
                    await client.query("BEGIN");
                    await client.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", ["unknown-prepared", 1]);
                    await client.query(`PREPARE TRANSACTION '${gid}'`);
                } finally {
                    try { client.release(); } catch { /* ignore */ }
                    await Promise.race([
                        p.end(),
                        new Promise<void>((resolve) => setTimeout(resolve, 5000)),
                    ]).catch(() => {});
                }
            }

            const h = await newCoordinator();
            try {
                const rec = await h.coord.recover();
                expect(rec.decisions).toEqual([]);
                expect(rec.unknownPrepared.map((x) => x.preparedTransactionId)).toContain(gid);
                // Recovery must NOT commit it.
                expect(await preparedXacts(shardUri("shard-0"))).toContain(gid);
            } finally {
                await closeHandle(h);
                // Operator-driven cleanup: roll back the unknown prepared xact.
                try { await execSql(shardUri("shard-0"), `ROLLBACK PREPARED '${gid}'`); } catch { /* ignore */ }
            }
        }

        // ------------------------------------------------------------------
        // Multiple recovery workers — concurrent recover() calls are idempotent
        // and do not produce contradictory decisions.
        // ------------------------------------------------------------------
        {
            const owner = "multiwork";
            const h1 = await newCoordinator();
            try {
                const txId = await h1.coord.begin();
                const c0 = await h1.coord.addParticipant(txId, "shard-0", h1.pools[1]);
                await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-a`, 1]);
                const c1 = await h1.coord.addParticipant(txId, "shard-1", h1.pools[2]);
                await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-b`, 2]);
                await h1.coord.prepare(txId);

                await stopPostgres("shard-1");
                await expect(h1.coord.commit(txId)).rejects.toThrow(); // COMMIT durable, shard-1 fails
                await closeHandle(h1);
                await startPostgres("shard-1");
                await waitForReady("shard-1");

                const h2 = await newCoordinator();
                const h3 = await newCoordinator();
                try {
                    // Two recovery workers run concurrently.
                    const [r2, r3] = await Promise.all([h2.coord.recover(), h3.coord.recover()]);
                    // Both observe the durable COMMIT decision; neither returns ABORT.
                    expect(r2.decisions.map((d) => d.decision)).toEqual(["COMMIT"]);
                    expect(r3.decisions.map((d) => d.decision)).toEqual(["COMMIT"]);

                    // One committed shard-1; the other sees "does not exist" and
                    // treats it as success (idempotent).
                    expect(await countByOwner(shardUri("shard-0"), `${owner}-a`)).toBe(1);
                    expect(await countByOwner(shardUri("shard-1"), `${owner}-b`)).toBe(1);
                    expect(await preparedXacts(shardUri("shard-0"))).toEqual([]);
                    expect(await preparedXacts(shardUri("shard-1"))).toEqual([]);

                    // Repeated recovery is idempotent (no pending work).
                    const r2b = await h2.coord.recover();
                    const r3b = await h3.coord.recover();
                    expect(r2b.decisions).toEqual([]);
                    expect(r3b.decisions).toEqual([]);
                } finally {
                    await closeHandle(h2);
                    await closeHandle(h3);
                }
            } finally {
                await closeHandle(h1).catch(() => {});
                await deleteOwner(owner);
            }
        }
    }, 180000);
});
