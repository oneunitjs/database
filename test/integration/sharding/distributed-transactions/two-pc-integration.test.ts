// ---------------------------------------------------------------------------
// Stage 3 — Real PostgreSQL Two-Phase Commit Integration Tests
//
// Exercises the DistributedTransactionCoordinator against REAL PostgreSQL
// instances with prepared transactions enabled.
//
// Topology:
//   shard-0       : port 5440, database dtc_test   (participant)
//   shard-1       : port 5441, database dtc_test   (participant)
//   shard-2       : port 5442, database dtc_test   (participant)
//   coordination-db : port 5443, database coordination (durable decisions)
//
// Uses the public DistributedTransactionCoordinator API (not a simplified
// alternative coordinator) and asserts against real PostgreSQL state
// (committed/rolled-back rows, coordinator durable tables, pg_prepared_xacts).
//
// NOTE: tests run concurrently within this file (vitest default), so every
// test writes rows under a unique owner prefix and deletes only its own rows.
// Tables are created once (idempotent); no global DELETE/TRUNCATE runs per
// test, which would wipe concurrently-executing tests' durable rows.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "vitest";

import { DistributedTransactionCoordinator } from "../../../../src/distributed-tx/index.js";
import { Pool } from "pg";

const SHARDS = [
    { id: "shard-0", host: "127.0.0.1", port: 5440, user: "postgres", database: "dtc_test" },
    { id: "shard-1", host: "127.0.0.1", port: 5441, user: "postgres", database: "dtc_test" },
    { id: "shard-2", host: "127.0.0.1", port: 5442, user: "postgres", database: "dtc_test" },
];
const COORD = { host: "127.0.0.1", port: 5443, user: "postgres", database: "coordination" };

function shardUri(id: string) {
    const s = SHARDS.find((x) => x.id === id)!;
    return `postgresql://${s.user}@${s.host}:${s.port}/${s.database}`;
}

function coordUri() {
    return `postgresql://${COORD.user}@${COORD.host}:${COORD.port}/${COORD.database}`;
}

// Persistent helper pools (one per database), reused for the whole test file.
const shardPools = new Map<string, Pool>();
let coordPoolInst: Pool | undefined;

function shardPoolHelper(id: string): Pool {
    let p = shardPools.get(id);
    if (!p) {
        const s = SHARDS.find((x) => x.id === id)!;
        p = new Pool({ ...s, max: 5, statement_timeout: 8000 });
        shardPools.set(id, p);
    }
    return p;
}

function coordPoolHelper(): Pool {
    if (!coordPoolInst) {
        coordPoolInst = new Pool({ ...COORD, max: 5, statement_timeout: 8000 });
    }
    return coordPoolInst;
}

function shardPoolHelperFromUri(uri: string): Pool {
    const id = SHARDS.find((s) => shardUri(s.id) === uri)?.id;
    if (!id) throw new Error(`Unknown shard uri: ${uri}`);
    return shardPoolHelper(id);
}

async function execSql(uri: string, sql: string, params?: unknown[]) {
    return await (uri === coordUri() ? coordPoolHelper() : shardPoolHelperFromUri(uri)).query(sql, params ?? []);
}

async function countByOwner(uri: string, owner: string): Promise<number> {
    const r = await execSql(uri, "SELECT count(*) AS c FROM accounts WHERE owner = $1", [owner]);
    return Number(r.rows[0]?.c ?? 0);
}

async function preparedXacts(uri: string): Promise<string[]> {
    const r = await execSql(uri, "SELECT gid FROM pg_prepared_xacts ORDER BY gid");
    return r.rows.map((row) => (row as { gid: string }).gid);
}

async function closeAllHelperPools() {
    for (const p of shardPools.values()) {
        try { await p.end(); } catch { /* ignore */ }
    }
    shardPools.clear();
    try { await coordPoolInst?.end(); } catch { /* ignore */ }
    coordPoolInst = undefined;
}

let instancesReady = false;

async function ensureSchema() {
    // Idempotent table creation only — never wipes data, so concurrent tests
    // sharing these tables do not destroy each other's in-flight rows.
    for (const s of SHARDS) {
        await execSql(shardUri(s.id), `
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
    // Terminate all non-helper connections to dtc_test on each shard.
    for (const s of SHARDS) {
        try {
            await execSql(shardUri(s.id), `
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = $1 AND pid <> pg_backend_pid()
            `, [s.database]);
        } catch { /* ignore */ }
        // Roll back any lingering prepared transactions (abandoned by crashes).
        const gids = await preparedXacts(shardUri(s.id));
        for (const gid of gids) {
            try { await execSql(shardUri(s.id), `ROLLBACK PREPARED '${gid}'`); } catch { /* ignore */ }
        }
    }
    try {
        await execSql(coordUri(), `DELETE FROM distributed_transaction_participants`);
        await execSql(coordUri(), `DELETE FROM distributed_transactions`);
    } catch { /* ignore */ }
}

describe("Stage 3 — Real PostgreSQL 2PC integration (distributed transactions)", () => {
    beforeAll(async () => {
        for (const s of SHARDS) {
            const r = await execSql(shardUri(s.id), "SHOW max_prepared_transactions");
            const v = Number(Object.values(r.rows[0] ?? {})[0] ?? 0);
            if (v <= 0) throw new Error(`${s.id} max_prepared_transactions = ${v}`);
        }
        const cr = await execSql(coordUri(), "SHOW max_prepared_transactions");
        const cv = Number(Object.values(cr.rows[0] ?? {})[0] ?? 0);
        if (cv <= 0) throw new Error("coordination max_prepared_transactions <= 0");
        instancesReady = true;
        await ensureSchema();
    }, 60000);

    afterAll(async () => {
        if (instancesReady) await cleanupAll();
        await closeAllHelperPools();
    });

    interface Handle {
        coord: DistributedTransactionCoordinator;
        pools: Pool[];
    }

    async function newCoordinator(): Promise<Handle> {
        const cp = new Pool({ ...COORD, max: 5, statement_timeout: 8000 });
        const coord = new DistributedTransactionCoordinator({
            coordinationPool: cp,
            coordinatorId: `test-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
            maxPreparedTransactionAgeMs: 5000,
            logger: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        });
        await coord.initialize();
        const pools: Pool[] = [cp];
        for (const s of SHARDS) {
            const p = new Pool({ ...s, max: 5, statement_timeout: 8000 });
            pools.push(p);
            coord.registerParticipantPool(s.id, p);
        }
        return { coord, pools };
    }

    async function closeHandle(h: Handle) {
        try { await h.coord.shutdown(); } catch { /* ignore */ }
        for (const p of h.pools) {
            try { await p.end(); } catch { /* ignore */ }
        }
    }

    async function deleteByOwner(ownerPrefix: string) {
        for (const s of SHARDS) {
            try { await execSql(shardUri(s.id), `DELETE FROM accounts WHERE owner LIKE $1`, [`${ownerPrefix}%`]); } catch { /* ignore */ }
        }
    }

    it("two-participant commit: data committed on both participants", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "two-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-alice`, 100]);
            const c1 = await h.coord.addParticipant(txId, "shard-1", h.pools[2]);
            await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-bob`, 200]);

            await h.coord.prepare(txId);
            await h.coord.commit(txId);
            expect((await h.coord.getStatus(txId))!.status).toBe("committed");

            expect(await countByOwner(shardUri("shard-0"), `${owner}-alice`)).toBe(1);
            expect(await countByOwner(shardUri("shard-1"), `${owner}-bob`)).toBe(1);
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
        }
    });

    it("three-participant commit: data committed on all three participants", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "three-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-a`, 1]);
            const c1 = await h.coord.addParticipant(txId, "shard-1", h.pools[2]);
            await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-b`, 2]);
            const c2 = await h.coord.addParticipant(txId, "shard-2", h.pools[3]);
            await c2.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-c`, 3]);

            await h.coord.prepare(txId);
            await h.coord.commit(txId);
            expect((await h.coord.getStatus(txId))!.status).toBe("committed");

            expect(await countByOwner(shardUri("shard-0"), `${owner}-a`)).toBe(1);
            expect(await countByOwner(shardUri("shard-1"), `${owner}-b`)).toBe(1);
            expect(await countByOwner(shardUri("shard-2"), `${owner}-c`)).toBe(1);
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
        }
    });

    it("explicit rollback: no participant retains changes", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "rollback-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-x`, 10]);
            const c1 = await h.coord.addParticipant(txId, "shard-1", h.pools[2]);
            await c1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-y`, 20]);

            await h.coord.prepare(txId);
            await h.coord.abort(txId);

            expect(await countByOwner(shardUri("shard-0"), `${owner}-x`)).toBe(0);
            expect(await countByOwner(shardUri("shard-1"), `${owner}-y`)).toBe(0);
            // No lingering prepared transactions after abort.
            expect(await preparedXacts(shardUri("shard-0"))).toEqual([]);
            expect(await preparedXacts(shardUri("shard-1"))).toEqual([]);
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
        }
    });

    it("participant query failure before PREPARE triggers abort", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "fail-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-ok`, 1]);
            const c1 = await h.coord.addParticipant(txId, "shard-1", h.pools[2]);
            // Column count mismatch -> statement error, aborts the transaction.
            await expect(c1.query("INSERT INTO accounts (owner, balance) VALUES ($1)", [`${owner}-bad`]))
                .rejects.toThrow();

            await expect(h.coord.abort(txId)).resolves.toBeUndefined();
            expect(await countByOwner(shardUri("shard-0"), `${owner}-ok`)).toBe(0);
            expect(await countByOwner(shardUri("shard-1"), `${owner}-bad`)).toBe(0);
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
        }
    });

    it("concurrent distributed transactions commit independently", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const ha = await newCoordinator();
        const hb = await newCoordinator();
        try {
            const txA = await ha.coord.begin();
            const a0 = await ha.coord.addParticipant(txA, "shard-0", ha.pools[1]);
            await a0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", ["concurrent-txA-shard0", 1]);
            const a1 = await ha.coord.addParticipant(txA, "shard-1", ha.pools[2]);
            await a1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", ["concurrent-txA-shard1", 2]);

            const txB = await hb.coord.begin();
            const b0 = await hb.coord.addParticipant(txB, "shard-0", hb.pools[1]);
            await b0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", ["concurrent-txB-shard0", 3]);
            const b1 = await hb.coord.addParticipant(txB, "shard-2", hb.pools[3]);
            await b1.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", ["concurrent-txB-shard2", 4]);

            await ha.coord.prepare(txA);
            await hb.coord.prepare(txB);
            await ha.coord.commit(txA);
            await hb.coord.commit(txB);

            expect((await ha.coord.getStatus(txA))!.status).toBe("committed");
            expect((await hb.coord.getStatus(txB))!.status).toBe("committed");
            expect(await countByOwner(shardUri("shard-0"), "concurrent-txA-shard0")).toBe(1);
            expect(await countByOwner(shardUri("shard-1"), "concurrent-txA-shard1")).toBe(1);
            expect(await countByOwner(shardUri("shard-0"), "concurrent-txB-shard0")).toBe(1);
            expect(await countByOwner(shardUri("shard-2"), "concurrent-txB-shard2")).toBe(1);
        } finally {
            await closeHandle(ha);
            await closeHandle(hb);
            await deleteByOwner("concurrent-txA-shard0");
            await deleteByOwner("concurrent-txA-shard1");
            await deleteByOwner("concurrent-txB-shard0");
            await deleteByOwner("concurrent-txB-shard2");
        }
    });

    it("durable decision persisted in coordination tables", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "durable-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-durable`, 1]);
            await h.coord.prepare(txId);
            await h.coord.commit(txId);
            expect((await h.coord.getStatus(txId))!.status).toBe("committed");

            const txRows = await execSql(coordUri(),
                "SELECT status, global_decision FROM distributed_transactions WHERE transaction_id = $1", [txId]);
            expect(txRows.rows.length).toBe(1);
            expect(txRows.rows[0].status).toBe("committed");
            expect(txRows.rows[0].global_decision).toBe("COMMIT");

            const partRows = await execSql(coordUri(),
                "SELECT shard_id, participant_status FROM distributed_transaction_participants WHERE transaction_id = $1 ORDER BY shard_id", [txId]);
            expect(partRows.rows.length).toBe(1);
            expect(partRows.rows[0].participant_status).toBe("committed");
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
            // Remove this test's coordinator rows (shared table, concurrent-safe by tx id).
            try { await execSql(coordUri(), `DELETE FROM distributed_transaction_participants WHERE transaction_id LIKE $1`, [`durable-part%`]); } catch { /* ignore */ }
            try { await execSql(coordUri(), `DELETE FROM distributed_transactions WHERE transaction_id LIKE $1`, [`durable-part%`]); } catch { /* ignore */ }
        }
    });

    it("transaction state transitions active -> prepared -> committed", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "state-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            expect((await h.coord.getStatus(txId))!.status).toBe("active");

            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-s`, 1]);
            expect((await h.coord.getStatus(txId))!.status).toBe("active");

            await h.coord.prepare(txId);
            expect((await h.coord.getStatus(txId))!.status).toBe("prepared");

            await h.coord.commit(txId);
            expect((await h.coord.getStatus(txId))!.status).toBe("committed");
            expect(await countByOwner(shardUri("shard-0"), `${owner}-s`)).toBe(1);
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
        }
    });

    it("cannot abort a transaction with a durable COMMIT decision", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const owner = "norevert-part";
        const h = await newCoordinator();
        try {
            const txId = await h.coord.begin();
            const c0 = await h.coord.addParticipant(txId, "shard-0", h.pools[1]);
            await c0.query("INSERT INTO accounts (owner, balance) VALUES ($1, $2)", [`${owner}-n`, 1]);
            await h.coord.prepare(txId);
            await h.coord.commit(txId);

            await expect(h.coord.abort(txId)).rejects.toThrow(/COMMIT/);
            expect((await h.coord.getStatus(txId))!.status).toBe("committed");
            expect(await countByOwner(shardUri("shard-0"), `${owner}-n`)).toBe(1);
        } finally {
            await closeHandle(h);
            await deleteByOwner(owner);
        }
    });

    it("repeated recovery invocation is idempotent with no pending work", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const h = await newCoordinator();
        try {
            const r1 = await h.coord.recover();
            expect(r1.recovered).toBe(0);
            expect(r1.decisions).toEqual([]);
            const r2 = await h.coord.recover();
            expect(r2.recovered).toBe(r1.recovered);
            expect(r2.decisions).toEqual(r1.decisions);
        } finally {
            await closeHandle(h);
        }
    });
});
