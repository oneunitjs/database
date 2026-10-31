import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { createDatabase } from "../../src/index.js";
import { Pool } from "pg";

const PG_USER = process.env.PG_USER || "postgres";
const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = process.env.PG_PORT || 5432;
const PG_PASSWORD = process.env.PG_PASSWORD || "";
function pgUri(dbname) { const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : ""; return `postgresql://${PG_USER}${auth}@${PG_HOST}:${PG_PORT}/${dbname}`; }
let hasPostgres = false;
let useSocket = false;
function pgSocketUri(dbname) { const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : ""; return `postgresql://${PG_USER}${auth}@/${dbname}?host=/run/postgresql`; }
function dbUri(dbname) { return useSocket ? pgSocketUri(dbname) : pgUri(dbname); }
function accountsDDL() { return `CREATE TABLE IF NOT EXISTS accounts (id BIGSERIAL PRIMARY KEY, owner TEXT NOT NULL, balance BIGINT NOT NULL DEFAULT 0, shard_key TEXT NOT NULL)`; }
function makeDescriptor(migrationId) {
    return { migrationId, tables: [{ table: "accounts", primaryKeyColumns: ["id"], shardKeyColumns: ["shard_key"], stableKeyOrder: true, description: "accounts", readBatch: async ({ client, batchSize, cursor }) => { const lastId = cursor && cursor.lastValues ? cursor.lastValues[0] : 0; const result = await client.query(`SELECT id, owner, balance, shard_key FROM accounts WHERE id > $1 ORDER BY id ASC LIMIT $2`, [lastId, batchSize]); const rows = result.rows; const nextCursor = rows.length === 0 ? null : { kind: "keyset", columns: ["id"], lastValues: [rows[rows.length - 1].id], batchNo: (cursor && cursor.batchNo ? cursor.batchNo : 0) + 1, updatedAt: new Date().toISOString() }; return { rows, nextCursor, done: rows.length < batchSize }; }, upsertRows: async ({ client, rows }) => { for (const row of rows) await client.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key`, [row.id, row.owner, row.balance, row.shard_key]); return { inserted: rows.length, updated: 0 }; }, countRows: async ({ client }) => { const res = await client.query(`SELECT COUNT(*) AS c FROM accounts`); return Number(res.rows[0].c); } }], order: ["accounts"], consistencyBoundary: "streamed" };
}
function targetTopology() { return { shards: [{ id: "dst", version: 2, connection: { host: undefined, port: undefined, database: undefined, user: undefined }, addedAt: new Date().toISOString() }], strategy: "hash", strategyConfig: {}, routingVersion: 2, capturedAt: new Date().toISOString() }; }
async function setupShards(dbs) { await dbs.src.query(accountsDDL()); await dbs.dst.query(accountsDDL()); }
async function truncateAccounts(dbs) { try { await dbs.src.query(`TRUNCATE accounts RESTART IDENTITY`); await dbs.dst.query(`TRUNCATE accounts RESTART IDENTITY`); await dbs.src.query(`TRUNCATE resharding_routing_versions`); await dbs.src.query(`TRUNCATE resharding_migrations`); await dbs.src.query(`TRUNCATE resharding_units`); await dbs.src.query(`TRUNCATE resharding_changes`); await dbs.src.query(`DELETE FROM resharding_write_gate`); } catch {} }
function newDb() { return createDatabase({ sharding: { enabled: true, strategy: "hash", shards: [ { id: "src", connection: { connectionString: dbUri("co_src"), max: 5 } }, { id: "dst", connection: { connectionString: dbUri("co_dst"), max: 5 } } ] } }); }
async function initRsharding(rsharding, srcPool, dstPool) { await rsharding.init(); await rsharding.initShard("src", srcPool); await rsharding.initShard("dst", dstPool); }
function makeDeferred() { let resolve, reject; const p = new Promise((res, rej) => { resolve = res; reject = rej; }); return { promise: p, resolve, reject }; }
describe("Resharding commit-order tests (live PostgreSQL)", () => {
    beforeAll(async () => {
        try { const pool = new Pool({ connectionString: pgUri("postgres") }); await pool.query("SELECT 1"); await pool.end(); hasPostgres = true; useSocket = false; }
        catch { try { const pool = new Pool({ connectionString: pgSocketUri("postgres") }); await pool.query("SELECT 1"); await pool.end(); hasPostgres = true; useSocket = true; } catch { hasPostgres = false; } }
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["co_src", "co_dst"]) { try { await pool.query(`DROP DATABASE ${db} WITH (FORCE)`); } catch {} await pool.query(`CREATE DATABASE ${db}`); }
        await pool.end();
    }, 30000);
    afterAll(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["co_src", "co_dst"]) { try { await pool.query(`DROP DATABASE ${db} WITH (FORCE)`); } catch {} }
        await pool.end();
    }, 30000);
    beforeEach(async () => {
        if (!hasPostgres) return;
        const dbs = { src: new Pool({ connectionString: dbUri("co_src"), max: 5 }), dst: new Pool({ connectionString: dbUri("co_dst"), max: 5 }) };
        await setupShards(dbs); await dbs.src.end(); await dbs.dst.end();
    }, 10000);
    afterEach(async () => {
        if (!hasPostgres) return;
        const dbs = { src: new Pool({ connectionString: dbUri("co_src"), max: 5 }), dst: new Pool({ connectionString: dbUri("co_dst"), max: 5 }) };
        await truncateAccounts(dbs); await dbs.src.end(); await dbs.dst.end();
    }, 10000);
    it("B reaches recordChange first and serialization is reflected in commit_order", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("co_src"), max: 10 });
        const dstPool = new Pool({ connectionString: dbUri("co_dst"), max: 10 });
        let db = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            db = newDb(); const rsharding = db.resharding; await initRsharding(rsharding, srcPool, dstPool);
            const migrationId = "co-commit-order"; const descriptor = makeDescriptor(migrationId);
            await rsharding.plan(descriptor, targetTopology()); await rsharding.start(); await rsharding.copyUnits(descriptor);
            expect((await rsharding.getStatus()).status).toBe("catching_up");
            const bRecorded = makeDeferred();
            const startBalances = { b: 101, a: 102 };
            const bClient = await srcPool.connect();
            const bTxPromise = (async () => { await bClient.query("BEGIN"); const eventB = await rsharding.recordChange(bClient, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: startBalances.b, shard_key: "k-1" } }); bRecorded.resolve(); await bClient.query("SELECT pg_sleep(0.3)"); await bClient.query(`UPDATE accounts SET balance = $1 WHERE id = 1`, [startBalances.b]); await bClient.query("COMMIT"); bClient.release(); return eventB; })();
            await bRecorded.promise;
            const aClient = await srcPool.connect(); await aClient.query("BEGIN");
            const aRecordPromise = rsharding.recordChange(aClient, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: startBalances.b, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: startBalances.a, shard_key: "k-1" } });
            const [eventB, eventA] = await Promise.all([bTxPromise, aRecordPromise]);
            expect(eventB.commitOrder).toBeLessThan(eventA.commitOrder);
            await aClient.query(`UPDATE accounts SET balance = $1 WHERE id = 1`, [startBalances.a]); await aClient.query("COMMIT"); aClient.release();
            const srcRow = await srcPool.query(`SELECT balance FROM accounts WHERE id = 1`); expect(Number(srcRow.rows[0].balance)).toBe(startBalances.a);
            const changeRows = await srcPool.query(`SELECT commit_order, txid FROM resharding_changes WHERE migration_id = $1 ORDER BY commit_order ASC`, [migrationId]);
            expect(changeRows.rows.length).toBe(2); expect(Number(changeRows.rows[0].commit_order)).toBeLessThan(Number(changeRows.rows[1].commit_order));
            await rsharding.validate(); await rsharding.cutover(); await rsharding.finalize();
            const dstRow = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`); expect(Number(dstRow.rows[0].balance)).toBe(startBalances.a);
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db.sharding.shutdown();
        } finally { try { if (db) await db.sharding.shutdown(); } catch {} await srcPool.end(); await dstPool.end(); }
    }, 120000);
    it("transaction rolling back after acquiring ordering lock leaves no pending event", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("co_src"), max: 5 }); const dstPool = new Pool({ connectionString: dbUri("co_dst"), max: 5 }); let db = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            db = newDb(); const rsharding = db.resharding; await initRsharding(rsharding, srcPool, dstPool);
            const migrationId = "co-rollback"; const descriptor = makeDescriptor(migrationId);
            await rsharding.plan(descriptor, targetTopology()); await rsharding.start(); await rsharding.copyUnits(descriptor);
            const client = await srcPool.connect(); await client.query("BEGIN");
            await rsharding.recordChange(client, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: 999, shard_key: "k-1" } });
            await client.query("UPDATE accounts SET balance = 999 WHERE id = 1");
            await client.query("ROLLBACK"); client.release();
            const cnt = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1`, [migrationId]); expect(Number(cnt.rows[0].c)).toBe(0);
            const srcBal = await srcPool.query(`SELECT balance FROM accounts WHERE id = 1`); expect(Number(srcBal.rows[0].balance)).toBe(100);
            await rsharding.validate(); await rsharding.cutover(); await rsharding.finalize();
            const dstBal = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`); expect(Number(dstBal.rows[0].balance)).toBe(100);
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db.sharding.shutdown();
        } finally { try { if (db) await db.sharding.shutdown(); } catch {} await srcPool.end(); await dstPool.end(); }
    }, 120000);
    it("multiple change events in one source transaction replay deterministically", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("co_src"), max: 5 }); const dstPool = new Pool({ connectionString: dbUri("co_dst"), max: 5 }); let db = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (2, 'bob', 200, 'k-2')`);
            db = newDb(); const rsharding = db.resharding; await initRsharding(rsharding, srcPool, dstPool);
            const migrationId = "co-multi-events"; const descriptor = makeDescriptor(migrationId);
            await rsharding.plan(descriptor, targetTopology()); await rsharding.start(); await rsharding.copyUnits(descriptor);
            const client = await srcPool.connect(); await client.query("BEGIN");
            await client.query(`UPDATE accounts SET balance = 110 WHERE id = 1`);
            await client.query(`UPDATE accounts SET balance = 220 WHERE id = 2`);
            await rsharding.recordChange(client, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: 110, shard_key: "k-1" } });
            await rsharding.recordChange(client, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-2", oldRow: { id: 2, owner: "bob", balance: 200, shard_key: "k-2" }, newRow: { id: 2, owner: "bob", balance: 220, shard_key: "k-2" } });
            await client.query("COMMIT"); client.release();
            const events = await srcPool.query(`SELECT commit_order, txid FROM resharding_changes WHERE migration_id = $1 ORDER BY commit_order ASC`, [migrationId]);
            expect(events.rows.length).toBe(2);
            expect(events.rows.map((r) => Number(r.txid)).every((t) => t === Number(events.rows[0].txid))).toBe(true);
            const distinctOrders = new Set(events.rows.map((r) => Number(r.commit_order))); expect(distinctOrders.size).toBe(2);
            await rsharding.validate(); await rsharding.cutover(); await rsharding.finalize();
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db.sharding.shutdown();
        } finally { try { if (db) await db.sharding.shutdown(); } catch {} await srcPool.end(); await dstPool.end(); }
    }, 120000);
    it("transaction fails while inserting outbox event and no partial change persists", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("co_src"), max: 5 }); const dstPool = new Pool({ connectionString: dbUri("co_dst"), max: 5 }); let db = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            db = newDb(); const rsharding = db.resharding; await initRsharding(rsharding, srcPool, dstPool);
            const migrationId = "co-fail-outbox"; const descriptor = makeDescriptor(migrationId);
            await rsharding.plan(descriptor, targetTopology()); await rsharding.start(); await rsharding.copyUnits(descriptor);
            const client = await srcPool.connect();
            try { await client.query("BEGIN"); await client.query(`UPDATE accounts SET balance = 777 WHERE id = 1`); await expect(rsharding.recordChange(client, { migrationId, unitId: "hash:src->dst", table: null, operation: "update", shardKey: "k-1", oldRow: null, newRow: null })).rejects.toThrow(); await client.query("ROLLBACK"); }
            catch { try { await client.query("ROLLBACK"); } catch {} }
            finally { client.release(); }
            const srcBal = await srcPool.query(`SELECT balance FROM accounts WHERE id = 1`); expect(Number(srcBal.rows[0].balance)).toBe(100);
            const cnt = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1`, [migrationId]); expect(Number(cnt.rows[0].c)).toBe(0);
            await rsharding.validate(); await rsharding.cutover(); await rsharding.finalize();
            const dstBal = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`); expect(Number(dstBal.rows[0].balance)).toBe(100);
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db.sharding.shutdown();
        } finally { try { if (db) await db.sharding.shutdown(); } catch {} await srcPool.end(); await dstPool.end(); }
    }, 120000);
    it("several concurrent writers obtain distinct commit_order values", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("co_src"), max: 10 }); const dstPool = new Pool({ connectionString: dbUri("co_dst"), max: 10 }); let db = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            db = newDb(); const rsharding = db.resharding; await initRsharding(rsharding, srcPool, dstPool);
            const migrationId = "co-concurrent-writers"; const descriptor = makeDescriptor(migrationId);
            await rsharding.plan(descriptor, targetTopology()); await rsharding.start(); await rsharding.copyUnits(descriptor);
            const writerCount = 5; const results = [];
            const makeWriter = async (i) => { const client = await srcPool.connect(); await client.query("BEGIN"); const evt = await rsharding.recordChange(client, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: 100 + i, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: 200 + i, shard_key: "k-1" } }); await client.query(`UPDATE accounts SET balance = $1 WHERE id = 1`, [200 + i]); await client.query("COMMIT"); client.release(); return evt; };
            await Promise.all(Array.from({ length: writerCount }, (_, i) => makeWriter(i).then((e) => results.push(e))));
            expect(results.length).toBe(writerCount); expect(new Set(results.map((e) => e.commitOrder)).size).toBe(writerCount);
            const count = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1`, [migrationId]); expect(Number(count.rows[0].c)).toBe(writerCount);
            await rsharding.validate(); await rsharding.cutover(); await rsharding.finalize();
            const srcCount = await srcPool.query(`SELECT COUNT(*) AS c FROM accounts`); const dstCount = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`); expect(Number(srcCount.rows[0].c)).toBe(Number(dstCount.rows[0].c));
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db.sharding.shutdown();
        } finally { try { if (db) await db.sharding.shutdown(); } catch {} await srcPool.end(); await dstPool.end(); }
    }, 120000);
});
