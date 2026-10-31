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
function makeDescriptor(migrationId) { return { migrationId, tables: [{ table: "accounts", primaryKeyColumns: ["id"], shardKeyColumns: ["shard_key"], stableKeyOrder: true, description: "accounts", readBatch: async ({ client, batchSize, cursor }) => { const lastId = cursor && cursor.lastValues ? cursor.lastValues[0] : 0; const result = await client.query(`SELECT id, owner, balance, shard_key FROM accounts WHERE id > $1 ORDER BY id ASC LIMIT $2`, [lastId, batchSize]); const rows = result.rows; const nextCursor = rows.length === 0 ? null : { kind: "keyset", columns: ["id"], lastValues: [rows[rows.length - 1].id], batchNo: (cursor && cursor.batchNo ? cursor.batchNo : 0) + 1, updatedAt: new Date().toISOString() }; return { rows, nextCursor, done: rows.length < batchSize }; }, upsertRows: async ({ client, rows }) => { for (const row of rows) await client.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key`, [row.id, row.owner, row.balance, row.shard_key]); return { inserted: rows.length, updated: 0 }; }, countRows: async ({ client }) => { const res = await client.query(`SELECT COUNT(*) AS c FROM accounts`); return Number(res.rows[0].c); } }], order: ["accounts"], consistencyBoundary: "streamed" }; }
function targetTopology() { return { shards: [{ id: "dst", version: 2, connection: { host: undefined, port: undefined, database: undefined, user: undefined }, addedAt: new Date().toISOString() }], strategy: "hash", strategyConfig: {}, routingVersion: 2, capturedAt: new Date().toISOString() }; }
async function setupShards(dbs) { await dbs.src.query(accountsDDL()); await dbs.dst.query(accountsDDL()); }
async function truncateAccounts(dbs) { try { await dbs.src.query(`TRUNCATE accounts RESTART IDENTITY`); await dbs.dst.query(`TRUNCATE accounts RESTART IDENTITY`); await dbs.src.query(`TRUNCATE resharding_routing_versions`); await dbs.src.query(`TRUNCATE resharding_migrations`); await dbs.src.query(`TRUNCATE resharding_units`); await dbs.src.query(`TRUNCATE resharding_changes`); await dbs.src.query(`DELETE FROM resharding_write_gate`); } catch {} }
function newDb() { return createDatabase({ sharding: { enabled: true, strategy: "hash", shards: [ { id: "src", connection: { connectionString: dbUri("cr_src"), max: 5 } }, { id: "dst", connection: { connectionString: dbUri("cr_dst"), max: 5 } } ] } }); }
async function initRsharding(rsharding, srcPool, dstPool) { await rsharding.init(); await rsharding.initShard("src", srcPool); await rsharding.initShard("dst", dstPool); }
describe("Resharding crash-restart tests (live PostgreSQL)", () => {
    beforeAll(async () => {
        try { const pool = new Pool({ connectionString: pgUri("postgres") }); await pool.query("SELECT 1"); await pool.end(); hasPostgres = true; useSocket = false; }
        catch { try { const pool = new Pool({ connectionString: pgSocketUri("postgres") }); await pool.query("SELECT 1"); await pool.end(); hasPostgres = true; useSocket = true; } catch { hasPostgres = false; } }
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["cr_src", "cr_dst"]) { try { await pool.query(`DROP DATABASE ${db} WITH (FORCE)`); } catch {} await pool.query(`CREATE DATABASE ${db}`); }
        await pool.end();
    }, 30000);
    afterAll(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["cr_src", "cr_dst"]) { try { await pool.query(`DROP DATABASE ${db} WITH (FORCE)`); } catch {} }
        await pool.end();
    }, 30000);
    beforeEach(async () => {
        if (!hasPostgres) return;
        const dbs = { src: new Pool({ connectionString: dbUri("cr_src"), max: 5 }), dst: new Pool({ connectionString: dbUri("cr_dst"), max: 5 }) };
        await setupShards(dbs); await dbs.src.end(); await dbs.dst.end();
    }, 10000);
    afterEach(async () => {
        if (!hasPostgres) return;
        const dbs = { src: new Pool({ connectionString: dbUri("cr_src"), max: 5 }), dst: new Pool({ connectionString: dbUri("cr_dst"), max: 5 }) };
        await truncateAccounts(dbs); await dbs.src.end(); await dbs.dst.end();
    }, 10000);
    it("crash after destination commit but before ack redrains safely after restart", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("cr_src"), max: 10 });
        const dstPool = new Pool({ connectionString: dbUri("cr_dst"), max: 10 });
        let db1 = null, db2 = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            db1 = newDb(); const rsm1 = db1.resharding; await initRsharding(rsm1, srcPool, dstPool);
            const migrationId = "cr-crash-restart"; const descriptor = makeDescriptor(migrationId);
            await rsm1.plan(descriptor, targetTopology()); await rsm1.start(); await rsm1.copyUnits(descriptor);
            expect((await rsm1.getStatus()).status).toBe("catching_up");

            // A single source transaction mutating balance -> 102 records one CDC event.
            const client = await srcPool.connect();
            await client.query("BEGIN");
            await client.query(`UPDATE accounts SET balance = 102 WHERE id = 1`);
            await rsm1.recordChange(client, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: 102, shard_key: "k-1" } });
            await client.query("COMMIT"); client.release();

            // apply callback commits destination change then throws to simulate crash before ack.
            const crashApply = async (events) => {
                const dclient = await dstPool.connect();
                try {
                    await dclient.query("BEGIN");
                    for (const e of events) {
                        if (e.newRow) {
                            const r = e.newRow;
                            await dclient.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key`, [r.id, r.owner, r.balance, r.shard_key]);
                        }
                    }
                    await dclient.query("COMMIT");
                } catch (err) {
                    await dclient.query("ROLLBACK").catch(() => {});
                    throw err;
                } finally {
                    dclient.release();
                }
                throw new Error("simulated crash after destination commit before ack");
            };

            await expect(rsm1.drainChanges("ignored", "hash:src->dst", Number.MAX_SAFE_INTEGER, crashApply)).rejects.toThrow(/simulated crash/);

            // Pending event remains unacked (applied_at NULL) after crash.
            const pendingBefore = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1 AND applied_at IS NULL`, [migrationId]);
            expect(Number(pendingBefore.rows[0].c)).toBe(1);

            // Destination already has committed change.
            const dstBefore = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`);
            expect(Number(dstBefore.rows[0].balance)).toBe(102);

            // Fresh manager instance + fresh connections for restart.
            db2 = newDb(); const rsm2 = db2.resharding; await initRsharding(rsm2, srcPool, dstPool);
            await rsm2.load(migrationId);
            expect(rsm2.migrationId).toBe(migrationId);

            const okApply = async (events) => {
                const dclient = await dstPool.connect();
                try {
                    await dclient.query("BEGIN");
                    for (const e of events) {
                        if (e.newRow) {
                            const r = e.newRow;
                            await dclient.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key`, [r.id, r.owner, r.balance, r.shard_key]);
                        }
                    }
                    await dclient.query("COMMIT");
                } catch (err) {
                    await dclient.query("ROLLBACK").catch(() => {});
                    throw err;
                } finally {
                    dclient.release();
                }
            };

            const ack = await rsm2.drainChanges("ignored", "hash:src->dst", Number.MAX_SAFE_INTEGER, okApply);
            expect(ack.applied).toBe(1);

            // applied_at now advanced.
            const pendingAfter = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1 AND applied_at IS NULL`, [migrationId]);
            expect(Number(pendingAfter.rows[0].c)).toBe(0);

            // Destination matches source.
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db2.sharding.shutdown();
        } finally {
            try { if (db1) await db1.sharding.shutdown(); } catch {}
            try { if (db2) await db2.sharding.shutdown(); } catch {}
            await srcPool.end();
            await dstPool.end();
        }
    }, 180000);

    it("crash before destination commit leaves no partial destination row", async () => {
        if (!hasPostgres) { console.log("Skipping: no PostgreSQL available"); return; }
        const srcPool = new Pool({ connectionString: dbUri("cr_src"), max: 10 });
        const dstPool = new Pool({ connectionString: dbUri("cr_dst"), max: 10 });
        let db1 = null, db2 = null;
        try {
            await srcPool.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'alice', 100, 'k-1')`);
            db1 = newDb(); const rsm1 = db1.resharding; await initRsharding(rsm1, srcPool, dstPool);
            const migrationId = "cr-partial"; const descriptor = makeDescriptor(migrationId);
            await rsm1.plan(descriptor, targetTopology()); await rsm1.start(); await rsm1.copyUnits(descriptor);
            const client = await srcPool.connect();
            await client.query("BEGIN");
            await client.query(`UPDATE accounts SET balance = 103 WHERE id = 1`);
            await rsm1.recordChange(client, { migrationId, unitId: "hash:src->dst", table: "accounts", operation: "update", shardKey: "k-1", oldRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" }, newRow: { id: 1, owner: "alice", balance: 103, shard_key: "k-1" } });
            await client.query("COMMIT"); client.release();

            const before = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`);
            expect(Number(before.rows[0].balance)).toBe(100);

            // apply throws before commit => dest remains 100.
            const failingApply = async (events) => {
                const dclient = await dstPool.connect();
                try {
                    await dclient.query("BEGIN");
                    throw new Error("simulated crash before destination commit");
                } finally {
                    await dclient.query("ROLLBACK").catch(() => {});
                    dclient.release();
                }
            };
            await expect(rsm1.drainChanges("ignored", "hash:src->dst", Number.MAX_SAFE_INTEGER, failingApply)).rejects.toThrow(/simulated crash/);

            const after = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`);
            expect(Number(after.rows[0].balance)).toBe(100);

            // pending still 1.
            const pending = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1 AND applied_at IS NULL`, [migrationId]);
            expect(Number(pending.rows[0].c)).toBe(1);

            db2 = newDb(); const rsm2 = db2.resharding; await initRsharding(rsm2, srcPool, dstPool);
            await rsm2.load(migrationId);
            const okApply = async (events) => {
                const dclient = await dstPool.connect();
                try {
                    await dclient.query("BEGIN");
                    for (const e of events) {
                        if (e.newRow) {
                            const r = e.newRow;
                            await dclient.query(`INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key`, [r.id, r.owner, r.balance, r.shard_key]);
                        }
                    }
                    await dclient.query("COMMIT");
                } catch (err) {
                    await dclient.query("ROLLBACK").catch(() => {});
                    throw err;
                } finally { dclient.release(); }
            };
            await rsm2.drainChanges("ignored", "hash:src->dst", Number.MAX_SAFE_INTEGER, okApply);
            const finalDst = await dstPool.query(`SELECT balance FROM accounts WHERE id = 1`);
            expect(Number(finalDst.rows[0].balance)).toBe(103);
            const pending2 = await srcPool.query(`SELECT COUNT(*) AS c FROM resharding_changes WHERE migration_id = $1 AND applied_at IS NULL`, [migrationId]);
            expect(Number(pending2.rows[0].c)).toBe(0);
            const srcRecs = (await srcPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            const dstRecs = (await dstPool.query(`SELECT id, owner, balance, shard_key FROM accounts ORDER BY id`)).rows;
            expect(dstRecs).toEqual(srcRecs);
            await db2.sharding.shutdown();
        } finally {
            try { if (db1) await db1.sharding.shutdown(); } catch {}
            try { if (db2) await db2.sharding.shutdown(); } catch {}
            await srcPool.end();
            await dstPool.end();
        }
    }, 180000);
});
