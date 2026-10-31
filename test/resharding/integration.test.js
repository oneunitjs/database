import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";

import { createDatabase } from "../../src/index.js";
import { ReshardingError, WriteFencedError } from "../../src/resharding/resharding-errors.js";
import { Pool } from "pg";

const PG_USER = process.env.PG_USER || "postgres";
const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = process.env.PG_PORT || 5432;
const PG_PASSWORD = process.env.PG_PASSWORD || "";

function pgUri(dbname) {
    const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : "";
    return `postgresql://${PG_USER}${auth}@${PG_HOST}:${PG_PORT}/${dbname}`;
}

function pgSocketUri(dbname) {
    const auth = PG_PASSWORD ? `:${PG_PASSWORD}` : "";
    return `postgresql://${PG_USER}${auth}@/${dbname}?host=/run/postgresql`;
}

let hasPostgres = false;
let useSocket = false;

function dbUri(dbname) {
    return useSocket ? pgSocketUri(dbname) : pgUri(dbname);
}

function accountsDDL() {
    return `
        CREATE TABLE IF NOT EXISTS accounts (
            id BIGSERIAL PRIMARY KEY,
            owner TEXT NOT NULL,
            balance BIGINT NOT NULL DEFAULT 0,
            shard_key TEXT NOT NULL
        )
    `;
}

function makeDescriptor(migrationId) {
    return {
        migrationId,
        tables: [
            {
                table: "accounts",
                primaryKeyColumns: ["id"],
                shardKeyColumns: ["shard_key"],
                stableKeyOrder: true,
                description: "accounts table",
                readBatch: async ({ client, unit, batchSize, cursor }) => {
                    const lastId = cursor && cursor.lastValues ? cursor.lastValues[0] : 0;
                    const result = await client.query(
                        `SELECT id, owner, balance, shard_key FROM accounts WHERE id > $1 ORDER BY id ASC LIMIT $2`,
                        [lastId, batchSize],
                    );
                    const rows = result.rows;
                    const nextCursor =
                        rows.length === 0
                            ? null
                            : {
                                  kind: "keyset",
                                  columns: ["id"],
                                  lastValues: [rows[rows.length - 1].id],
                                  batchNo: (cursor && cursor.batchNo ? cursor.batchNo : 0) + 1,
                                  updatedAt: new Date().toISOString(),
                              };
                    return { rows, nextCursor, done: rows.length < batchSize };
                },
                upsertRows: async ({ client, rows }) => {
                    for (const row of rows) {
                        await client.query(
                            `INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1,$2,$3,$4)
                             ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key`,
                            [row.id, row.owner, row.balance, row.shard_key],
                        );
                    }
                    return { inserted: rows.length, updated: 0 };
                },
                countRows: async ({ client }) => {
                    const res = await client.query(`SELECT COUNT(*) AS c FROM accounts`);
                    return Number(res.rows[0].c);
                },
            },
        ],
        order: ["accounts"],
        consistencyBoundary: "streamed",
    };
}

function targetTopology() {
    return {
        shards: [
            {
                id: "dst",
                version: 2,
                connection: { host: undefined, port: undefined, database: undefined, user: undefined },
                addedAt: new Date().toISOString(),
            },
        ],
        strategy: "hash",
        strategyConfig: {},
        routingVersion: 2,
        capturedAt: new Date().toISOString(),
    };
}

async function setupShards(dbs) {
    await dbs.src.query(accountsDDL());
    await dbs.dst.query(accountsDDL());
}

async function truncateAccounts(dbs) {
    try {
        await dbs.src.query(`TRUNCATE accounts RESTART IDENTITY`);
        await dbs.dst.query(`TRUNCATE accounts RESTART IDENTITY`);
    } catch {
        // ignore
    }
}

describe("Resharding integration tests (live PostgreSQL)", () => {
    beforeAll(async () => {
        try {
            const pool = new Pool({ connectionString: pgUri("postgres") });
            await pool.query("SELECT 1");
            await pool.end();
            hasPostgres = true;
            useSocket = false;
        } catch {
            try {
                const pool = new Pool({ connectionString: pgSocketUri("postgres") });
                await pool.query("SELECT 1");
                await pool.end();
                hasPostgres = true;
                useSocket = true;
            } catch {
                hasPostgres = false;
            }
        }
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["rs_src", "rs_dst", "rs_coord"]) {
            try {
                await pool.query(`CREATE DATABASE ${db}`);
            } catch {
                // may already exist
            }
        }
        await pool.end();
    }, 30000);

    afterAll(async () => {
        if (!hasPostgres) return;
        const pool = new Pool({ connectionString: dbUri("postgres") });
        for (const db of ["rs_src", "rs_dst", "rs_coord"]) {
            try {
                await pool.query(`DROP DATABASE ${db} WITH (FORCE)`);
            } catch {
                // ignore
            }
        }
        await pool.end();
    }, 30000);

    beforeEach(async () => {
        if (!hasPostgres) return;
        const dbs = {
            src: new Pool({ connectionString: dbUri("rs_src") }),
            dst: new Pool({ connectionString: dbUri("rs_dst") }),
        };
        await setupShards(dbs);
        await dbs.src.end();
        await dbs.dst.end();
    }, 10000);

    afterEach(async () => {
        if (!hasPostgres) return;
        const dbs = {
            src: new Pool({ connectionString: dbUri("rs_src") }),
            dst: new Pool({ connectionString: dbUri("rs_dst") }),
        };
        await truncateAccounts(dbs);
        // Clear coordination state leaked by previous tests (routing versions,
        // migrations) so each test starts from a clean control plane.
        try {
            await dbs.src.query(`TRUNCATE resharding_routing_versions`);
            await dbs.src.query(`TRUNCATE resharding_migrations`);
            await dbs.src.query(`TRUNCATE resharding_units`);
        } catch {
            // tables may not exist yet
        }
        await dbs.src.end();
        await dbs.dst.end();
    }, 10000);

    function newDb() {
        return createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    { id: "src", connection: { connectionString: dbUri("rs_src"), max: 5 } },
                    { id: "dst", connection: { connectionString: dbUri("rs_dst"), max: 5 } },
                ],
            },
        });
    }

    async function initRsharding(rsharding, srcPool, dstPool) {
        await rsharding.init();
        await rsharding.initShard("src", srcPool);
        await rsharding.initShard("dst", dstPool);
    }

    it("plans, copies, validates, cuts over, and finalizes a migration", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1'), ('b', 200, 'k-2'), ('c', 300, 'k-3')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            expect(rsharding).toBeDefined();
            await initRsharding(rsharding, srcPool, dstPool);

            const plan = await rsharding.plan(makeDescriptor("mig-1"), targetTopology());
            expect(plan.migrationId).toBe("mig-1");
            expect(plan.units.length).toBeGreaterThan(0);

            await rsharding.start();
            expect((await rsharding.getStatus()).status).toBe("copying");

            await rsharding.copyUnits(makeDescriptor("mig-1"));
            expect((await rsharding.getStatus()).status).toBe("catching_up");

            const { report, passed } = await rsharding.validate();
            expect(passed).toBe(true);
            expect(report.length).toBeGreaterThan(0);
            expect((await rsharding.getStatus()).status).toBe("ready_for_cutover");

            const cut = await rsharding.cutover();
            expect(cut.status).toBe("completed");
            const version = await rsharding.refreshRoutingVersion();
            expect(version).toBe(2);

            const fin = await rsharding.finalize();
            expect(fin.status).toBe("finalized");

            const dstCount = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(dstCount.rows[0].c)).toBe(3);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("pause and resume preserve committed progress", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1'), ('b', 200, 'k-2'), ('c', 300, 'k-3')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            const plan = await rsharding.plan(makeDescriptor("mig-pause"), targetTopology());
            await rsharding.start();
            expect((await rsharding.getStatus()).status).toBe("copying");

            const paused = await rsharding.pause();
            expect(paused.status).toBe("paused");

            const resumed = await rsharding.resume();
            expect(resumed.status).toBe("copying");

            await rsharding.copyUnits(makeDescriptor("mig-pause"));
            expect((await rsharding.getStatus()).status).toBe("catching_up");

            const { passed } = await rsharding.validate();
            expect(passed).toBe(true);

            await rsharding.cutover();
            await rsharding.finalize();
            expect((await rsharding.getStatus()).status).toBe("finalized");

            const dstCount = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(dstCount.rows[0].c)).toBe(3);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("cancel does not delete source data and leaves routing unchanged", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1'), ('b', 200, 'k-2')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-cancel"), targetTopology());
            await rsharding.start();

            const cancelled = await rsharding.cancel();
            expect(cancelled.status).toBe("cancelled");

            const srcCount = await srcPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(srcCount.rows[0].c)).toBe(2);

            const dstCount = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(dstCount.rows[0].c)).toBe(0);

            const version = await rsharding.refreshRoutingVersion();
            expect(version).toBe(0);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("concurrent writes during copying are captured and applied at cutover", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1'), ('b', 200, 'k-2')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-concurrent"), targetTopology());
            await rsharding.start();

            // Simulate a concurrent write during copying via the transactional outbox.
            const concurrentClient = await srcPool.connect();
            try {
                await concurrentClient.query("BEGIN");
                await concurrentClient.query(
                    `INSERT INTO accounts (owner, balance, shard_key) VALUES ('c', 300, 'k-3')`,
                );
                await rsharding.recordChange(concurrentClient, {
                    migrationId: "mig-concurrent",
                    unitId: "hash:src->dst",
                    table: "accounts",
                    operation: "insert",
                    shardKey: "k-3",
                    oldRow: null,
                    newRow: { id: 3, owner: "c", balance: 300, shard_key: "k-3" },
                });
                await concurrentClient.query("COMMIT");
            } catch (error) {
                await concurrentClient.query("ROLLBACK").catch(() => {});
                throw error;
            } finally {
                concurrentClient.release();
            }

            await rsharding.copyUnits(makeDescriptor("mig-concurrent"));
            expect((await rsharding.getStatus()).status).toBe("catching_up");

            await rsharding.drainChanges("hash:src->dst", Number.MAX_SAFE_INTEGER);

            const { passed } = await rsharding.validate();
            expect(passed).toBe(true);

            await rsharding.cutover();
            await rsharding.finalize();

            const dstCount = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(dstCount.rows[0].c)).toBe(3);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("rejects planning with a stale routing version", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await expect(
                rsharding.plan(
                    makeDescriptor("mig-stale"),
                    targetTopology(),
                    { expectedRoutingVersion: 99 },
                ),
            ).rejects.toBeInstanceOf(ReshardingError);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 30000);

    it("concurrent start attempts on the same migration contend at the store", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-lock"), targetTopology());

            await rsharding.start();
            expect((await rsharding.getStatus()).status).toBe("copying");

            const status = await rsharding.getStatus();
            expect(status.status).toBe("copying");
            expect(status.version).toBeGreaterThanOrEqual(1);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 30000);

    it("restart recovery: load existing migration and resume", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1'), ('b', 200, 'k-2')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);
            await rsharding.plan(makeDescriptor("mig-restart"), targetTopology());
            await rsharding.start();
            expect((await rsharding.getStatus()).status).toBe("copying");

            db = null;
            db = newDb();
            const rsharding2 = db.resharding;
            await initRsharding(rsharding2, srcPool, dstPool);
            const loaded = await rsharding2.load("mig-restart");
            expect(loaded).toBeDefined();
            expect(loaded.status).toBe("copying");
            expect(rsharding2.migrationId).toBe("mig-restart");

            await rsharding2.copyUnits(makeDescriptor("mig-restart"));
            expect((await rsharding2.getStatus()).status).toBe("catching_up");

            const { passed } = await rsharding2.validate(makeDescriptor("mig-restart"));
            expect(passed).toBe(true);
            await rsharding2.cutover(makeDescriptor("mig-restart"));
            await rsharding2.finalize();
            expect((await rsharding2.getStatus()).status).toBe("finalized");

            const dstCount = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(dstCount.rows[0].c)).toBe(2);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("cancel leaves migration in cancelled state and source untouched", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (owner, balance, shard_key) VALUES ('a', 100, 'k-1')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);
            await rsharding.plan(makeDescriptor("mig-cancel2"), targetTopology());
            await rsharding.start();

            await rsharding.cancel();
            expect((await rsharding.getStatus()).status).toBe("cancelled");

            const srcCount = await srcPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(srcCount.rows[0].c)).toBe(1);
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 30000);

    // ------------------------------------------------------------------
    // Write fencing enforcement tests
    // ------------------------------------------------------------------

    it("rejects writes to a fenced source shard via db.query()", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'a', 100, 'k-1')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-fence-query"), targetTopology());
            await rsharding.start();
            await rsharding.copyUnits(makeDescriptor("mig-fence-query"));
            await rsharding.validate();

            // Before cutover, writes via db.query() succeed
            await db.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (2, 'pre', 999, 'k-pre')`,
                undefined,
                { shardKey: "k-pre" },
            );

            // Cutover sets the write gate on the source shard
            await rsharding.cutover();

            // After cutover, writes to the fenced shard are rejected via db.query()
            await expect(
                db.query(
                    `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (3, 'stale', 1, 'k-stale')`,
                    undefined,
                    { shardKey: "k-stale" },
                ),
            ).rejects.toBeInstanceOf(WriteFencedError);

            await rsharding.finalize();
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("rejects writes to a fenced source shard via db.transaction()", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'a', 100, 'k-1')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-fence-tx"), targetTopology());
            await rsharding.start();
            await rsharding.copyUnits(makeDescriptor("mig-fence-tx"));
            await rsharding.validate();
            await rsharding.cutover();

            // A transaction started before cutover and committed afterward is rejected
            // because the write gate is checked before BEGIN.
            await expect(
                db.transaction(
                    async (client) => {
                        await client.query(
                            `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (2, 'tx-stale', 2, 'k-tx')`,
                        );
                    },
                    { shardKey: "k-tx" },
                ),
            ).rejects.toBeInstanceOf(WriteFencedError);

            // Source data unchanged
            const count = await srcPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(count.rows[0].c)).toBe(1);

            await rsharding.finalize();
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("rejects writes to a fenced source shard via deprecated db.sharding.query()", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'a', 100, 'k-1')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-fence-legacy"), targetTopology());
            await rsharding.start();
            await rsharding.copyUnits(makeDescriptor("mig-fence-legacy"));
            await rsharding.validate();
            await rsharding.cutover();

            // Deprecated sharding API also enforces the write gate
            await expect(
                db.sharding.query("k-legacy", `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (2, 'legacy', 3, 'k-legacy')`),
            ).rejects.toBeInstanceOf(WriteFencedError);

            const count = await srcPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(count.rows[0].c)).toBe(1);

            await rsharding.finalize();
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("allows writes to non-fenced destination shard during cutover", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'a', 100, 'k-1')`,
            );

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-fence-dst"), targetTopology());
            await rsharding.start();
            await rsharding.copyUnits(makeDescriptor("mig-fence-dst"));
            await rsharding.validate();
            await rsharding.cutover();

            // Destination shard is NOT fenced (only source shards are)
            await dstPool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (99999, 'dst-write', 4, 'k-dst')`,
            );
            const count = await dstPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            // 1 from copyUnits + 1 from this insert
            expect(Number(count.rows[0].c)).toBe(2);

            await rsharding.finalize();
        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);

    it("stale writer with cached routing cannot write after cutover completes", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("rs_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("rs_dst"), max: 5 });
        let db = null;
        let staleDb = null;

        try {
            await srcPool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (1, 'a', 100, 'k-1')`,
            );

            // Primary db with resharding manager
            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("mig-stale-writer"), targetTopology());
            await rsharding.start();
            await rsharding.copyUnits(makeDescriptor("mig-stale-writer"));
            await rsharding.validate();
            await rsharding.cutover();
            await rsharding.finalize();

            // A "stale" application instance with its own database connection
            // (separate ReshardingManager, no knowledge of the migration)
            staleDb = newDb();

            // Writes to the source shard are rejected because the write gate
            // is set shard-locally and persists across instances
            await expect(
                staleDb.query(
                    `INSERT INTO accounts (id, owner, balance, shard_key) VALUES (2, 'stale-instance', 5, 'k-stale')`,
                    undefined,
                    { shardKey: "k-1" },
                ),
            ).rejects.toBeInstanceOf(WriteFencedError);

            const count = await srcPool.query(`SELECT COUNT(*) AS c FROM accounts`);
            expect(Number(count.rows[0].c)).toBe(1);

        } finally {
            try {
                await db && db.sharding && db.sharding.shutdown();
            } catch {
                // ignore
            }
            try {
                await staleDb && staleDb.sharding && staleDb.sharding.shutdown();
            } catch {
                // ignore
            }
            await srcPool.end();
            await dstPool.end();
        }
    }, 60000);
});
