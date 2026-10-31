import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";

import { createDatabase } from "../../src/index.js";
import { WriteFencedError } from "../../src/resharding/resharding-errors.js";
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

async function setupShards(srcPool, dstPool) {
    await srcPool.query(accountsDDL());
    await dstPool.query(accountsDDL());
}

async function truncateAccounts(srcPool, dstPool) {
    try {
        await srcPool.query(`TRUNCATE accounts RESTART IDENTITY`);
        await dstPool.query(`TRUNCATE accounts RESTART IDENTITY`);
    } catch {
        // ignore
    }
}

describe("CDC instrumentation tests (live PostgreSQL)", () => {
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
        for (const db of ["cdc_src", "cdc_dst", "cdc_coord"]) {
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
        for (const db of ["cdc_src", "cdc_dst", "cdc_coord"]) {
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
        const srcPool = new Pool({ connectionString: dbUri("cdc_src") });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst") });
        await setupShards(srcPool, dstPool);
        await srcPool.end();
        await dstPool.end();
    }, 10000);

    afterEach(async () => {
        if (!hasPostgres) return;
        const srcPool = new Pool({ connectionString: dbUri("cdc_src") });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst") });
        await truncateAccounts(srcPool, dstPool);
        try {
            await srcPool.query(`TRUNCATE resharding_routing_versions`);
            await srcPool.query(`TRUNCATE resharding_migrations`);
            await srcPool.query(`TRUNCATE resharding_units`);
            await srcPool.query(`TRUNCATE resharding_changes`);
        } catch {
            // tables may not exist yet
        }
        await srcPool.end();
        await dstPool.end();
    }, 10000);

    function newDb() {
        return createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    { id: "src", connection: { connectionString: dbUri("cdc_src"), max: 5 } },
                    { id: "dst", connection: { connectionString: dbUri("cdc_dst"), max: 5 } },
                ],
            },
        });
    }

    async function initRsharding(rsharding, srcPool, dstPool) {
        await rsharding.init();
        await rsharding.initShard("src", srcPool);
        await rsharding.initShard("dst", dstPool);
    }

    it("mutation and CDC event commit together atomically", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("cdc_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst"), max: 5 });
        let db = null;

        try {
            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("cdc-atomic"), targetTopology());
            await rsharding.start();

            await rsharding.instrumentedTransaction("src", async (client, recordChange) => {
                await client.query(
                    `INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1, $2, $3, $4)`,
                    [1, "alice", 100, "k-1"],
                );
                await recordChange({
                    migrationId: "cdc-atomic",
                    unitId: "hash:src->dst",
                    table: "accounts",
                    operation: "insert",
                    shardKey: "k-1",
                    oldRow: null,
                    newRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" },
                });
            });

            const account = await srcPool.query(`SELECT * FROM accounts WHERE id = 1`);
            expect(account.rows[0].owner).toBe("alice");

            const cdcEvents = await srcPool.query(
                `SELECT * FROM resharding_changes WHERE migration_id = $1`,
                ["cdc-atomic"],
            );
            expect(cdcEvents.rows.length).toBe(1);
            expect(cdcEvents.rows[0].operation).toBe("insert");

            await rsharding.cancel();
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

    it("mutation rollback removes the CDC event", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("cdc_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst"), max: 5 });
        let db = null;

        try {
            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("cdc-rollback"), targetTopology());
            await rsharding.start();

            await expect(
                rsharding.instrumentedTransaction("src", async (client, recordChange) => {
                    await client.query(
                        `INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1, $2, $3, $4)`,
                        [1, "alice", 100, "k-1"],
                    );
                    await recordChange({
                        migrationId: "cdc-rollback",
                        unitId: "hash:src->dst",
                        table: "accounts",
                        operation: "insert",
                        shardKey: "k-1",
                        oldRow: null,
                        newRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" },
                    });
                    throw new Error("simulated failure");
                }),
            ).rejects.toThrow("simulated failure");

            const account = await srcPool.query(`SELECT * FROM accounts WHERE id = 1`);
            expect(account.rows.length).toBe(0);

            const cdcEvents = await srcPool.query(
                `SELECT * FROM resharding_changes WHERE migration_id = $1`,
                ["cdc-rollback"],
            );
            expect(cdcEvents.rows.length).toBe(0);

            await rsharding.cancel();
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

    it("multiple events in one transaction replay as one group", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("cdc_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst"), max: 5 });
        let db = null;

        try {
            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("cdc-multi"), targetTopology());
            await rsharding.start();

            await rsharding.instrumentedTransaction("src", async (_client, recordChange) => {
                await recordChange({
                    migrationId: "cdc-multi",
                    unitId: "hash:src->dst",
                    table: "accounts",
                    operation: "insert",
                    shardKey: "k-1",
                    oldRow: null,
                    newRow: { id: 1, owner: "alice", balance: 100, shard_key: "k-1" },
                });
                await recordChange({
                    migrationId: "cdc-multi",
                    unitId: "hash:src->dst",
                    table: "accounts",
                    operation: "insert",
                    shardKey: "k-2",
                    oldRow: null,
                    newRow: { id: 2, owner: "bob", balance: 200, shard_key: "k-2" },
                });
            });

            const cdcEvents = await srcPool.query(
                `SELECT * FROM resharding_changes WHERE migration_id = $1 ORDER BY id`,
                ["cdc-multi"],
            );
            expect(cdcEvents.rows.length).toBe(2);
            expect(cdcEvents.rows[0].operation).toBe("insert");
            expect(cdcEvents.rows[1].operation).toBe("insert");
            expect(cdcEvents.rows[0].shard_key).toBe("k-1");
            expect(cdcEvents.rows[1].shard_key).toBe("k-2");

            await rsharding.cancel();
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

    it("unsupported write paths are documented (no automatic CDC on db.query)", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("cdc_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst"), max: 5 });
        let db = null;

        try {
            await setupShards(srcPool, dstPool);

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("cdc-unsupported"), targetTopology());
            await rsharding.start();

            await db.query(
                `INSERT INTO accounts (id, owner, balance, shard_key) VALUES ($1, $2, $3, $4)`,
                [1, "alice", 100, "k-1"],
                { shardKey: "k-1" },
            );

            const account = await srcPool.query(`SELECT * FROM accounts WHERE id = 1`);
            expect(account.rows[0].owner).toBe("alice");

            const cdcEvents = await srcPool.query(
                `SELECT * FROM resharding_changes WHERE migration_id = $1`,
                ["cdc-unsupported"],
            );
            expect(cdcEvents.rows.length).toBe(0);

            await rsharding.cancel();
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

    it("fencing applies to instrumented transactions", async () => {
        if (!hasPostgres) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }

        const srcPool = new Pool({ connectionString: dbUri("cdc_src"), max: 5 });
        const dstPool = new Pool({ connectionString: dbUri("cdc_dst"), max: 5 });
        let db = null;

        try {
            await setupShards(srcPool, dstPool);

            db = newDb();
            const rsharding = db.resharding;
            await initRsharding(rsharding, srcPool, dstPool);

            await rsharding.plan(makeDescriptor("cdc-fence"), targetTopology());
            await rsharding.start();
            await rsharding.copyUnits(makeDescriptor("cdc-fence"));
            await rsharding.validate();
            await rsharding.cutover();

            await expect(
                rsharding.instrumentedTransaction("src", async (_client, recordChange) => {
                    await recordChange({
                        migrationId: "cdc-fence",
                        unitId: "hash:src->dst",
                        table: "accounts",
                        operation: "insert",
                        shardKey: "k-1",
                        oldRow: null,
                        newRow: { id: 1, owner: "stale", balance: 1, shard_key: "k-1" },
                    });
                }),
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
});
