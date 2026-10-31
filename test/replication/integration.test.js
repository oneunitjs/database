import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { execSync } from "child_process";
import { Pool } from "pg";
import { createDatabase } from "../../src/index.js";
import { ReplicaUnavailableError } from "../../src/replication/errors.js";

const PRIMARY_URI = "postgresql://postgres@/postgres?host=/run/postgresql";
const REPLICA_URI = "postgresql://postgres@/postgres?host=/tmp/kilo&port=5433";
const REPLICA_DATA_DIR = "/tmp/kilo/replica_data";

function poolFor(uri) {
    return new Pool({ connectionString: uri });
}

let hasPrimary = false;
let hasReplica = false;

async function tryConnect(uri) {
    const pool = poolFor(uri);
    try {
        await pool.query("SELECT 1");
        await pool.end();
        return true;
    } catch {
        try {
            await pool.end();
        } catch {
            // ignore
        }
        return false;
    }
}

async function startReplica() {
    // Remove stale postmaster.pid left by a previous crashed instance.
    try {
        execSync("rm -f /tmp/kilo/replica_data/postmaster.pid", { stdio: "pipe" });
    } catch {
        // ignore
    }
    try {
        execSync(
            `pg_ctl -D ${REPLICA_DATA_DIR} -o "-p 5433" -l /tmp/kilo/replica_it.log start`,
            { stdio: "pipe", timeout: 20000 },
        );
    } catch {
        // already running or failed to start
    }
    for (let i = 0; i < 30; i++) {
        if (await tryConnect(REPLICA_URI)) {
            return true;
        }
        await new Promise((r) => setTimeout(r, 500));
    }
    return false;
}

async function seedPrimary() {
    const admin = poolFor(PRIMARY_URI);
    try {
        await admin.query("DROP TABLE IF EXISTS repl_integration_tick");
        await admin.query(
            "CREATE TABLE repl_integration_tick (id SERIAL PRIMARY KEY, n INT)",
        );
        await admin.query(
            "INSERT INTO repl_integration_tick (n) VALUES (1), (2), (3)",
        );
    } finally {
        await admin.end();
    }
}

describe("Replication integration tests (live PostgreSQL)", () => {
    beforeAll(async () => {
        hasPrimary = await tryConnect(PRIMARY_URI);
        if (!hasPrimary) {
            return;
        }
        await seedPrimary();
        try {
            hasReplica = await startReplica();
        } catch {
            hasReplica = false;
        }
        if (hasReplica) {
            // Wait for the replica to catch up with the seed row(s).
            for (let i = 0; i < 40; i++) {
                try {
                    const pool = poolFor(REPLICA_URI);
                    const r = await pool.query(
                        "SELECT count(*) FROM repl_integration_tick",
                    );
                    await pool.end();
                    if (Number(r.rows[0].count) === 3) {
                        break;
                    }
                } catch {
                    // keep waiting
                }
                await new Promise((r) => setTimeout(r, 500));
            }
        }
    }, 90000);

    afterAll(async () => {
        if (!hasPrimary) {
            return;
        }
        const admin = poolFor(PRIMARY_URI);
        try {
            await admin.query("DROP TABLE IF EXISTS repl_integration_tick");
        } catch {
            // ignore
        }
        await admin.end();
    });

    beforeEach(async () => {
        if (!hasPrimary) {
            return;
        }
        const admin = poolFor(PRIMARY_URI);
        try {
            await admin.query("TRUNCATE repl_integration_tick");
            await admin.query(
                "INSERT INTO repl_integration_tick (n) VALUES (1), (2), (3)",
            );
        } catch {
            // ignore
        }
        await admin.end();
    });

    it("routes reads to the primary by default (backward compatible)", async () => {
        if (!hasPrimary) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }
        const db = createDatabase({ connectionString: PRIMARY_URI });
        const res = await db.query("SELECT count(*) FROM repl_integration_tick");
        expect(Number(res.rows[0].count)).toBe(3);
        await db.shutdown();
    });

    it("routes replica-preferred reads to a live streaming replica", async () => {
        if (!hasPrimary || !hasReplica) {
            console.log(
                "Skipping: live primary/replica not available (primary=",
                hasPrimary, "replica=", hasReplica, ")",
            );
            return;
        }
        const db = createDatabase({
            connectionString: PRIMARY_URI,
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: { id: "primary", connection: { connectionString: PRIMARY_URI } },
                replicas: [
                    { id: "replica-0", connection: { connectionString: REPLICA_URI } },
                ],
                lagThresholdMs: 1000,
            },
        });

        await db.replication.health();
        const rep = db.replication.readEndpoint("replica-preferred");
        expect(rep).toBeDefined();
        expect(rep.role).toBe("replica");
        expect(rep.endpointId).toBe("replica-0");

        const res = await db.query("SELECT count(*) FROM repl_integration_tick", [], {
            readPreference: "replica-preferred",
        });
        expect(Number(res.rows[0].count)).toBe(3);

        await db.shutdown();
    });

    it("reports real replication lag and distinguishes unknown from zero lag", async () => {
        if (!hasPrimary || !hasReplica) {
            console.log("Skipping: live replica not available");
            return;
        }
        const db = createDatabase({
            connectionString: PRIMARY_URI,
            replication: {
                enabled: true,
                mode: "primary",
                primary: { id: "primary", connection: { connectionString: PRIMARY_URI } },
                replicas: [{ id: "replica-0", connection: { connectionString: REPLICA_URI } }],
            },
        });
        const health = await db.replication.health();
        expect(health.primary.status).toBe("up");
        const replica = health.replicas.find((r) => r.endpointId === "replica-0");
        expect(replica).toBeDefined();
        expect(replica.status).toBe("up");
        // On a real streaming replica, lag is measured (not unknown).
        expect(replica.lagUnknown).toBe(false);
        expect(typeof replica.lagMs).toBe("number");
        await db.shutdown();
    });

    it("routes writes to the primary and never silently to a replica", async () => {
        if (!hasPrimary) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }
        const db = createDatabase({
            connectionString: PRIMARY_URI,
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: { id: "primary", connection: { connectionString: PRIMARY_URI } },
                replicas: [{ id: "replica-0", connection: { connectionString: REPLICA_URI } }],
            },
        });
        const write = await db.query(
            "INSERT INTO repl_integration_tick (n) VALUES ($1) RETURNING id",
            [42],
        );
        expect(write.rows.length).toBe(1);
        expect(typeof write.rows[0].id).toBe("number");
        await db.shutdown();
    });

    it("transactions remain pinned to the primary", async () => {
        if (!hasPrimary) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }
        const db = createDatabase({
            connectionString: PRIMARY_URI,
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: { id: "primary", connection: { connectionString: PRIMARY_URI } },
                replicas: [{ id: "replica-0", connection: { connectionString: REPLICA_URI } }],
            },
        });
        const res = await db.transaction(async (client) => {
            const r = await client.query(
                "INSERT INTO repl_integration_tick (n) VALUES ($1) RETURNING id",
                [7],
            );
            return r.rows[0].id;
        });
        expect(typeof res).toBe("number");
        await db.shutdown();
    });

    it("replica-only throws ReplicaUnavailableError when no eligible replica", async () => {
        if (!hasPrimary) {
            console.log("Skipping: no PostgreSQL available");
            return;
        }
        const downDb = createDatabase({
            connectionString: PRIMARY_URI,
            replication: {
                enabled: true,
                mode: "replica-preferred",
                primary: { id: "primary", connection: { connectionString: PRIMARY_URI } },
                replicas: [],
            },
        });
        await expect(() =>
            downDb.replication.readEndpoint("replica-only"),
        ).toThrow(ReplicaUnavailableError);
        await downDb.shutdown().catch(() => {});
    });
});
