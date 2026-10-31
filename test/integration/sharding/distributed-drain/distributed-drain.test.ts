import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, unlinkSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { ShardManager } from "../../../../src/sharding/shard-manager.ts";
import { Pool } from "pg";

const SHARD_PORT = 5440;
const SHARD1_PORT = 5441;
const COORD_PORT = 5443;
const HERE = dirname(fileURLToPath(import.meta.url));

const coordination = {
    host: "127.0.0.1",
    port: COORD_PORT,
    user: "postgres",
    database: "coordination",
};

const shardConfigs = [
    {
        id: "shard-0",
        connection: {
            host: "127.0.0.1",
            port: SHARD_PORT,
            user: "postgres",
            database: "shard0",
        },
    },
    {
        id: "shard-1",
        connection: {
            host: "127.0.0.1",
            port: SHARD1_PORT,
            user: "postgres",
            database: "shard1",
        },
    },
];

const RESULT_DIR = "/tmp/kilo/sharding-integration/results";
const RESULT_A = join(RESULT_DIR, "drain-app-a.json");
const RESULT_B = join(RESULT_DIR, "drain-app-b.json");

let instancesReady = false;

function runApp(scriptName: string, resultFile: string): Promise<{ code: number | null; output: string }> {
    return new Promise((resolve) => {
        const tsxBin = join(process.cwd(), "node_modules/.bin/tsx");
        const child = spawn(tsxBin, [scriptName, resultFile], {
            cwd: process.cwd(),
            stdio: ["pipe", "pipe", "pipe"],
            env: { ...process.env },
        });

        let stdout = "";
        let stderr = "";
        child.stdout?.on("data", (d) => (stdout += d.toString()));
        child.stderr?.on("data", (d) => (stderr += d.toString()));

        const timer = setTimeout(() => {
            child.kill("SIGTERM");
        }, 25000);

        child.on("close", (code) => {
            clearTimeout(timer);
            resolve({ code, output: stdout + stderr });
        });
    });
}

function readResult(file: string): any {
    if (!existsSync(file)) return null;
    return JSON.parse(readFileSync(file, "utf-8"));
}

function poolFor(port: number, database: string): Pool {
    return new Pool({
        host: "127.0.0.1",
        port,
        user: "postgres",
        database,
        max: 1,
        connectionTimeoutMillis: 3000,
    });
}

async function resetDrainState() {
    const coord = poolFor(COORD_PORT, "coordination");
    try {
        await coord.query("DELETE FROM shard_active_ops").catch(() => {});
        await coord.query("DELETE FROM shard_drain_state").catch(() => {});
    } finally {
        await coord.end().catch(() => {});
    }
    for (const [port, database] of [
        [SHARD_PORT, "shard0"],
        [SHARD1_PORT, "shard1"],
    ] as const) {
        const pool = poolFor(port, database);
        try {
            await pool.query("DROP TABLE IF EXISTS shard_active_ops").catch(() => {});
            await pool.query("DROP TABLE IF EXISTS shard_drain_state").catch(() => {});
        } finally {
            await pool.end().catch(() => {});
        }
    }
}

describe("Stage 3 — Distributed Drain (multi-process, real PostgreSQL)", () => {
    beforeAll(async () => {
        const { checkClusterHealth } = await import(
            "../infrastructure/pg-cluster.ts"
        );
        const health = await checkClusterHealth();
        instancesReady = health.every((h) => h.healthy);
        if (!instancesReady) {
            console.log("WARNING: Infrastructure not ready:", health);
        }
        mkdirSync(RESULT_DIR, { recursive: true });
        try { unlinkSync(RESULT_A); } catch {}
        try { unlinkSync(RESULT_B); } catch {}
        if (instancesReady) await resetDrainState();
    }, 30000);

    afterAll(async () => {
        if (instancesReady) await resetDrainState();
    }, 15000);

    it("distributed drain waits for active operation from another instance", async () => {
        if (!instancesReady) {
            console.log("Skipping: infrastructure not ready");
            return;
        }

        const appAPromise = runApp(join(HERE, "drain-app-a.ts"), RESULT_A);
        const appBPromise = runApp(join(HERE, "drain-app-b.ts"), RESULT_B);
        const [resultAraw, resultBraw] = await Promise.all([appAPromise, appBPromise]);

        const resultA = readResult(RESULT_A);
        const resultB = readResult(RESULT_B);

        console.log("Application A result:", JSON.stringify(resultA, null, 2));
        console.log("Application B result:", JSON.stringify(resultB, null, 2));
        if (!resultA) console.log("App A process:", resultAraw);
        if (!resultB) console.log("App B process:", resultBraw);

        expect(resultA?.acquired).toBe(true);
        expect(resultA?.released).toBe(true);
        expect(resultA?.error).toBeNull();

        expect(resultB?.drainStarted).toBe(true);
        expect(resultB?.drainTimeout).toBe(true);
        expect(resultB?.drainCompleted).toBe(false);
        expect(resultB?.error).toMatch(/still in flight/);
        expect(resultB?.activeOpsWhenDrainStarted).toBeGreaterThan(0);

        const manager = new ShardManager(shardConfigs, undefined, undefined, undefined, {
            instanceId: "verify",
            coordinationMode: "strict",
            coordination,
        });
        try {
            const finalOps = manager.getActiveOps("shard-0");
            expect(finalOps).toBe(0);
            expect(manager.hasShard("shard-0")).toBe(true);
        } finally {
            await manager.shutdown();
        }

        const coordPool = poolFor(COORD_PORT, "coordination");
        const shard1Pool = poolFor(SHARD1_PORT, "shard1");
        try {
            const coordTables = await coordPool.query(
                `SELECT to_regclass('public.shard_drain_state') AS drain,
                        to_regclass('public.shard_active_ops') AS ops`,
            );
            expect(coordTables.rows[0]?.drain).toBe("shard_drain_state");
            expect(coordTables.rows[0]?.ops).toBe("shard_active_ops");

            const shard1Tables = await shard1Pool.query(
                `SELECT to_regclass('public.shard_drain_state') AS drain,
                        to_regclass('public.shard_active_ops') AS ops`,
            );
            expect(shard1Tables.rows[0]?.drain).toBeNull();
            expect(shard1Tables.rows[0]?.ops).toBeNull();
        } finally {
            await coordPool.end().catch(() => {});
            await shard1Pool.end().catch(() => {});
        }
    }, 60000);

    it("shard removal respects drain state across instances", async () => {
        if (!instancesReady) {
            console.log("Skipping: infrastructure not ready");
            return;
        }

        const manager = new ShardManager(shardConfigs, undefined, undefined, undefined, {
            instanceId: "verify-2",
            coordinationMode: "strict",
            abandonedOperationTimeoutMs: 5000,
            coordination,
        });

        try {
            expect(manager.hasShard("shard-1")).toBe(true);
            expect(manager.hasShard("shard-0")).toBe(true);
        } finally {
            await manager.shutdown();
        }
    }, 30000);
});
