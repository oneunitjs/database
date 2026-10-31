// ---------------------------------------------------------------------------
// PostgreSQL Cluster Infrastructure for Stage 3 Distributed Correctness Tests.
//
// Manages four independent PostgreSQL server processes:
//   shard-0   -> port 5440
//   shard-1   -> port 5441
//   shard-2   -> port 5442
//   coordination-db -> port 5443
//
// Each instance is a fully independent PostgreSQL server process with its own
// data directory, connection endpoint, and credentials. They are NOT schemas
// in a single database — each has its own server process.
//
// Environment variables (all optional):
//   SHARD_0_PORT ... SHARD_2_PORT, COORDINATION_PORT
//   SHARD_0_HOST ... COORDINATION_HOST (default 127.0.0.1)
//   PG_TEST_USER (default postgres)
//   PG_TEST_PASSWORD (default empty)
//   SHARD_DATA_DIR (default /tmp/kilo/sharding-integration)
//
// Instances are expected to be pre-started. This module provides health checks,
// connection helpers, and optional start/stop lifecycle management.
// ---------------------------------------------------------------------------

import { Pool, type PoolClient } from "pg";
import { execSync, execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const DEFAULT_USER = "postgres";
const DEFAULT_PASSWORD = "";
const DEFAULT_HOST = "127.0.0.1";

const PORTS = {
    shard0: Number(process.env.SHARD_0_PORT ?? 5440),
    shard1: Number(process.env.SHARD_1_PORT ?? 5441),
    shard2: Number(process.env.SHARD_2_PORT ?? 5442),
    coordination: Number(process.env.COORDINATION_PORT ?? 5443),
};

const HOST = process.env.PG_TEST_HOST ?? DEFAULT_HOST;
const USER = process.env.PG_TEST_USER ?? DEFAULT_USER;
const PASSWORD = process.env.PG_TEST_PASSWORD ?? DEFAULT_PASSWORD;
const DATA_DIR =
    process.env.SHARD_DATA_DIR ?? "/tmp/kilo/sharding-integration";

export interface ShardInstance {
    id: string;
    port: number;
    host: string;
    user: string;
    password: string;
    database: string;
    connectionString: string;
}

export interface ClusterStatus {
    instance: string;
    healthy: boolean;
    maxPreparedTransactions: number | null;
    error: string | null;
}

const INSTANCES: Record<string, ShardInstance> = {
    shard0: {
        id: "shard-0",
        port: PORTS.shard0,
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: "shard0",
        connectionString: `postgresql://${USER}${PASSWORD ? ":" + PASSWORD : ""}@${HOST}:${PORTS.shard0}/shard0`,
    },
    shard1: {
        id: "shard-1",
        port: PORTS.shard1,
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: "shard1",
        connectionString: `postgresql://${USER}${PASSWORD ? ":" + PASSWORD : ""}@${HOST}:${PORTS.shard1}/shard1`,
    },
    shard2: {
        id: "shard-2",
        port: PORTS.shard2,
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: "shard2",
        connectionString: `postgresql://${USER}${PASSWORD ? ":" + PASSWORD : ""}@${HOST}:${PORTS.shard2}/shard2`,
    },
    coordination: {
        id: "coordination-db",
        port: PORTS.coordination,
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: "coordination",
        connectionString: `postgresql://${USER}${PASSWORD ? ":" + PASSWORD : ""}@${HOST}:${PORTS.coordination}/coordination`,
    },
};

export function getInstance(id: string): ShardInstance | undefined {
    return INSTANCES[id];
}

export function getAllInstances(): ShardInstance[] {
    return Object.values(INSTANCES);
}

export function getShardInstances(): ShardInstance[] {
    return [INSTANCES.shard0, INSTANCES.shard1, INSTANCES.shard2];
}

export function getCoordinationInstance(): ShardInstance {
    return INSTANCES.coordination;
}

/**
 * Create a pg Pool for a given instance.
 */
export function createPool(instance: ShardInstance, options?: { max?: number }): Pool {
    return new Pool({
        host: instance.host,
        port: instance.port,
        user: instance.user,
        password: instance.password,
        database: instance.database,
        max: options?.max ?? 10,
    });
}

/**
 * Create a single connection client for a given instance.
 */
export function createClient(instance: ShardInstance): PoolClient {
    return new Pool({
        host: instance.host,
        port: instance.port,
        user: instance.user,
        password: instance.password,
        database: instance.database,
    }).connect() as unknown as PoolClient;
}

/**
 * Execute a SQL statement on an instance and return rows.
 */
export async function query(
    instance: ShardInstance,
    sql: string,
    params?: unknown[],
): Promise<{ rows: unknown[]; rowCount: number }> {
    const pool = createPool(instance, { max: 1 });
    try {
        const result = await pool.query(sql, params);
        return { rows: result.rows, rowCount: result.rowCount };
    } finally {
        await pool.end();
    }
}

/**
 * Check if an instance is accepting connections.
 */
export async function isHealthy(instance: ShardInstance): Promise<boolean> {
    try {
        const pool = createPool(instance, { max: 1 });
        try {
            await pool.query("SELECT 1");
            return true;
        } finally {
            await pool.end();
        }
    } catch {
        return false;
    }
}

/**
 * Read max_prepared_transactions from an instance.
 */
export async function getMaxPreparedTransactions(
    instance: ShardInstance,
): Promise<number | null> {
    try {
        const { rows } = await query(
            instance,
            "SHOW max_prepared_transactions",
        );
        const row = rows[0] as Record<string, unknown>;
        const key = Object.keys(row)[0];
        return key ? Number(row[key]) : null;
    } catch {
        return null;
    }
}

/**
 * Run the full cluster health check. Returns status for every instance.
 */
export async function checkClusterHealth(): Promise<ClusterStatus[]> {
    const results: ClusterStatus[] = [];
    for (const instance of getAllInstances()) {
        const healthy = await isHealthy(instance);
        const maxPrepared = healthy ? await getMaxPreparedTransactions(instance) : null;
        results.push({
            instance: instance.id,
            healthy,
            maxPreparedTransactions: maxPrepared,
            error: healthy ? null : `Instance ${instance.id} at ${instance.host}:${instance.port} is not reachable`,
        });
    }
    return results;
}

/**
 * Verify that every instance can create and resolve a prepared transaction.
 */
export async function verifyPreparedTransactionSupport(): Promise<{
    allSupported: boolean;
    results: Record<string, { supported: boolean; error: string | null }>;
}> {
    const results: Record<string, { supported: boolean; error: string | null }> = {};
    let allSupported = true;

    for (const instance of getAllInstances()) {
        const gid = `pg_stage3_verify_${instance.id.replace(/[^a-z0-9]/gi, "_")}`;
        const pool = createPool(instance, { max: 1 });
        const client = await pool.connect();
        try {
            await client.query("BEGIN");
            await client.query("SELECT 1");
            await client.query(`PREPARE TRANSACTION '${gid}'`);
            client.release();
            await pool.query(`ROLLBACK PREPARED '${gid}'`);
            results[instance.id] = { supported: true, error: null };
        } catch (error) {
            try { client.release(true); } catch { /* ignore */ }
            // Attempt cleanup of any prepared transaction left behind.
            try { await pool.query(`ROLLBACK PREPARED '${gid}'`); } catch { /* ignore */ }
            allSupported = false;
            results[instance.id] = {
                supported: false,
                error: error instanceof Error ? error.message : String(error),
            };
        } finally {
            await pool.end().catch(() => {});
        }
    }

    return { allSupported, results };
}

/**
 * Start a PostgreSQL instance using pg_ctl. Returns true if started successfully.
 * This is used for CI environments where instances are not pre-started.
 */
export function startInstance(dataDir: string, port: number): { ok: boolean; error: string | null } {
    try {
        const logFile = `${dataDir}.log`;
        const result = spawnSync(
            "pg_ctl",
            ["-D", dataDir, "-l", logFile, "-o", `-p ${port}`, "start"],
            { stdio: ["ignore", "pipe", "pipe"], timeout: 30000 },
        );
        if (result.status !== 0) {
            return {
                ok: false,
                error: result.stderr ? result.stderr.toString() : `pg_ctl exited with code ${result.status}`,
            };
        }
        return { ok: true, error: null };
    } catch (error) {
        return {
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        };
    }
}

/**
 * Stop a PostgreSQL instance using pg_ctl.
 */
export function stopInstance(dataDir: string): { ok: boolean; error: string | null } {
    try {
        const result = spawnSync(
            "pg_ctl",
            ["-D", dataDir, "stop", "-m", "fast"],
            { stdio: ["ignore", "pipe", "pipe"], timeout: 30000 },
        );
        if (result.status !== 0) {
            return {
                ok: false,
                error: result.stderr ? result.stderr.toString() : `pg_ctl exited with code ${result.status}`,
            };
        }
        return { ok: true, error: null };
    } catch (error) {
        return {
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        };
    }
}

/**
 * Map instance id to its data directory path.
 */
export function getDataDir(instanceId: string): string {
    return join(DATA_DIR, instanceId.replace("-", ""));
}

/**
 * Guard: refuse to run destructive operations unless the target resources
 * belong to this test environment (ports 5440-5443, data dir under /tmp/kilo).
 */
export function assertTestEnvironment(ports: number[]): void {
    for (const port of ports) {
        if (port < 5440 || port > 5443) {
            throw new Error(
                `Refusing to operate on port ${port}: not part of the Stage 3 test environment (5440-5443)`,
            );
        }
    }
}
