// ---------------------------------------------------------------------------
// Test Helpers for Stage 3 Distributed Correctness Tests.
//
// Provides:
//   - Test database creation/destruction on each shard
//   - Schema fixtures (accounts, events, outbox tables)
//   - Seed data helpers
//   - Independent row-count verification across shards
//   - Multi-process application harness for distributed drain tests
// ---------------------------------------------------------------------------

import { Pool } from "pg";
import {
    createPool,
    getAllInstances,
    getShardInstances,
    getCoordinationInstance,
    getInstance,
    query,
} from "../infrastructure/pg-cluster.js";

const TEST_DB_PREFIX = "stage3_test_";

/**
 * Create a test database on every shard instance and the coordination instance.
 * Safe to call multiple times (idempotent).
 */
export async function createTestDatabases(
    dbName: string,
): Promise<void> {
    const pool = createPool(getCoordinationInstance(), { max: 1 });
    try {
        try {
            await pool.query(`CREATE DATABASE ${dbName}`);
        } catch {
            // may already exist
        }
    } finally {
        await pool.end();
    }

    for (const instance of getShardInstances()) {
        const p = createPool(instance, { max: 1 });
        try {
            try {
                await p.query(`CREATE DATABASE ${dbName}`);
            } catch {
                // may already exist
            }
        } finally {
            await p.end();
        }
    }
}

/**
 * Drop a test database from every instance.
 */
export async function dropTestDatabases(dbName: string): Promise<void> {
    for (const instance of getAllInstances()) {
        const p = createPool(instance, { max: 1 });
        try {
            try {
                await p.query(`DROP DATABASE ${dbName} WITH (FORCE)`);
            } catch {
                // ignore
            }
        } finally {
            await p.end();
        }
    }
}

/**
 * Get a connection string for a test database on a given instance.
 */
export function testDbUri(instanceId: string, dbName: string): string {
    const instance = getInstance(instanceId);
    if (!instance) {
        throw new Error(`Unknown instance: ${instanceId}`);
    }
    // Replace the database name in the connection string
    return instance.connectionString.replace(
        /\/[^\/]+$/,
        `/${dbName}`,
    );
}

/**
 * Create the standard test schema on a database connection.
 */
export async function createTestSchema(
    connectionString: string,
): Promise<void> {
    const pool = new Pool({ connectionString });
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS accounts (
                id BIGSERIAL PRIMARY KEY,
                owner TEXT NOT NULL,
                balance BIGINT NOT NULL DEFAULT 0,
                shard_key TEXT NOT NULL,
                version BIGINT NOT NULL DEFAULT 1
            )
        `);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                shard_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        `);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS outbox (
                id BIGSERIAL PRIMARY KEY,
                shard_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB,
                processed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        `);
        await pool.query(
            `CREATE INDEX IF NOT EXISTS idx_accounts_shard_key ON accounts(shard_key)`,
        );
        await pool.query(
            `CREATE INDEX IF NOT EXISTS idx_events_shard_key ON events(shard_key)`,
        );
        await pool.query(
            `CREATE INDEX IF NOT EXISTS idx_outbox_shard_key ON outbox(shard_key)`,
        );
    } finally {
        await pool.end();
    }
}

/**
 * Seed deterministic accounts across shards.
 * Each account has a stable id, owner, balance, shard_key, and version.
 */
export async function seedAccounts(
    connectionString: string,
    accounts: Array<{ id: number; owner: string; balance: number; shardKey: string }>,
): Promise<void> {
    const pool = new Pool({ connectionString });
    try {
        for (const acc of accounts) {
            await pool.query(
                `INSERT INTO accounts (id, owner, balance, shard_key, version)
                 VALUES ($1, $2, $3, $4, 1)
                 ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, shard_key = EXCLUDED.shard_key, version = accounts.version + 1`,
                [acc.id, acc.owner, acc.balance, acc.shardKey],
            );
        }
    } finally {
        await pool.end();
    }
}

/**
 * Count accounts on a database.
 */
export async function countAccounts(
    connectionString: string,
): Promise<number> {
    const pool = new Pool({ connectionString });
    try {
        const result = await pool.query("SELECT count(*) AS c FROM accounts");
        return Number(result.rows[0]?.c ?? 0);
    } finally {
        await pool.end();
    }
}

/**
 * Sum balances on a database.
 */
export async function sumBalances(
    connectionString: string,
): Promise<number> {
    const pool = new Pool({ connectionString });
    try {
        const result = await pool.query("SELECT coalesce(sum(balance), 0) AS s FROM accounts");
        return Number(result.rows[0]?.s ?? 0);
    } finally {
        await pool.end();
    }
}

/**
 * Get all accounts on a database.
 */
export async function getAllAccounts(
    connectionString: string,
): Promise<Array<{ id: number; owner: string; balance: number; shard_key: string; version: number }>> {
    const pool = new Pool({ connectionString });
    try {
        const result = await pool.query(
            `SELECT id, owner, balance, shard_key, version FROM accounts ORDER BY id`,
        );
        return result.rows.map((r) => ({
            id: Number(r.id),
            owner: r.owner,
            balance: Number(r.balance),
            shard_key: r.shard_key,
            version: Number(r.version),
        }));
    } finally {
        await pool.end();
    }
}

/**
 * Verify that no unexpected rows exist (data integrity check).
 */
export async function verifyNoExtraRows(
    connectionString: string,
    expectedIds: number[],
): Promise<{ ok: boolean; extraIds: number[] }> {
    const pool = new Pool({ connectionString });
    try {
        const result = await pool.query("SELECT id FROM accounts ORDER BY id");
        const actualIds = result.rows.map((r) => Number(r.id));
        const expectedSet = new Set(expectedIds);
        const extraIds = actualIds.filter((id) => !expectedSet.has(id));
        return { ok: extraIds.length === 0, extraIds };
    } finally {
        await pool.end();
    }
}

/**
 * Spawn a separate Node.js application process for multi-instance testing.
 * The process runs a test script file and reports results via stdout JSON lines.
 *
 * Options:
 *   script - path to the application script to run
 *   env - additional environment variables
 *   timeout - milliseconds to wait before killing the process
 *
 * Returns a ProcessHandle with methods to interact with the process.
 */
export interface ProcessHandle {
    pid: number;
    stdout: string;
    stderr: string;
    isRunning: () => boolean;
    stop: (signal?: "SIGTERM" | "SIGKILL") => Promise<void>;
    kill: () => Promise<void>;
    wait: (timeoutMs?: number) => Promise<{ code: number | null; signal: string | null }>;
    onOutput: (line: string) => void;
    outputLines: string[];
}

export function spawnAppProcess(
    scriptPath: string,
    options: {
        env?: Record<string, string>;
        timeoutMs?: number;
        onOutput?: (line: string) => void;
    } = {},
): ProcessHandle {
    const env = { ...process.env, ...options.env };
    const child = spawnSync("node", [scriptPath], {
        env,
        stdio: ["pipe", "pipe", "pipe", "ipc"],
        timeout: options.timeoutMs,
    }) as unknown as {
        pid: number;
        stdout: Buffer;
        stderr: Buffer;
        status: number | null;
        signal: string | null;
        error: Error | null;
    };

    // Note: spawnSync is blocking. For true async process management,
    // we use child_process.spawn instead. This helper is kept for simple
    // fire-and-forget scenarios. For distributed tests requiring interaction,
    // use spawnAppProcessAsync instead.
    return {
        pid: child.pid ?? -1,
        stdout: child.stdout?.toString() ?? "",
        stderr: child.stderr?.toString() ?? "",
        isRunning: () => child.status === null,
        stop: async () => Promise.resolve(),
        kill: async () => Promise.resolve(),
        wait: async () => ({ code: child.status, signal: child.signal }),
        onOutput: () => {},
        outputLines: (child.stdout?.toString() ?? "").split("\n").filter(Boolean),
    };
}

/**
 * Asynchronously spawn a separate Node.js application process.
 * Uses child_process.spawn for true async process management.
 */
import { spawn, type ChildProcess } from "node:child_process";

export function spawnAppProcessAsync(
    scriptPath: string,
    options: {
        env?: Record<string, string>;
        onOutput?: (line: string) => void;
    } = {},
): ProcessHandle & { process: ChildProcess } {
    const env = { ...process.env, ...options.env };
    const child = spawn("node", [scriptPath], {
        env,
        stdio: ["pipe", "pipe", "pipe", "ipc"],
    });

    let stdout = "";
    let stderr = "";
    const outputLines: string[] = [];
    let resolveStop: (() => void) | null = null;
    const stopPromise = new Promise<void>((res) => {
        resolveStop = res;
    });

    child.stdout?.on("data", (chunk) => {
        const text = chunk.toString();
        stdout += text;
        for (const line of text.split("\n")) {
            if (line.trim()) {
                outputLines.push(line.trim());
                options.onOutput?.(line.trim());
            }
        }
    });

    child.stderr?.on("data", (chunk) => {
        stderr += chunk.toString();
    });

    let stopped = false;
    const handle: ProcessHandle & { process: ChildProcess } = {
        pid: child.pid,
        get stdout() { return stdout; },
        get stderr() { return stderr; },
        isRunning: () => !stopped && child.exitCode === null && !child.killed,
        stop: async (signal = "SIGTERM") => {
            if (stopped) return;
            stopped = true;
            return new Promise<void>((res) => {
                child.once("exit", () => res());
                child.kill(signal);
            });
        },
        kill: async () => {
            if (stopped) return;
            stopped = true;
            return new Promise<void>((res) => {
                child.once("exit", () => res());
                child.kill("SIGKILL");
            });
        },
        wait: async (timeoutMs = 30000) => {
            return new Promise<{ code: number | null; signal: string | null }>((res) => {
                const timer = setTimeout(() => {
                    child.kill("SIGKILL");
                    res({ code: child.exitCode, signal: child.signal });
                }, timeoutMs);
                child.once("exit", (code, signal) => {
                    clearTimeout(timer);
                    res({ code, signal });
                });
            });
        },
        onOutput: (line: string) => {},
        outputLines,
        process: child,
    };

    // Wire up the stop promise resolution
    child.once("exit", () => {
        if (resolveStop) resolveStop();
    });

    return handle;
}

/**
 * Write a JSON result file that a child process can use to report results
 * back to the parent test.
 */
export function writeResultFile(filePath: string, data: unknown): void {
    const fs = await import("node:fs");
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
}

/**
 * Read a result file written by a child process.
 */
export function readResultFile<T = unknown>(filePath: string): T | null {
    const fs = await import("node:fs");
    try {
        return JSON.parse(fs.readFileSync(filePath, "utf-8")) as T;
    } catch {
        return null;
    }
}

/**
 * Wait for a condition to become true, polling at intervals.
 */
export async function waitFor(
    condition: () => boolean | Promise<boolean>,
    options: {
        timeoutMs?: number;
        intervalMs?: number;
        message?: string;
    } = {},
): Promise<void> {
    const timeoutMs = options.timeoutMs ?? 30000;
    const intervalMs = options.intervalMs ?? 200;
    const start = Date.now();

    while (Date.now() - start < timeoutMs) {
        if (await condition()) {
            return;
        }
        await new Promise((res) => setTimeout(res, intervalMs));
    }

    throw new Error(
        options.message ?? `Condition not met within ${timeoutMs}ms`,
    );
}
