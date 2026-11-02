import { Pool } from "pg";

export const ADMIN_CONFIG = { user: "postgres", database: "postgres", host: "/run/postgresql", max: 5 };

let adminPool: Pool | null = null;
let dbCounter = 0;

export function getAdminPool(): Pool {
    if (!adminPool) {
        adminPool = new Pool(ADMIN_CONFIG);
    }
    return adminPool;
}

export function testDbConfig(dbName: string) {
    return { user: "postgres", database: dbName, host: "/run/postgresql", max: 5 };
}

export async function createIsolatedDb(admin: Pool, prefix: string): Promise<string> {
    dbCounter += 1;
    const suffix = `${Date.now()}-${dbCounter}-${Math.random().toString(36).slice(2, 10)}`;
    const dbName = `${prefix}_${suffix}`.replace(/[^a-z0-9_]/gi, "_").slice(0, 60);
    await admin.query(`CREATE DATABASE "${dbName}"`);
    return dbName;
}

export async function dropDatabase(admin: Pool, dbName: string): Promise<void> {
    try {
        await admin.query(
            `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
            [dbName],
        );
    } catch {
        // ignore
    }
    try {
        await admin.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    } catch {
        // ignore
    }
}

export async function endPoolSafely(pool: Pool): Promise<void> {
    try {
        await pool.end();
    } catch {
        // ignore
    }
}

/**
 * Creates an isolated database, runs a test with a dedicated pool, then drops
 * the database and ends the pool. Guarantees cleanup even on failure.
 */
export async function withIsolatedDb<T>(
    prefix: string,
    fn: (pool: Pool, dbName: string) => Promise<T>,
): Promise<T> {
    const admin = getAdminPool();
    const dbName = await createIsolatedDb(admin, prefix);
    const pool = new Pool(testDbConfig(dbName));
    try {
        return await fn(pool, dbName);
    } finally {
        await endPoolSafely(pool);
        await dropDatabase(admin, dbName);
    }
}

/**
 * Create several isolated databases on the same admin connection.
 */
export async function createIsolatedDbs(admin: Pool, prefix: string, count: number): Promise<string[]> {
    const names: string[] = [];
    for (let i = 0; i < count; i++) {
        names.push(await createIsolatedDb(admin, `${prefix}_${i}`));
    }
    return names;
}

export async function dropDatabases(admin: Pool, dbNames: string[]): Promise<void> {
    for (const name of dbNames) {
        await dropDatabase(admin, name);
    }
}

/** Wait for PostgreSQL to notice a terminated backend and release its locks. */
export async function waitForBackendDeath(ms = 1500): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
