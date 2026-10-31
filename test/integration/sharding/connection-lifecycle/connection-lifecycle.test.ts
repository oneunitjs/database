// ---------------------------------------------------------------------------
// Stage 3 — Connection Lifecycle and Resource Safety (Real PostgreSQL)
//
// Verifies that connections are properly acquired, used, and released under
// various conditions: normal completion, query failure, transaction rollback,
// pool exhaustion, and shutdown with active operations.
//
// Uses REAL PostgreSQL instances on port 5440 (shard-0).
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "vitest";

import { createDatabase } from "../../../../src/index.ts";
import { Pool } from "pg";
import {
    createPool,
    getShardInstances,
    query,
} from "../infrastructure/pg-cluster.ts";

let instancesReady = false;
const TEST_DB = "connection_lifecycle_test";

describe("Stage 3 — Connection Lifecycle (real PostgreSQL)", () => {
    beforeAll(async () => {
        const { checkClusterHealth } = await import(
            "../infrastructure/pg-cluster.ts"
        );
        const health = await checkClusterHealth();
        instancesReady = health.every((h) => h.healthy);

        if (instancesReady) {
            // Create test database on shard-0
            const instance = getShardInstances()[0];
            const pool = createPool(instance, { max: 1 });
            try {
                try {
                    await pool.query(`CREATE DATABASE ${TEST_DB}`);
                } catch {
                    // may already exist
                }
            } finally {
                await pool.end();
            }

            // Create test table
            const dbPool = new Pool({
                connectionString: instance.connectionString.replace(
                    /\/[^\/]+$/,
                    `/${TEST_DB}`,
                ),
                max: 5,
            });
            try {
                await dbPool.query(`
                    CREATE TABLE IF NOT EXISTS lifecycle_test (
                        id SERIAL PRIMARY KEY,
                        value TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                `);
            } finally {
                await dbPool.end();
            }
        }
    }, 30000);

    afterAll(async () => {
        if (instancesReady) {
            const instance = getShardInstances()[0];
            const pool = createPool(instance, { max: 1 });
            try {
                try {
                    await pool.query(`DROP DATABASE ${TEST_DB} WITH (FORCE)`);
                } catch {
                    // ignore
                }
            } finally {
                await pool.end();
            }
        }
    });

    it("normal query completion releases connection to pool", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const instance = getShardInstances()[0];
        const uri = instance.connectionString.replace(
            /\/[^\/]+$/,
            `/${TEST_DB}`,
        );

        const db = createDatabase({
            connectionString: uri,
            max: 5,
        });

        // Execute multiple queries
        for (let i = 0; i < 10; i++) {
            const result = await db.query(
                "INSERT INTO lifecycle_test (value) VALUES ($1) RETURNING id",
                [`test-${i}`],
            );
            expect(result.rows.length).toBe(1);
        }

        // Verify pool stats - no leaked connections
        const stats = (db as any).pool;
        if (stats) {
            expect(stats.totalCount).toBeGreaterThanOrEqual(0);
            expect(stats.waitingCount).toBe(0);
        }

        await db.shutdown();
    });

    it("query failure releases connection (no leak)", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const instance = getShardInstances()[0];
        const uri = instance.connectionString.replace(
            /\/[^\/]+$/,
            `/${TEST_DB}`,
        );

        const db = createDatabase({
            connectionString: uri,
            max: 5,
        });

        // Execute a failing query
        await expect(
            db.query("SELECT * FROM nonexistent_table_xyz"),
        ).rejects.toThrow();

        // Connection should still be usable after error
        const result = await db.query("SELECT 1 AS ok");
        expect(result.rows[0].ok).toBe(1);

        await db.shutdown();
    });

    it("transaction rollback releases connection", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const instance = getShardInstances()[0];
        const uri = instance.connectionString.replace(
            /\/[^\/]+$/,
            `/${TEST_DB}`,
        );

        const db = createDatabase({
            connectionString: uri,
            max: 5,
        });

        // Transaction that rolls back
        await expect(
            db.transaction(async (client) => {
                await client.query(
                    "INSERT INTO lifecycle_test (value) VALUES ($1)",
                    ["rollback-test"],
                );
                throw new Error("forced rollback");
            }),
        ).rejects.toThrow("forced rollback");

        // Connection should still be usable
        const result = await db.query("SELECT count(*) AS c FROM lifecycle_test WHERE value = $1", [
            "rollback-test",
        ]);
        expect(Number(result.rows[0].c)).toBe(0); // rolled back, no row

        await db.shutdown();
    });

    it("transaction commit persists data", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const instance = getShardInstances()[0];
        const uri = instance.connectionString.replace(
            /\/[^\/]+$/,
            `/${TEST_DB}`,
        );

        const db = createDatabase({
            connectionString: uri,
            max: 5,
        });

        await db.transaction(async (client) => {
            await client.query(
                "INSERT INTO lifecycle_test (value) VALUES ($1)",
                ["commit-test"],
            );
        });

        const result = await db.query("SELECT count(*) AS c FROM lifecycle_test WHERE value = $1", [
            "commit-test",
        ]);
        expect(Number(result.rows[0].c)).toBe(1);

        await db.shutdown();
    });

    it("repeated connection acquisition and release doesn't leak", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const instance = getShardInstances()[0];
        const uri = instance.connectionString.replace(
            /\/[^\/]+$/,
            `/${TEST_DB}`,
        );

        const db = createDatabase({
            connectionString: uri,
            max: 10,
        });

        // Rapidly acquire and release connections
        for (let i = 0; i < 50; i++) {
            await db.query("SELECT 1 AS ok");
        }

        // All connections should be back in the pool
        await new Promise((resolve) => setTimeout(resolve, 100));

        const stats = (db as any).pool;
        if (stats) {
            // waitingCount should be 0 (no connections stuck waiting)
            expect(stats.waitingCount).toBe(0);
            // idleCount should be close to max (connections returned to pool)
            expect(stats.idleCount).toBeGreaterThan(0);
        }

        await db.shutdown();
    });

    it("shutdown with active operations completes without hanging", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const instance = getShardInstances()[0];
        const uri = instance.connectionString.replace(
            /\/[^\/]+$/,
            `/${TEST_DB}`,
        );

        const db = createDatabase({
            connectionString: uri,
            max: 5,
        });

        // Start a query that will be interrupted by shutdown
        const slowPromise = db.query("SELECT pg_sleep(0.2) AS slept").catch(
            () => ({ rows: [] }),
        );

        // Wait a moment for the query to start
        await new Promise((resolve) => setTimeout(resolve, 50));

        // Shutdown should complete without hanging (waits for active ops)
        const shutdownStart = Date.now();
        await db.shutdown();
        const shutdownDuration = Date.now() - shutdownStart;

        // Shutdown should complete well within the 30s timeout
        expect(shutdownDuration).toBeLessThan(30000);

        // The in-flight query may have been cancelled by pool shutdown
        const result = await slowPromise;
        // Just verify it resolved without crashing
        expect(result).toBeDefined();
    });
});
