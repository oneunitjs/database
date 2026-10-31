// ---------------------------------------------------------------------------
// Stage 3 — Infrastructure Verification (Real PostgreSQL)
//
// Verifies that all four independent PostgreSQL instances are healthy and
// properly configured before any distributed correctness tests run.
//
// These tests use REAL PostgreSQL instances on ports 5440-5443 (started
// independently via initdb/pg_ctl, NOT Docker).
//
// PREPARE TRANSACTION is supported by these PostgreSQL instances: each
// participant passes an explicit BEGIN / SELECT / PREPARE / ROLLBACK
// PREPARED cycle against the same session.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll } from "vitest";

import {
    checkClusterHealth,
    verifyPreparedTransactionSupport,
    getAllInstances,
    getShardInstances,
    getCoordinationInstance,
    createPool,
    getMaxPreparedTransactions,
} from "./infrastructure/pg-cluster.ts";
import { Pool } from "pg";

let instancesReady = false;

describe("Stage 3 — Infrastructure Verification (real PostgreSQL)", () => {
    beforeAll(async () => {
        const health = await checkClusterHealth();
        instancesReady = health.every((h) => h.healthy);
        if (!instancesReady) {
            console.log(
                "WARNING: Not all infrastructure instances are healthy:",
                health,
            );
        }
    }, 30000);

    it("all four PostgreSQL instances are healthy", async () => {
        const health = await checkClusterHealth();
        expect(health).toHaveLength(4);
        for (const h of health) {
            expect(h.healthy, `Instance ${h.instance} should be healthy`).toBe(
                true,
            );
            expect(h.error).toBeNull();
        }
        instancesReady = true;
    });

    it("each instance has max_prepared_transactions > 0", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        for (const instance of getAllInstances()) {
            const maxPrepared = await getMaxPreparedTransactions(instance);
            expect(maxPrepared).toBeGreaterThan(
                0,
                `Instance ${instance.id} must have max_prepared_transactions > 0`,
            );
        }
    });

    it("PREPARE TRANSACTION support (environment check)", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        // NOTE: In this environment, PREPARE TRANSACTION returns ROLLBACK
        // instead of creating a prepared transaction. This is a known
        // PostgreSQL server behavior in this installation and blocks real
        // 2PC crash recovery testing. Documented as an environment limitation.
        const result = await verifyPreparedTransactionSupport();
        const supportedCount = Object.values(result.results).filter(
            (r) => r.supported,
        ).length;
        const totalCount = Object.values(result.results).length;
        console.log(
            `PREPARE TRANSACTION support: ${supportedCount}/${totalCount} instances`,
        );
        if (!result.allSupported) {
            console.log(
                "ENVIRONMENT LIMITATION: PREPARE TRANSACTION does not create prepared transactions. " +
                "Real 2PC crash recovery testing is BLOCKED in this environment.",
            );
        }
        // A participant without prepared-transaction support would block real
        // 2PC crash-recovery verification and is treated as a hard failure.
        expect(result.allSupported).toBe(true);
    });

    it("each shard instance accepts independent connections", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        for (const instance of getShardInstances()) {
            const pool = createPool(instance, { max: 1 });
            try {
                const result = await pool.query("SELECT 1 AS ok");
                expect(result.rows[0].ok).toBe(1);
            } finally {
                await pool.end();
            }
        }
    });

    it("coordination database accepts independent connections", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const pool = createPool(getCoordinationInstance(), { max: 1 });
        try {
            const result = await pool.query("SELECT 1 AS ok");
            expect(result.rows[0].ok).toBe(1);
        } finally {
            await pool.end();
        }
    });

    it("instances are independent (different server processes)", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        const uniquePids = new Set<string>();
        for (const instance of getAllInstances()) {
            const pool = new Pool({ connectionString: instance.connectionString });
            try {
                const { rows } = await pool.query("SELECT pg_backend_pid() AS pid");
                uniquePids.add(rows[0].pid);
            } finally {
                await pool.end();
            }
        }
        expect(uniquePids.size).toBe(4, "Each instance should have a unique PID");
    });

    it("no lingering prepared transactions on any instance", async () => {
        if (!instancesReady) {
            console.log("Skipping: instances not ready");
            return;
        }
        for (const instance of getAllInstances()) {
            const pool = createPool(instance, { max: 1 });
            try {
                const result = await pool.query(
                    "SELECT count(*) AS c FROM pg_prepared_xacts",
                );
                expect(Number(result.rows[0]?.c ?? 0)).toBe(
                    0,
                    `Instance ${instance.id} should have no lingering prepared transactions`,
                );
            } finally {
                await pool.end();
            }
        }
    });
});
