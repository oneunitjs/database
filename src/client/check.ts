import type { Pool } from "pg";
import type { CheckApi } from "../types.js";
import { performance } from "node:perf_hooks";

function poolStats(pool: Pool | undefined): { total: number; idle: number; waiting: number } | undefined {
    if (typeof pool?.totalCount !== "number") {
        return undefined;
    }

    return {
        total: pool.totalCount,
        idle: pool.idleCount,
        waiting: pool.waitingCount,
    };
}

export function createCheck(pool: Pool): CheckApi {
    async function check(options: { timeout?: number } = {}): Promise<{
        status: "up" | "down";
        latency: { value: number; unit: "ms" };
        pool?: { total: number; idle: number; waiting: number };
        error?: string;
        code?: string;
    }> {
        const start = performance.now();

        try {
            await pool.query("SELECT 1");

            return {
                status: "up",
                latency: {
                    value: Math.round(performance.now() - start),
                    unit: "ms",
                },
                pool: poolStats(pool),
            };
        } catch (error) {
            return {
                status: "down",
                latency: {
                    value: Math.round(performance.now() - start),
                    unit: "ms",
                },
                error: error instanceof Error ? error.message : String(error),
                code: (error as { code?: string }).code,
                pool: poolStats(pool),
            };
        }
    }

    /**
     * Richer health payload suitable for readiness endpoints. Always resolves;
     * it never throws so callers can report `down` without extra try/catch.
     */
    async function health(options: { timeout?: number } = {}): Promise<{
        status: "up" | "down";
        healthy: boolean;
        checkedAt: string;
        latency: { value: number; unit: "ms" };
        pool?: { total: number; idle: number; waiting: number };
        error: string | null;
        errorCode: string | null;
    }> {
        const result = await check(options);

        return {
            ...result,
            healthy: result.status === "up",
            checkedAt: new Date().toISOString(),
            error: result.error ?? null,
            errorCode: result.code ?? null,
        };
    }

    return {
        check,
        health,
    };
}