import type { Pool } from "pg";
import type { Logger } from "../types.js";
import { LsnUtils } from "./lsn-utils.js";

export interface ReplicationLagInfo {
    endpointId: string;
    replicaLsn: string;
    primaryLsn: string;
    lsnDiffBytes: bigint;
    lsnDiffFormatted: string;
    measuredAt: Date;
    lagMs?: number;
}

export interface WalTrackingOptions {
    checkIntervalMs: number;
    queryTimeoutMs: number;
    maxLsnLagBytes: bigint;
}

export const DEFAULT_WAL_TRACKING_OPTIONS: Required<WalTrackingOptions> = {
    checkIntervalMs: 10_000,
    queryTimeoutMs: 2_000,
    maxLsnLagBytes: 1024n * 1024n * 1024n,
};

export interface WalTrackableEndpoint {
    endpointId: string;
    role: "primary" | "replica";
    pool: Pool;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    });
    return Promise.race([promise, timeout]).finally(() => {
        if (timer) {
            clearTimeout(timer);
        }
    });
}

export class WalTracker {
    private readonly endpoints: Map<string, WalTrackableEndpoint>;
    private readonly logger?: Logger;
    private readonly options: Required<WalTrackingOptions>;
    private primaryLsn: string | null = null;
    private readonly replicaLsns = new Map<string, { lsn: string; updatedAt: Date }>();
    private trackingInterval?: ReturnType<typeof setInterval>;
    private tracking = false;

    constructor(
        endpoints: Iterable<WalTrackableEndpoint> | Map<string, WalTrackableEndpoint> = [],
        logger?: Logger,
        options: Partial<WalTrackingOptions> = {},
    ) {
        this.endpoints = endpoints instanceof Map ? new Map(endpoints) : new Map();
        if (!(endpoints instanceof Map)) {
            for (const endpoint of endpoints) {
                this.endpoints.set(endpoint.endpointId, endpoint);
            }
        }
        this.logger = logger;
        this.options = { ...DEFAULT_WAL_TRACKING_OPTIONS, ...options };
    }

    setEndpoint(endpoint: WalTrackableEndpoint): void {
        this.endpoints.set(endpoint.endpointId, endpoint);
    }

    removeEndpoint(endpointId: string): void {
        this.endpoints.delete(endpointId);
        this.replicaLsns.delete(endpointId);
    }

    start(): void {
        if (this.tracking) {
            return;
        }
        this.tracking = true;
        this.trackingInterval = setInterval(() => {
            void this.updateLagInfo();
        }, this.options.checkIntervalMs);
        if (typeof this.trackingInterval.unref === "function") {
            this.trackingInterval.unref();
        }
    }

    stop(): void {
        if (!this.tracking) {
            return;
        }
        this.tracking = false;
        if (this.trackingInterval) {
            clearInterval(this.trackingInterval);
            this.trackingInterval = undefined;
        }
    }

    get isTracking(): boolean {
        return this.tracking;
    }

    async updateLagInfo(): Promise<ReplicationLagInfo[]> {
        const results: ReplicationLagInfo[] = [];
        const now = new Date();

        const primaryEndpoint = Array.from(this.endpoints.values()).find((e) => e.role === "primary");
        if (primaryEndpoint) {
            try {
                this.primaryLsn = await this.getCurrentLsn(primaryEndpoint);
            } catch (error) {
                this.logger?.error?.(
                    {
                        endpointId: primaryEndpoint.endpointId,
                        error: error instanceof Error ? error.message : String(error),
                    },
                    "Failed to get primary LSN",
                );
            }
        }

        const replicaEndpoints = Array.from(this.endpoints.values()).filter((e) => e.role === "replica");
        for (const endpoint of replicaEndpoints) {
            try {
                const lsn = await this.getCurrentLsn(endpoint);
                this.replicaLsns.set(endpoint.endpointId, { lsn, updatedAt: now });
                if (this.primaryLsn) {
                    results.push(this.buildLagInfo(endpoint.endpointId, lsn, this.primaryLsn, now));
                }
            } catch (error) {
                this.logger?.error?.(
                    {
                        endpointId: endpoint.endpointId,
                        error: error instanceof Error ? error.message : String(error),
                    },
                    "Failed to get replica LSN",
                );
            }
        }
        return results;
    }

    private async getCurrentLsn(endpoint: WalTrackableEndpoint): Promise<string> {
        const query = endpoint.role === "primary" ? "SELECT pg_current_wal_lsn() AS lsn" : "SELECT pg_last_xact_replay_lsn() AS lsn";
        const result = await withTimeout(
            endpoint.pool.query<{ lsn: string | null }>(query),
            this.options.queryTimeoutMs,
            `LSN query timeout after ${this.options.queryTimeoutMs}ms`,
        );
        const lsn = result.rows[0]?.lsn;
        if (!lsn) {
            throw new Error(`LSN query returned empty result for ${endpoint.endpointId}`);
        }
        return lsn;
    }

    private buildLagInfo(
        endpointId: string,
        replicaLsn: string,
        primaryLsn: string,
        measuredAt: Date,
    ): ReplicationLagInfo {
        const lsnDiff = LsnUtils.lagBytes(primaryLsn, replicaLsn);
        return {
            endpointId,
            replicaLsn,
            primaryLsn,
            lsnDiffBytes: lsnDiff,
            lsnDiffFormatted: LsnUtils.formatBytes(lsnDiff),
            measuredAt,
        };
    }

    getLsn(endpointId: string): string | null {
        return this.replicaLsns.get(endpointId)?.lsn ?? null;
    }

    getPrimaryLsn(): string | null {
        return this.primaryLsn;
    }

    getLagInfo(endpointId: string): ReplicationLagInfo | null {
        const info = this.replicaLsns.get(endpointId);
        if (!info || !this.primaryLsn) {
            return null;
        }
        return this.buildLagInfo(endpointId, info.lsn, this.primaryLsn, info.updatedAt);
    }

    getAllLagInfo(): ReplicationLagInfo[] {
        if (!this.primaryLsn) {
            return [];
        }
        const results: ReplicationLagInfo[] = [];
        for (const [endpointId, info] of this.replicaLsns) {
            results.push(this.buildLagInfo(endpointId, info.lsn, this.primaryLsn, info.updatedAt));
        }
        return results;
    }

    hasExcessiveLag(): boolean {
        return this.getAllLagInfo().some((info) => info.lsnDiffBytes > this.options.maxLsnLagBytes);
    }

    getLaggingReplicas(): ReplicationLagInfo[] {
        return this.getAllLagInfo().filter((info) => info.lsnDiffBytes > this.options.maxLsnLagBytes);
    }
}
