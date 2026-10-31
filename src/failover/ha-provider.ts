import type { HaProvider, HaNodeInfo, HaNodeRole, HaNodeStatus, HaTopology, FailoverProviderConfig } from "./failover-types.js";

/**
 * Static HA provider — no external HA controller.
 * The shard's configured connection is always treated as the primary.
 * No automatic promotion is possible; failover requires operator intervention.
 * This provider preserves backward compatibility for users who do not run
 * Patroni or any external HA system.
 */
export class StaticHaProvider implements HaProvider {
    readonly name = "static";
    readonly supportsPromotion = false;
    readonly supportsTopologyGeneration = false;
    readonly supportsReplicationLag = false;

    private readonly shardPrimary: Map<string, HaNodeInfo> = new Map();

    constructor(
        private readonly shardConfigs: Map<string, { id: string; host?: string; port?: number; database?: string; user?: string; connectionString?: string }>,
        private readonly config: FailoverProviderConfig = {},
    ) {
        for (const [shardId, cfg] of shardConfigs) {
            this.shardPrimary.set(shardId, {
                id: shardId,
                role: "primary",
                status: "up",
                lagMs: null,
                lagUnknown: true,
                lastCheckedAt: new Date().toISOString(),
                consecutiveFailures: 0,
                connection: {
                    host: cfg.host,
                    port: cfg.port,
                    database: cfg.database,
                    user: cfg.user,
                    connectionString: cfg.connectionString,
                },
            });
        }
    }

    async initialize(): Promise<void> {
        // Nothing to create — static topology
    }

    async discoverTopology(shardId: string): Promise<HaTopology> {
        const primary = this.shardPrimary.get(shardId);
        return {
            generation: 1,
            primary,
            replicas: [],
            clusterId: undefined,
            capturedAt: new Date().toISOString(),
        };
    }

    async checkNodeHealth(_nodeId: string, _connection: HaNodeInfo["connection"]): Promise<HaNodeStatus> {
        // Static provider cannot perform real health checks without a connection pool.
        // Return "unknown" so the failover manager uses its own connection-based checks.
        return "unknown";
    }

    async shutdown(): Promise<void> {
        this.shardPrimary.clear();
    }
}

/**
 * Patroni HA provider — integrates with a Patroni cluster via its REST API.
 * Requires a running Patroni cluster with the REST API exposed.
 * This provider supports automatic promotion, topology generation, and
 * replication lag inspection.
 */
export class PatroniHaProvider implements HaProvider {
    readonly name = "patroni";
    readonly supportsPromotion = true;
    readonly supportsTopologyGeneration = true;
    readonly supportsReplicationLag = true;

    private readonly baseUrl: string;

    constructor(baseUrl: string, private readonly config: FailoverProviderConfig = {}) {
        this.baseUrl = baseUrl.replace(/\/$/, "");
    }

    async initialize(): Promise<void> {
        // Verify Patroni is reachable
        await this._fetchJson(`/`);
    }

    async discoverTopology(shardId: string): Promise<HaTopology> {
        // Patroni manages a single PostgreSQL cluster; the shardId maps to
        // a Patroni cluster scope. We query /primary and /replicas.
        const primary = await this._fetchJson<{ name: string; host: string; port: number; role: string; state: string; lag?: number }>("/primary");
        const members = await this._fetchJson<{ name: string; host: string; port: number; role: string; state: string; lag?: number }[]>("/replicas");

        const primaryNode: HaNodeInfo | undefined = primary
            ? {
                  id: primary.name,
                  role: "primary" as HaNodeRole,
                  status: primary.state === "running" ? "up" : "down",
                  lagMs: null,
                  lagUnknown: true,
                  lastCheckedAt: new Date().toISOString(),
                  consecutiveFailures: 0,
                  connection: {
                      host: primary.host,
                      port: primary.port,
                      connectionString: `postgresql://postgres@${primary.host}:${primary.port}/postgres`,
                  },
              }
            : undefined;

        const replicas: HaNodeInfo[] = (members ?? []).map((m) => ({
            id: m.name,
            role: "replica" as HaNodeRole,
            status: m.state === "running" ? "up" : "down",
            lagMs: m.lag ?? null,
            lagUnknown: m.lag === undefined,
            lastCheckedAt: new Date().toISOString(),
            consecutiveFailures: 0,
            connection: {
                host: m.host,
                port: m.port,
                connectionString: `postgresql://postgres@${m.host}:${m.port}/postgres`,
            },
        }));

        return {
            generation: 1, // Patroni does not expose a generation number directly
            primary: primaryNode,
            replicas,
            clusterId: shardId,
            capturedAt: new Date().toISOString(),
        };
    }

    async checkNodeHealth(_nodeId: string, _connection: HaNodeInfo["connection"]): Promise<HaNodeStatus> {
        try {
            await this._fetchJson(`/`);
            return "up";
        } catch {
            return "down";
        }
    }

    async promoteReplica(shardId: string, replicaId: string): Promise<HaNodeInfo> {
        // Patroni: POST /failover with the candidate leader
        await this._fetchJson(`/failover`, { method: "POST", body: JSON.stringify({ candidate: replicaId }) });
        // After promotion, fetch the new topology
        const topology = await this.discoverTopology(shardId);
        if (!topology.primary) {
            throw new Error(`Patroni promotion of "${replicaId}" did not produce a primary`);
        }
        return topology.primary;
    }

    async verifyPrimary(nodeId: string, _expectedGeneration: number): Promise<boolean> {
        try {
            const primary = await this._fetchJson<{ name: string; state: string }>(`/primary`);
            return primary.name === nodeId && primary.state === "running";
        } catch {
            return false;
        }
    }

    async fenceOldPrimary(_oldPrimaryId: string): Promise<boolean> {
        // Patroni handles fencing automatically via leader leases and
        // DCS (etcd/Consul/Kubernetes). Return true — Patroni is authoritative.
        return true;
    }

    async getTopologyGeneration(_shardId: string): Promise<number> {
        // Patroni does not expose a monotonic generation. Return 1.
        return 1;
    }

    async shutdown(): Promise<void> {
        // Nothing to clean up
    }

    private async _fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
        const url = `${this.baseUrl}${path}`;
        const response = await fetch(url, init);
        if (!response.ok) {
            throw new Error(`Patroni API ${path} returned ${response.status}`);
        }
        return (await response.json()) as T;
    }
}
