export interface ReplicaSelectionEvent {
    timestamp: Date;
    replicaId: string;
    policy: string;
    success: boolean;
    latencyMs?: number;
    error?: string;
}

export interface ReplicaSuccessRate {
    total: number;
    success: number;
    rate: number;
}

export interface ReplicaLatencyStats {
    totalMs: number;
    count: number;
    avgMs: number;
}

export interface ReplicaSelectionStats {
    totalSelections: number;
    byReplica: Map<string, number>;
    byPolicy: Map<string, number>;
    successRateByReplica: Map<string, ReplicaSuccessRate>;
    avgLatencyByReplica: Map<string, ReplicaLatencyStats>;
    lastSelectionByReplica: Map<string, Date>;
    errorCountByReplica: Map<string, number>;
}

const DEFAULT_MAX_EVENTS = 10_000;

export class ReplicaSelectionStatsTracker {
    private readonly events: ReplicaSelectionEvent[] = [];
    private readonly maxEvents: number;

    constructor(maxEvents: number = DEFAULT_MAX_EVENTS) {
        this.maxEvents = Number.isFinite(maxEvents) && maxEvents > 0 ? Math.floor(maxEvents) : DEFAULT_MAX_EVENTS;
    }

    recordSelection(event: ReplicaSelectionEvent): void {
        this.events.push(event);
        if (this.events.length > this.maxEvents) {
            this.events.splice(0, this.events.length - this.maxEvents);
        }
    }

    getStats(): ReplicaSelectionStats {
        const stats: ReplicaSelectionStats = {
            totalSelections: this.events.length,
            byReplica: new Map(),
            byPolicy: new Map(),
            successRateByReplica: new Map(),
            avgLatencyByReplica: new Map(),
            lastSelectionByReplica: new Map(),
            errorCountByReplica: new Map(),
        };

        for (const event of this.events) {
            stats.byReplica.set(event.replicaId, (stats.byReplica.get(event.replicaId) ?? 0) + 1);
            stats.byPolicy.set(event.policy, (stats.byPolicy.get(event.policy) ?? 0) + 1);
            stats.lastSelectionByReplica.set(event.replicaId, event.timestamp);

            let replicaSuccess = stats.successRateByReplica.get(event.replicaId);
            if (!replicaSuccess) {
                replicaSuccess = { total: 0, success: 0, rate: 0 };
                stats.successRateByReplica.set(event.replicaId, replicaSuccess);
            }
            replicaSuccess.total += 1;
            if (event.success) {
                replicaSuccess.success += 1;
            }
            replicaSuccess.rate = replicaSuccess.total > 0 ? replicaSuccess.success / replicaSuccess.total : 0;

            if (typeof event.latencyMs === "number" && Number.isFinite(event.latencyMs)) {
                let replicaLatency = stats.avgLatencyByReplica.get(event.replicaId);
                if (!replicaLatency) {
                    replicaLatency = { totalMs: 0, count: 0, avgMs: 0 };
                    stats.avgLatencyByReplica.set(event.replicaId, replicaLatency);
                }
                replicaLatency.totalMs += event.latencyMs;
                replicaLatency.count += 1;
                replicaLatency.avgMs = replicaLatency.count > 0 ? replicaLatency.totalMs / replicaLatency.count : 0;
            }

            if (!event.success) {
                stats.errorCountByReplica.set(
                    event.replicaId,
                    (stats.errorCountByReplica.get(event.replicaId) ?? 0) + 1,
                );
            }
        }

        return stats;
    }

    getRecentEvents(limit: number = 100): ReplicaSelectionEvent[] {
        const n = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 100;
        return this.events.slice(-n);
    }

    getEventsForReplica(replicaId: string, limit: number = 100): ReplicaSelectionEvent[] {
        const n = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 100;
        return this.events.filter((e) => e.replicaId === replicaId).slice(-n);
    }

    getEventsForPolicy(policy: string, limit: number = 100): ReplicaSelectionEvent[] {
        const n = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 100;
        return this.events.filter((e) => e.policy === policy).slice(-n);
    }

    clear(): void {
        this.events.length = 0;
    }
}
