/**
 * Tracks in-flight connections per replica endpoint so least-connections
 * selection can pick the least loaded replica.
 */
export class ReplicationConnectionTracker {
    private readonly connectionCounts = new Map<string, number>();

    acquireConnection(endpointId: string): void {
        if (!endpointId) {
            return;
        }
        this.connectionCounts.set(endpointId, (this.connectionCounts.get(endpointId) ?? 0) + 1);
    }

    releaseConnection(endpointId: string): void {
        if (!endpointId) {
            return;
        }
        const current = this.connectionCounts.get(endpointId) ?? 0;
        const next = Math.max(0, current - 1);
        if (next === 0) {
            this.connectionCounts.delete(endpointId);
            return;
        }
        this.connectionCounts.set(endpointId, next);
    }

    getConnectionCount(endpointId: string): number {
        return this.connectionCounts.get(endpointId) ?? 0;
    }

    getAllConnectionCounts(): Map<string, number> {
        return new Map(this.connectionCounts);
    }

    reset(): void {
        this.connectionCounts.clear();
    }
}
