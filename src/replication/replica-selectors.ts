import { ReplicationConnectionTracker } from "./replication-connection-tracker.js";

export type ReplicaSelectionPolicy = "first" | "weighted" | "round-robin" | "least-connections";

export interface SelectableReplica {
    endpointId: string;
    role: "primary" | "replica";
    weight?: number;
}

export interface ReplicaSelector {
    selectReplica(eligible: readonly SelectableReplica[]): SelectableReplica | null;
    selectReplicas(eligible: readonly SelectableReplica[], count: number): SelectableReplica[];
}

export interface WeightedReplicaSelectionOptions {
    defaultWeight: number;
    normalizeWeights: boolean;
    minWeight: number;
}

export const DEFAULT_WEIGHTED_OPTIONS: Required<WeightedReplicaSelectionOptions> = {
    defaultWeight: 1,
    normalizeWeights: false,
    minWeight: 0,
};

export class FirstEligibleReplicaSelector implements ReplicaSelector {
    selectReplica(eligible: readonly SelectableReplica[]): SelectableReplica | null {
        return eligible[0] ?? null;
    }

    selectReplicas(eligible: readonly SelectableReplica[], count: number): SelectableReplica[] {
        if (!Number.isFinite(count) || count <= 0) {
            return [];
        }
        return eligible.slice(0, count);
    }
}

export class WeightedReplicaSelector implements ReplicaSelector {
    private readonly options: Required<WeightedReplicaSelectionOptions>;
    private readonly random: () => number;

    constructor(
        options: Partial<WeightedReplicaSelectionOptions> = {},
        random: () => number = Math.random,
    ) {
        this.options = { ...DEFAULT_WEIGHTED_OPTIONS, ...options };
        this.random = random;
    }

    private weightOf(endpoint: SelectableReplica): number {
        const raw = endpoint.weight;
        if (typeof raw === "number" && Number.isFinite(raw) && raw >= 0) {
            return raw;
        }
        return this.options.defaultWeight;
    }

    selectReplica(eligible: readonly SelectableReplica[]): SelectableReplica | null {
        const weighted = eligible.filter((e) => this.weightOf(e) > this.options.minWeight);
        if (weighted.length === 0) {
            return eligible[0] ?? null;
        }
        const weights = weighted.map((e) => this.weightOf(e));
        const total = weights.reduce((sum, w) => sum + w, 0);
        if (total <= 0) {
            return weighted[0];
        }
        const random = this.random() * total;
        let cumulative = 0;
        for (let i = 0; i < weighted.length; i++) {
            cumulative += weights[i];
            if (random <= cumulative) {
                return weighted[i];
            }
        }
        return weighted[weighted.length - 1];
    }

    selectReplicas(eligible: readonly SelectableReplica[], count: number): SelectableReplica[] {
        if (!Number.isFinite(count) || count <= 0) {
            return [];
        }
        const selected: SelectableReplica[] = [];
        const seen = new Set<string>();
        const max = Math.min(count, eligible.length);
        for (let i = 0; i < max * 3 && selected.length < max; i++) {
            const replica = this.selectReplica(eligible);
            if (!replica || seen.has(replica.endpointId)) {
                continue;
            }
            seen.add(replica.endpointId);
            selected.push(replica);
        }
        return selected;
    }
}

export class RoundRobinReplicaSelector implements ReplicaSelector {
    private currentIndex = 0;

    selectReplica(eligible: readonly SelectableReplica[]): SelectableReplica | null {
        if (eligible.length === 0) {
            return null;
        }
        const selected = eligible[this.currentIndex % eligible.length];
        this.currentIndex += 1;
        if (this.currentIndex >= Number.MAX_SAFE_INTEGER) {
            this.currentIndex = 0;
        }
        return selected;
    }

    selectReplicas(eligible: readonly SelectableReplica[], count: number): SelectableReplica[] {
        if (!Number.isFinite(count) || count <= 0 || eligible.length === 0) {
            return [];
        }
        const replicas: SelectableReplica[] = [];
        const n = Math.min(count, eligible.length);
        for (let i = 0; i < n; i++) {
            const selected = this.selectReplica(eligible);
            if (selected) {
                replicas.push(selected);
            }
        }
        return replicas;
    }

    reset(): void {
        this.currentIndex = 0;
    }
}

export class LeastConnectionsReplicaSelector implements ReplicaSelector {
    constructor(private readonly connectionTracker: ReplicationConnectionTracker) {}

    selectReplica(eligible: readonly SelectableReplica[]): SelectableReplica | null {
        if (eligible.length === 0) {
            return null;
        }
        let best = eligible[0];
        let bestCount = this.connectionTracker.getConnectionCount(best.endpointId);
        for (let i = 1; i < eligible.length; i++) {
            const count = this.connectionTracker.getConnectionCount(eligible[i].endpointId);
            if (count < bestCount) {
                best = eligible[i];
                bestCount = count;
            }
        }
        return best;
    }

    selectReplicas(eligible: readonly SelectableReplica[], count: number): SelectableReplica[] {
        if (!Number.isFinite(count) || count <= 0 || eligible.length === 0) {
            return [];
        }
        return [...eligible]
            .sort(
                (a, b) =>
                    this.connectionTracker.getConnectionCount(a.endpointId) -
                    this.connectionTracker.getConnectionCount(b.endpointId),
            )
            .slice(0, Math.min(count, eligible.length));
    }
}

export function createReplicaSelector(
    policy: ReplicaSelectionPolicy,
    options: {
        weighted?: Partial<WeightedReplicaSelectionOptions>;
        connectionTracker?: ReplicationConnectionTracker;
        random?: () => number;
    } = {},
): ReplicaSelector {
    switch (policy) {
        case "weighted":
            return new WeightedReplicaSelector(options.weighted, options.random);
        case "round-robin":
            return new RoundRobinReplicaSelector();
        case "least-connections":
            return new LeastConnectionsReplicaSelector(
                options.connectionTracker ?? new ReplicationConnectionTracker(),
            );
        case "first":
        default:
            return new FirstEligibleReplicaSelector();
    }
}
