import type { PoolClient } from "pg";

export interface ParticipantHealthOptions {
    intervalMs: number;
    timeoutMs: number;
    healthCheckQuery: string;
    maxConsecutiveFailures?: number;
}

export const DEFAULT_PARTICIPANT_HEALTH_OPTIONS: Required<ParticipantHealthOptions> = {
    intervalMs: 10_000,
    timeoutMs: 5_000,
    healthCheckQuery: "SELECT 1",
    maxConsecutiveFailures: 3,
};

export interface ParticipantHealthStatus {
    participantId: string;
    shardId: string;
    healthy: boolean;
    lastCheckTime: Date | null;
    lastSuccessTime: Date | null;
    lastFailureTime: Date | null;
    consecutiveFailures: number;
    consecutiveSuccesses: number;
    latencyMs?: number;
    error?: string;
}

export class ParticipantHealthChecker {
    private readonly statuses = new Map<string, ParticipantHealthStatus>();
    private healthCheckInterval?: ReturnType<typeof setInterval>;

    constructor(
        private readonly participants: Map<string, { shardId: string; client: PoolClient }> = new Map(),
        private readonly logger: ParticipantHealthLogger,
        private readonly options: Required<ParticipantHealthOptions> = DEFAULT_PARTICIPANT_HEALTH_OPTIONS,
    ) {
        for (const [id, p] of participants) {
            this.statuses.set(id, this.createInitialStatus(id, p.shardId));
        }
    }

    start(): void {
        if (this.healthCheckInterval) {
            return;
        }
        this.healthCheckInterval = setInterval(() => this.checkAllParticipants(), this.options.intervalMs);
        if (this.healthCheckInterval.unref) {
            this.healthCheckInterval.unref();
        }
    }

    async checkAllParticipants(): Promise<void> {
        for (const [id, participant] of this.participants) {
            await this.checkParticipant(id, participant);
        }
    }

    stop(): void {
        if (this.healthCheckInterval) {
            clearInterval(this.healthCheckInterval);
            this.healthCheckInterval = undefined;
        }
    }

    async checkParticipant(id: string, participant: { shardId: string; client: PoolClient }): Promise<void> {
        const status = this.statuses.get(id) ?? this.createInitialStatus(id, participant.shardId);
        const start = Date.now();
        try {
            const client = participant.client;
            await Promise.race([
                client.query(this.options.healthCheckQuery),
                new Promise<never>((_, reject) => {
                    setTimeout(
                        () => reject(new Error("Health check timeout after " + this.options.timeoutMs + "ms")),
                        this.options.timeoutMs,
                    );
                }),
            ]);
            status.healthy = true;
            status.lastCheckTime = new Date();
            status.lastSuccessTime = new Date();
            status.consecutiveSuccesses++;
            status.consecutiveFailures = 0;
            status.latencyMs = Date.now() - start;
            status.error = undefined;
        } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            status.healthy = false;
            status.lastCheckTime = new Date();
            status.lastFailureTime = new Date();
            status.consecutiveFailures++;
            status.consecutiveSuccesses = 0;
            status.error = msg;
        }
        this.statuses.set(id, status);
    }

    isHealthy(id: string): boolean {
        return this.statuses.get(id)?.healthy ?? true;
    }

    getUnhealthyParticipants(): string[] {
        return Array.from(this.statuses.entries())
            .filter(([, s]) => !s.healthy)
            .map(([id]) => id);
    }

    getStatus(id: string): ParticipantHealthStatus | undefined {
        return this.statuses.get(id);
    }

    getAllStatuses(): Map<string, ParticipantHealthStatus> {
        return new Map(this.statuses);
    }

    private createInitialStatus(id: string, shardId: string): ParticipantHealthStatus {
        return {
            participantId: id,
            shardId,
            healthy: true,
            lastCheckTime: null,
            lastSuccessTime: null,
            lastFailureTime: null,
            consecutiveFailures: 0,
            consecutiveSuccesses: 0,
        };
    }
}

export type ParticipantHealthLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

export const defaultParticipantHealthLogger: ParticipantHealthLogger = {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
};
