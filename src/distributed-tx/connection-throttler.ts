import type { Logger } from "../types.js";

export interface ConnectionThrottlerOptions {
    maxConnectionsPerTransaction?: number;
    logger?: Logger;
}

export class ConnectionThrottler {
    private readonly max: number;
    private readonly usage = new Map<string, number>();
    private readonly logger?: Logger;

    constructor(options: ConnectionThrottlerOptions = {}) {
        this.max = options.maxConnectionsPerTransaction ?? 100;
        this.logger = options.logger;
    }

    async acquire(transactionId: string): Promise<void> {
        const current = this.usage.get(transactionId) ?? 0;
        if (current >= this.max) {
            throw new Error(`Connection limit reached for transaction ${transactionId}`);
        }
        this.usage.set(transactionId, current + 1);
    }

    release(transactionId: string): void {
        const current = this.usage.get(transactionId) ?? 0;
        if (current <= 1) this.usage.delete(transactionId);
        else this.usage.set(transactionId, current - 1);
    }

    getActiveConnections(transactionId: string): number {
        return this.usage.get(transactionId) ?? 0;
    }

    canAcquire(transactionId: string): boolean {
        const current = this.usage.get(transactionId) ?? 0;
        return current < this.max;
    }
}

export { ConnectionThrottler as ConnectionThrottleManager };
