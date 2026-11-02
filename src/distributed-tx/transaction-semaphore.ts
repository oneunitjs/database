import type { Logger } from "../types.js";

export interface SemaphoreOptions {
    maxConcurrent?: number;
    timeoutMs?: number;
    logger?: Logger;
}

export const DEFAULT_SEMAPHORE_OPTIONS: Required<SemaphoreOptions> = {
    maxConcurrent: 5,
    timeoutMs: 30000,
    logger: undefined as any,
};

export class TransactionSemaphore {
    private readonly maxConcurrent: number;
    private readonly timeoutMs: number;
    private readonly logger?: Logger;
    private activeTransactions: Set<string> = new Set();
    private queue: Array<{ resolve: () => void; reject: (error: Error) => void; transactionId: string }> = [];

    constructor(options: SemaphoreOptions = {}) {
        const merged = { ...DEFAULT_SEMAPHORE_OPTIONS, ...options };
        this.maxConcurrent = merged.maxConcurrent;
        this.timeoutMs = merged.timeoutMs;
        this.logger = merged.logger;
    }

    async acquire(transactionId: string): Promise<() => void> {
        if (this.activeTransactions.size >= this.maxConcurrent) {
            return new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error(`Transaction semaphore timeout for ${transactionId}`));
                }, this.timeoutMs);
                this.queue.push({
                    resolve: () => {
                        clearTimeout(timeout);
                        resolve(this.release.bind(this, transactionId));
                    },
                    reject,
                    transactionId,
                });
            });
        }
        this.activeTransactions.add(transactionId);
        return this.release.bind(this, transactionId);
    }

    private release(transactionId: string): void {
        this.activeTransactions.delete(transactionId);
        const next = this.queue.shift();
        if (next) next.resolve();
    }

    getActiveCount(): number { return this.activeTransactions.size; }
    getQueueLength(): number { return this.queue.length; }
    isFull(): boolean { return this.activeTransactions.size >= this.maxConcurrent; }
    getActiveTransactions(): string[] { return Array.from(this.activeTransactions); }
}
