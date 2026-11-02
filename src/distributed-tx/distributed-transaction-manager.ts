import type { Logger } from "../types.js";
import type { DistributedTransactionCoordinator } from "./distributed-transaction-coordinator.js";
import { TransactionSemaphore } from "./transaction-semaphore.js";
import { CircuitBreaker } from "./circuit-breaker.js";
import { PoolMonitor } from "../client/pool-monitor.js";

export interface DistributedTransactionManagerOptions {
    maxConcurrentTransactions?: number;
    poolMonitor: PoolMonitor;
    semaphoreOptions?: any;
    circuitBreakerOptions?: any;
    logger?: Logger;
}

export class DistributedTransactionManager {
    private readonly coordinator: DistributedTransactionCoordinator;
    private readonly semaphore: TransactionSemaphore;
    private readonly circuitBreaker: CircuitBreaker;
    private readonly poolMonitor: PoolMonitor;
    private readonly logger?: Logger;

    constructor(coordinator: DistributedTransactionCoordinator, options: DistributedTransactionManagerOptions) {
        this.coordinator = coordinator;
        this.poolMonitor = options.poolMonitor;
        this.logger = options.logger;
        this.semaphore = new TransactionSemaphore({ ...options.semaphoreOptions, maxConcurrent: options.maxConcurrentTransactions ?? 5, logger: options.logger });
        this.circuitBreaker = new CircuitBreaker(options.poolMonitor, { ...options.circuitBreakerOptions, logger: options.logger });
        this.circuitBreaker.start();
    }

    async transaction(callback: (tx: any) => Promise<any>, options?: any) {
        if (!this.circuitBreaker.canExecute()) throw new Error("Distributed transactions blocked: pool exhaustion detected");
        const release = await this.semaphore.acquire(`tx-${Date.now()}`);
        try {
            const result = await (this.coordinator as any).transaction(callback, options);
            this.circuitBreaker.reportSuccess();
            return result;
        } catch (error) {
            this.circuitBreaker.reportFailure();
            throw error;
        } finally {
            release();
        }
    }

    getSemaphore() { return this.semaphore; }
    getCircuitBreaker() { return this.circuitBreaker; }
    getPoolMonitor() { return this.poolMonitor; }
    getStatus() {
        return {
            activeTransactions: this.semaphore.getActiveCount(),
            queuedTransactions: this.semaphore.getQueueLength(),
            circuitState: this.circuitBreaker.getState(),
            averagePoolUtilization: this.poolMonitor.getAverageUtilization(),
            maxPoolUtilization: this.poolMonitor.getMaxUtilization(),
        };
    }
    shutdown() { this.circuitBreaker.stop(); }
}
