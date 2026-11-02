import type { Logger } from "../types.js";
import { PoolMonitor } from "../client/pool-monitor.js";

export interface CircuitBreakerOptions {
    openThreshold?: number;
    resetTimeoutMs?: number;
    checkIntervalMs?: number;
    logger?: Logger;
}

export const DEFAULT_CIRCUIT_BREAKER_OPTIONS: Required<CircuitBreakerOptions> = {
    openThreshold: 0.9,
    resetTimeoutMs: 30000,
    checkIntervalMs: 5000,
    logger: undefined as any,
};

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export class CircuitBreaker {
    private readonly poolMonitor: PoolMonitor;
    private readonly options: Required<CircuitBreakerOptions>;
    private state: CircuitState = "CLOSED";
    private openedAt = 0;
    private checkInterval?: ReturnType<typeof setInterval>;
    private readonly stateListeners: Set<(state: CircuitState) => void> = new Set();

    constructor(poolMonitor: PoolMonitor, options: CircuitBreakerOptions = {}) {
        this.poolMonitor = poolMonitor;
        this.options = { ...DEFAULT_CIRCUIT_BREAKER_OPTIONS, ...options } as Required<CircuitBreakerOptions>;
    }

    start(): void {
        if (this.checkInterval) return;
        this.checkInterval = setInterval(() => this.checkState(), this.options.checkIntervalMs);
    }

    stop(): void {
        if (this.checkInterval) {
            clearInterval(this.checkInterval);
            this.checkInterval = undefined;
        }
    }

    canExecute(): boolean {
        if (this.state === "CLOSED") {
            if (this.poolMonitor.isNearExhaustion(this.options.openThreshold)) {
                this.open();
                return false;
            }
            return true;
        }
        if (this.state === "OPEN") {
            if (Date.now() - this.openedAt >= this.options.resetTimeoutMs) {
                this.state = "HALF_OPEN";
                this.notifyStateChange();
            }
            return false;
        }
        return true;
    }

    private checkState(): void {
        if (this.state === "CLOSED") {
            if (this.poolMonitor.isNearExhaustion(this.options.openThreshold)) this.open();
        } else if (this.state === "OPEN") {
            if (Date.now() - this.openedAt >= this.options.resetTimeoutMs) this.state = "HALF_OPEN";
        }
    }

    private open(): void {
        this.state = "OPEN";
        this.openedAt = Date.now();
        this.notifyStateChange();
        this.options.logger?.warn?.(`Circuit breaker OPENED`);
    }

    close(): void { this.state = "CLOSED"; this.notifyStateChange(); }
    onStateChange(listener: (state: CircuitState) => void): () => void {
        this.stateListeners.add(listener);
        return () => this.stateListeners.delete(listener);
    }

    private notifyStateChange(): void {
        for (const l of this.stateListeners) {
            try { l(this.state); } catch { /* ignore */ }
        }
    }

    getState(): CircuitState { return this.state; }
    reportFailure(): void { if (this.state === "HALF_OPEN") this.open(); }
    reportSuccess(): void { if (this.state === "HALF_OPEN") this.close(); }
    forceClose(): void { this.state = "CLOSED"; this.openedAt = 0; this.notifyStateChange(); }
    forceOpen(): void { this.open(); }
}
