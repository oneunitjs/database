import type { FailoverState, ShardFailoverState, HaNodeInfo } from "./failover-types.js";

/**
 * Types of failover hooks that can be registered.
 */
export type FailoverHookType =
    | "beforeStateChange"
    | "afterStateChange"
    | "beforeFailover"
    | "afterFailover"
    | "failoverFailed"
    | "beforePromotion"
    | "afterPromotion"
    | "promotionFailed"
    | "beforeDemotion"
    | "afterDemotion"
    | "demotionFailed"
    | "beforeHealthCheck"
    | "afterHealthCheck"
    | "onPrimaryChange"
    | "onTopologyChange"
    | "onFencingActivated"
    | "onFencingReleased";

/**
 * Hook data for each hook type.
 */
export interface FailoverHookData {
    beforeStateChange: { fromState: ShardFailoverState; toState: ShardFailoverState; reason: string };
    afterStateChange: { fromState: ShardFailoverState; toState: ShardFailoverState; reason: string };
    beforeFailover: { currentPrimary: HaNodeInfo | undefined; reason: string };
    afterFailover: { newPrimary: HaNodeInfo | undefined; reason: string };
    failoverFailed: { reason: string; state: FailoverState };
    beforePromotion: { replicaId: string };
    afterPromotion: { newPrimary: HaNodeInfo; previousPrimary: HaNodeInfo | undefined };
    promotionFailed: { reason: string; replicaId?: string };
    beforeDemotion: { currentPrimary: HaNodeInfo | undefined; reason: string };
    afterDemotion: { newPrimary: HaNodeInfo | undefined; reason: string };
    demotionFailed: { reason: string };
    beforeHealthCheck: { nodeId: string };
    afterHealthCheck: { nodeId: string; healthy: boolean; durationMs: number };
    onPrimaryChange: { previousPrimary: string | undefined; newPrimary: string | undefined };
    onTopologyChange: { previousGeneration: number; newGeneration: number };
    onFencingActivated: { nodeId: string; reason: string };
    onFencingReleased: { nodeId: string };
}

/**
 * Context passed to a hook invocation.
 */
export interface FailoverHookContext<T extends FailoverHookType = FailoverHookType> {
    type: T;
    shardId: string;
    timestamp: Date;
    manager: FailoverManagerHookAPI;
    data: FailoverHookData[T];
}

/**
 * A failover hook callback.
 */
export type FailoverHook<T extends FailoverHookType = FailoverHookType> = (
    context: FailoverHookContext<T>,
) => Promise<void> | void;

/**
 * Read-only API exposed to hooks, mirroring safe FailoverManager surface.
 */
export interface FailoverManagerHookAPI {
    getCurrentState(): FailoverState;
    getCurrentPrimary(): string | undefined;
    getCurrentGeneration(): number;
    getShardId(): string;
}

/**
 * Map of hook type to array of hooks.
 */
export type FailoverHooks = {
    [K in FailoverHookType]?: FailoverHook<K>[];
};

/**
 * Logger type compatible with FailoverManager's RequiredFailoverLogger.
 */
export type RequiredFailoverLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

/**
 * Registers and invokes failover hooks. Hook errors are caught and logged
 * so a failing hook never breaks failover operations.
 */
export class FailoverHookRunner {
    private hooks: FailoverHooks = {};

    constructor(private readonly logger: RequiredFailoverLogger) {}

    /**
     * Invoke all hooks registered for a given type.
     */
    async invoke<T extends FailoverHookType>(
        type: T,
        context: Omit<FailoverHookContext<T>, "type" | "timestamp">,
    ): Promise<void> {
        const fullContext: FailoverHookContext<T> = { ...context, type, timestamp: new Date() };
        const key = type as keyof FailoverHooks;
        const typeHooks = this.hooks[key];
        if (!typeHooks || typeHooks.length === 0) return;

        this.logger.debug({ hookType: type, hookCount: typeHooks.length }, "Invoking failover hooks");

        const errors: Error[] = [];
        for (const hook of typeHooks) {
            try {
                await Promise.resolve((hook as FailoverHook<T>)(fullContext));
            } catch (error) {
                errors.push(error instanceof Error ? error : new Error(String(error)));
            }
        }

        if (errors.length > 0) {
            this.logger.warn({ hookType: type, errorCount: errors.length }, "Some hooks failed");
        }
    }

    /**
     * Register a hook for a given type.
     */
    registerHook<T extends FailoverHookType>(type: T, hook: FailoverHook<T>): void {
        const key = type as keyof FailoverHooks;
        if (!this.hooks[key]) {
            this.hooks[key] = [];
        }
        this.hooks[key]!.push(hook as FailoverHook<FailoverHookType>);
    }

    /**
     * Unregister a previously registered hook.
     */
    unregisterHook<T extends FailoverHookType>(type: T, hook: FailoverHook<T>): boolean {
        const key = type as keyof FailoverHooks;
        const typeHooks = this.hooks[key];
        if (!typeHooks) return false;
        const index = typeHooks.indexOf(hook as FailoverHook<FailoverHookType>);
        if (index === -1) return false;
        typeHooks.splice(index, 1);
        return true;
    }

    /**
     * Get the count of hooks registered for a type (for testing/introspection).
     */
    getHookCount(type: FailoverHookType): number {
        const key = type as keyof FailoverHooks;
        return this.hooks[key]?.length ?? 0;
    }

    /**
     * Clear all hooks.
     */
    clear(): void {
        this.hooks = {};
    }
}

/**
 * Default no-op logger for use when no logger is provided.
 */
export const defaultHookLogger: RequiredFailoverLogger = {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
};
