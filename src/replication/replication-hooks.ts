import type { ReplicaSelectionPolicy } from "./replica-selectors.js";
import type { ReplicaSelectionStats } from "./replication-stats.js";
import type { ReplicationEndpointHealth } from "../types.js";

export interface ReadRoutingEndpoint {
    endpointId: string;
    role: "primary" | "replica";
}

export type ReadRoutingHookType =
    | "beforeSelectReplica"
    | "afterSelectReplica"
    | "beforeQuery"
    | "afterQuery"
    | "onReplicaFailure"
    | "onReplicaSuccess";

export type ReadRoutingHookData = {
    beforeSelectReplica: { policy?: ReplicaSelectionPolicy | string; availableReplicas: string[] };
    afterSelectReplica: { policy?: ReplicaSelectionPolicy | string; selectedReplica: string | null };
    beforeQuery: { replica: ReadRoutingEndpoint };
    afterQuery: { replica: ReadRoutingEndpoint; durationMs: number; rowCount: number };
    onReplicaFailure: { replica: ReadRoutingEndpoint; error: Error; retryCount: number };
    onReplicaSuccess: { replica: ReadRoutingEndpoint; durationMs: number; rowCount: number };
};

export interface ReplicationRouterHookAPI {
    getAvailableReplicas(): string[];
    getCurrentPolicy(): string;
    getReplicaHealth(replicaId: string): ReplicationEndpointHealth | undefined;
    getSelectionStats(): ReplicaSelectionStats;
}

export interface ReadRoutingHookContext<T extends ReadRoutingHookType = ReadRoutingHookType> {
    type: T;
    query?: string;
    params?: unknown[];
    replica?: ReadRoutingEndpoint;
    timestamp: Date;
    router: ReplicationRouterHookAPI;
    data: ReadRoutingHookData[T];
}

export type ReadRoutingHook<T extends ReadRoutingHookType = ReadRoutingHookType> = (
    context: ReadRoutingHookContext<T>,
) => Promise<void> | void;

export type ReadRoutingHooks = {
    [K in ReadRoutingHookType]?: ReadRoutingHook<K>[];
};

export type ReadRoutingHookLogger = {
    debug?(payload: unknown, message?: string): void;
    warn?(payload: unknown, message?: string): void;
    error?(payload: unknown, message?: string): void;
};

export class ReadRoutingHookRunner {
    private hooks: ReadRoutingHooks = {};

    constructor(
        hooks: ReadRoutingHooks = {},
        private readonly logger: ReadRoutingHookLogger = {},
    ) {
        this.hooks = { ...hooks };
    }

    async invoke<T extends ReadRoutingHookType>(
        type: T,
        context: Omit<ReadRoutingHookContext<T>, "type" | "timestamp">,
    ): Promise<void> {
        const fullContext: ReadRoutingHookContext<T> = {
            ...context,
            type,
            timestamp: new Date(),
        };
        const typeHooks = this.hooks[type] ?? [];
        if (typeHooks.length === 0) {
            return;
        }
        this.logger.debug?.({ hookType: type, hookCount: typeHooks.length }, "Invoking read routing hooks");
        const errors: Error[] = [];
        for (const hook of typeHooks) {
            try {
                await Promise.resolve((hook as ReadRoutingHook<T>)(fullContext));
            } catch (error) {
                errors.push(error instanceof Error ? error : new Error(String(error)));
            }
        }
        if (errors.length > 0) {
            this.logger.warn?.(
                { hookType: type, errorCount: errors.length },
                "Some read routing hooks failed",
            );
        }
    }

    registerHook<T extends ReadRoutingHookType>(type: T, hook: ReadRoutingHook<T>): void {
        if (!this.hooks[type]) {
            this.hooks[type] = [];
        }
        this.hooks[type]!.push(hook as ReadRoutingHook<ReadRoutingHookType>);
    }

    unregisterHook<T extends ReadRoutingHookType>(type: T, hook: ReadRoutingHook<T>): boolean {
        const typeHooks = this.hooks[type];
        if (!typeHooks) {
            return false;
        }
        const index = typeHooks.indexOf(hook as ReadRoutingHook<ReadRoutingHookType>);
        if (index === -1) {
            return false;
        }
        typeHooks.splice(index, 1);
        return true;
    }

    getHookCount(type: ReadRoutingHookType): number {
        return this.hooks[type]?.length ?? 0;
    }

    clear(): void {
        this.hooks = {};
    }
}

export const defaultReadRoutingHookLogger: ReadRoutingHookLogger = {
    debug: () => {},
    warn: () => {},
    error: () => {},
};
