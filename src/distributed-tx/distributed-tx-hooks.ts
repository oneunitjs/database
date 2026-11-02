export type DistributedTxHookType =
    | "beforePrepare"
    | "afterPrepare"
    | "prepareFailed"
    | "beforeCommit"
    | "afterCommit"
    | "commitFailed"
    | "beforeAbort"
    | "afterAbort"
    | "abortFailed"
    | "onParticipantJoin"
    | "onParticipantLeave"
    | "onStateChange"
    | "onTimeout";

export interface DistributedTxHookContext<T extends DistributedTxHookType = DistributedTxHookType> {
    type: T;
    transactionId: string;
    timestamp: Date;
    coordinator: DistributedTxCoordinatorHookAPI;
    data: DistributedTxHookData[T];
}

export type DistributedTxHook<T extends DistributedTxHookType = DistributedTxHookType> = (
    context: DistributedTxHookContext<T>,
) => Promise<void> | void;

export interface DistributedTxHookData {
    beforePrepare: { participants: string[] };
    afterPrepare: { preparedTransactionIds: string[]; durationMs: number };
    prepareFailed: { error: Error; preparedCount: number; failedCount: number };
    beforeCommit: { participantCount: number };
    afterCommit: { committedCount: number; durationMs: number };
    commitFailed: { error: Error; committedCount: number; failedCount: number };
    beforeAbort: { participantCount: number };
    afterAbort: { abortedCount: number; durationMs: number };
    abortFailed: { error: Error; abortedCount: number; failedCount: number };
    onParticipantJoin: { shardId: string };
    onParticipantLeave: { shardId: string };
    onStateChange: { fromState: string; toState: string };
    onTimeout: { phase: string; timeoutMs: number; shardId?: string };
}

export interface DistributedTxCoordinatorHookAPI {
    getCurrentState(): string;
    getCurrentTransactionId(): string;
    getParticipantCount(): number;
}

export type DistributedTxHooks = {
    [K in DistributedTxHookType]?: DistributedTxHook<K>[];
};

export class DistributedTxHookRunner {
    private hooks: Record<string, DistributedTxHook<DistributedTxHookType>[]> = {};

    constructor(private readonly logger: DistributedTxLogger) {}

    async invoke<T extends DistributedTxHookType>(
        type: T,
        context: Omit<DistributedTxHookContext<T>, "type" | "timestamp">,
    ): Promise<void> {
        const fullContext: DistributedTxHookContext<T> = { ...context, type, timestamp: new Date() };
        const key = type as string;
        const typeHooks = this.hooks[key] as DistributedTxHook<DistributedTxHookType>[] | undefined;
        if (!typeHooks || typeHooks.length === 0) {
            return;
        }
        this.logger.debug({ hookType: type, hookCount: typeHooks.length, transactionId: context.transactionId }, "Invoking distributed tx hooks");
        const errors: Error[] = [];
        for (const hook of typeHooks) {
            try {
                await Promise.resolve((hook as DistributedTxHook<T>)(fullContext));
            } catch (error) {
                errors.push(error instanceof Error ? error : new Error(String(error)));
            }
        }
        if (errors.length > 0) {
            this.logger.warn({ hookType: type, errorCount: errors.length, transactionId: context.transactionId }, "Some hooks failed");
        }
    }

    registerHook<T extends DistributedTxHookType>(type: T, hook: DistributedTxHook<T>): void {
        const key = type as string;
        if (!this.hooks[key]) {
            this.hooks[key] = [];
        }
        this.hooks[key]!.push(hook as DistributedTxHook<DistributedTxHookType>);
    }

    unregisterHook<T extends DistributedTxHookType>(type: T, hook: DistributedTxHook<T>): boolean {
        const key = type as string;
        const typeHooks = this.hooks[key];
        if (!typeHooks) {
            return false;
        }
        const index = typeHooks.indexOf(hook as DistributedTxHook<DistributedTxHookType>);
        if (index === -1) {
            return false;
        }
        typeHooks.splice(index, 1);
        return true;
    }

    getHookCount(type: DistributedTxHookType): number {
        const key = type as keyof DistributedTxHooks;
        return this.hooks[key]?.length ?? 0;
    }

    clear(): void {
        this.hooks = {};
    }
}

export type DistributedTxLogger = {
    debug(payload: unknown, message?: string): void;
    info(payload: unknown, message?: string): void;
    warn(payload: unknown, message?: string): void;
    error(payload: unknown, message?: string): void;
};

export const defaultDistributedTxHookLogger: DistributedTxLogger = {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
};
