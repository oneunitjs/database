import type { RetryOptions, RetryInfo, NormalizedError } from "../types.js";
import { isTransientError, normalizeError, TimeoutError } from "./errors.js";

function defaultSleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function computeDelay(attempt: number, options: RetryOptions): number {
    const base = options.baseDelay ?? 50;
    const factor = options.factor ?? 2;
    const maxDelay = options.maxDelay ?? 2000;
    const raw = Math.min(maxDelay, base * Math.pow(factor, attempt));
    const jitter = options.jitter ?? 0.2;
    if (!jitter) {
        return raw;
    }
    const spread = raw * jitter;
    return Math.round(raw - spread / 2 + Math.random() * spread);
}

function throwIfAborted(signal: AbortSignal | undefined): void {
    if (signal?.aborted) {
        throw signal.reason instanceof Error
            ? signal.reason
            : new Error("Operation aborted");
    }
}

/**
 * Execute `fn` and retry it while the failure is considered transient.
 *
 * Retries are opt-in for queries and transactions because not every statement
 * is idempotent. The predicate defaults to `isTransientError`, which only
 * matches errors PostgreSQL explicitly documents as retryable (serialization
 * failures, deadlocks, connection loss, resource exhaustion).
 */
export async function withRetry<T>(
    fn: (attempt: number) => Promise<T>,
    options: RetryOptions = {},
): Promise<T> {
    const settings = {
        retries: options.retries ?? options.maxRetries ?? 3,
        baseDelay: options.baseDelay ?? 50,
        maxDelay: options.maxDelay ?? 2000,
        factor: options.factor ?? 2,
        jitter: options.jitter ?? 0.2,
        shouldRetry: options.shouldRetry ?? isTransientError,
        sleep: options.sleep ?? defaultSleep,
        signal: options.signal,
        onRetry: options.onRetry,
    };

    let attempt = 0;

    for (;;) {
        throwIfAborted(settings.signal);

        try {
            return await fn(attempt);
        } catch (error) {
            const isLast = attempt >= settings.retries;
            const retryable =
                !isLast &&
                !(error instanceof TimeoutError && options.retryOnTimeout === false) &&
                settings.shouldRetry(error);

            if (!retryable) {
                throw error;
            }

            const delay = computeDelay(attempt, settings);
            attempt += 1;

            try {
                settings.onRetry?.({
                    error: normalizeError(error) as NormalizedError,
                    attempt,
                    delay,
                    retries: settings.retries,
                } as RetryInfo);
            } catch {
                // A misbehaving hook must never break the retry loop.
            }

            await settings.sleep(delay);
        }
    }
}

export function createRetry(defaults: RetryOptions = {}): {
    run: <T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions) => Promise<T>;
    retry: <T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions) => Promise<T>;
    withRetry: <T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions) => Promise<T>;
    isTransient: typeof isTransientError;
} {
    function run<T>(fn: (attempt: number) => Promise<T>, options: RetryOptions = {}): Promise<T> {
        return withRetry(fn, { ...defaults, ...options });
    }

    return {
        run,
        retry: run,
        withRetry: run,
        isTransient: isTransientError,
    };
}