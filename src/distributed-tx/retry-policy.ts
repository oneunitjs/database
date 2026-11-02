export interface RetryPolicyOptions {
    maxRetries: number;
    initialDelayMs: number;
    maxDelayMs: number;
    backoffMultiplier: number;
    jitter: number;
    isRetryable?: (error: unknown) => boolean;
}

export const DEFAULT_RETRY_POLICY_OPTIONS: Required<RetryPolicyOptions> = {
    maxRetries: 3,
    initialDelayMs: 100,
    maxDelayMs: 5_000,
    backoffMultiplier: 2,
    jitter: 0.1,
    isRetryable: defaultIsRetryable,
};

export function defaultIsRetryable(error: unknown): boolean {
    if (error instanceof Error) {
        const transient = (error as { transient?: boolean }).transient;
        if (transient === true) {
            return true;
        }
        if (transient === false) {
            return false;
        }
        const code = (error as NodeJS.ErrnoException).code;
        const retryableCodes = new Set([
            "ECONNREFUSED",
            "ECONNRESET",
            "ETIMEDOUT",
            "ENETUNREACH",
            "EHOSTUNREACH",
            "ENOTFOUND",
            "EAI_AGAIN",
            "DeadlockDetected",
        ]);
        if (code && retryableCodes.has(code)) {
            return true;
        }
    }
    const message = String(error).toLowerCase();
    return (
        message.includes("timeout") ||
        message.includes("timed out") ||
        message.includes("connection") ||
        message.includes("econn") ||
        message.includes("etimedout") ||
        message.includes("econnrefused") ||
        message.includes("econnreset") ||
        message.includes("enetunreach") ||
        message.includes("deadlock")
    );
}

export class RetryPolicy {
    private readonly options: Required<RetryPolicyOptions>;

    constructor(options?: Partial<RetryPolicyOptions>) {
        this.options = { ...DEFAULT_RETRY_POLICY_OPTIONS, ...options };
    }

    async execute<T>(operation: () => Promise<T>): Promise<T> {
        let lastError: unknown;
        let delay = this.options.initialDelayMs;

        for (let attempt = 0; attempt <= this.options.maxRetries; attempt++) {
            try {
                return await operation();
            } catch (error) {
                lastError = error;

                if (attempt >= this.options.maxRetries) {
                    break;
                }

                if (!this.options.isRetryable(error)) {
                    break;
                }

                await this.sleep(delay);
                delay = Math.min(
                    delay * this.options.backoffMultiplier,
                    this.options.maxDelayMs,
                );
            }
        }

        throw lastError;
    }

    wrap<T>(fn: () => Promise<T>): () => Promise<T> {
        return async () => this.execute(fn);
    }

    isRetryable(error: unknown): boolean {
        return this.options.isRetryable(error);
    }

    getNextDelay(attempt: number): number {
        if (attempt >= this.options.maxRetries) {
            return -1;
        }
        const delay =
            this.options.initialDelayMs *
            Math.pow(this.options.backoffMultiplier, attempt);
        const cappedDelay = Math.min(delay, this.options.maxDelayMs);
        const jitterAmount =
            (Math.random() * 2 - 1) * cappedDelay * this.options.jitter;
        return Math.max(0, Math.round(cappedDelay + jitterAmount));
    }

    getMaxRetries(): number {
        return this.options.maxRetries;
    }

    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
