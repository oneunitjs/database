import type { Pool } from "pg";
import type { ShutdownApi } from "../types.js";
import { TimeoutError } from "./errors.js";

const DEFAULT_SHUTDOWN_TIMEOUT = 10_000;

/**
 * Gracefully close the pool. Safe to call multiple times: concurrent callers
 * share the same in-flight promise and repeated calls after completion resolve
 * immediately instead of throwing "pool already ended".
 */
export function createShutdown(
    pool: Pool,
    { logger, timeout = DEFAULT_SHUTDOWN_TIMEOUT }: { logger?: unknown; timeout?: number } = {},
): ShutdownApi {
    let pending: Promise<void> | null = null;
    let closed = false;

    async function shutdown(options: { timeout?: number } = {}): Promise<void> {
        if (closed) {
            return;
        }
        if (pending) {
            return pending;
        }

        const timeoutMs = options.timeout ?? timeout;

        pending = (async () => {
            if (!timeoutMs || timeoutMs <= 0) {
                await pool.end();
                return;
            }

            await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(() => {
                    reject(new TimeoutError(`Pool shutdown timed out after ${timeoutMs}ms`, {
                        timeout: timeoutMs,
                    }));
                }, timeoutMs);

                Promise.resolve(pool.end()).then(
                    (value) => {
                        clearTimeout(timer);
                        resolve(value);
                    },
                    (error) => {
                        clearTimeout(timer);
                        reject(error);
                    },
                );
            });
        })();

        try {
            await pending;
            closed = true;
        } catch (error) {
            (logger as { error?: (meta: object, msg: string) => void })?.error?.({ err: error }, "Failed to shut down database pool");
            throw error;
        } finally {
            pending = null;
        }
    }

    return {
        shutdown,
        get isClosed(): boolean {
            return closed;
        },
    };
}