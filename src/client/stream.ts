import type { Pool, PoolClient } from "pg";
import type { StreamApi, StreamOptions } from "../types.js";
import { Readable } from "node:stream";
import { isConnectionError, TimeoutError } from "./errors.js";
import QueryStream from "pg-query-stream";

interface QueryStreamInstance extends NodeJS.ReadableStream {
    destroy?: (error: Error) => void;
}

type PoolResolver = (shardKey?: string, shardId?: string) => Pool;

export function createStream(
    pool: Pool,
    logger: unknown,
    defaults: { timeout?: number } = {},
    resolvePool?: PoolResolver,
): StreamApi {
    async function stream(
        text: string,
        values: unknown[] = [],
        streamOptions: StreamOptions = {},
    ): Promise<NodeJS.ReadableStream> {
        const targetPool = resolvePool
            ? resolvePool(streamOptions.shardKey, streamOptions.shardId)
            : pool;
        const client = await targetPool.connect();

        try {
            const query = new QueryStream(text, values, streamOptions as Record<string, unknown>);
            let result = client.query(query);
            if (result && typeof (result as { then?: unknown }).then === "function") {
                result = await result;
            }

            let released = false;
            let timer: ReturnType<typeof setTimeout> | undefined;

            const release = (error?: Error): void => {
                if (released) {
                    return;
                }
                released = true;
                if (timer) {
                    clearTimeout(timer);
                }
                if (error && isConnectionError(error)) {
                    client.release?.(error);
                } else {
                    client.release?.();
                }
            };

            const timeoutMs = streamOptions.timeout ?? defaults.timeout ?? 0;
            if (timeoutMs > 0) {
                timer = setTimeout(() => {
                    const error = new TimeoutError(`Stream timed out after ${timeoutMs}ms`, {
                        timeout: timeoutMs,
                    });
                    (result as QueryStreamInstance).destroy?.(error);
                    release(error);
                }, timeoutMs);
            }

            result.once("end", () => release());
            result.once("close", () => release());
            result.once("error", (error) => {
                (logger as { error?: (meta: object, msg: string) => void })?.error?.({ err: error }, "Database stream failed");
                release(error instanceof Error ? error : new Error(String(error)));
            });

            return result as NodeJS.ReadableStream;
        } catch (error) {
            client.release?.(error instanceof Error ? error : new Error(String(error)));
            throw error;
        }
    }

    return {
        stream,
    };
}