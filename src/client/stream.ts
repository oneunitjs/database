import type { Pool } from "pg";
import type { StreamApi, StreamOptions } from "../types.js";
import { isConnectionError, TimeoutError } from "./errors.js";

type QueryStreamConstructor = new (text: string, values?: unknown[], options?: Record<string, unknown>) => NodeJS.ReadableStream & {
    destroy?: (error: Error) => void;
};

let queryStreamCtor: QueryStreamConstructor | undefined;
let queryStreamLoad: Promise<QueryStreamConstructor> | undefined;

async function loadQueryStream(): Promise<QueryStreamConstructor> {
    if (queryStreamCtor) {
        return queryStreamCtor;
    }
    if (!queryStreamLoad) {
        queryStreamLoad = import("pg-query-stream")
            .then((mod) => {
                const ctor = (mod as unknown as { default?: QueryStreamConstructor }).default ?? (mod as unknown as QueryStreamConstructor);
                queryStreamCtor = ctor;
                return ctor;
            })
            .catch((error: unknown) => {
                queryStreamLoad = undefined;
                throw new Error(
                    'Streaming requires the optional "pg-query-stream" peer dependency. Install it with: npm install pg-query-stream',
                    { cause: error instanceof Error ? error : undefined },
                );
            });
    }
    return queryStreamLoad;
}

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
        const {
            timeout = defaults.timeout,
            signal,
            retries = 0,
            onProgress,
        } = streamOptions;

        const targetPool = resolvePool
            ? resolvePool(streamOptions.shardKey, streamOptions.shardId)
            : pool;

        const QueryStream = await loadQueryStream();
        const client = await targetPool.connect();

        let released = false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let aborted = false;
        let rowsStreamed = 0;
        const startTime = Date.now();

        const release = (error?: Error): void => {
            if (released) {
                return;
            }
            released = true;
            if (timer) {
                clearTimeout(timer);
            }
            if (signal) {
                signal.removeEventListener("abort", handleAbort);
            }
            if (error && isConnectionError(error)) {
                client.release?.(error);
            } else {
                client.release?.();
            }
        };

        const handleAbort = (): void => {
            aborted = true;
            if (timer) {
                clearTimeout(timer);
            }
            release(new Error("Stream aborted"));
        };

        if (signal) {
            if (signal.aborted) {
                handleAbort();
                throw new Error("Stream aborted");
            }
            signal.addEventListener("abort", handleAbort, { once: true });
        }

        const executeStream = async (): Promise<QueryStreamInstance> => {
            const query = new QueryStream(text, values, streamOptions as Record<string, unknown>);
            let result = client.query(query as never) as QueryStreamInstance | Promise<QueryStreamInstance>;
            if (result && typeof (result as { then?: unknown }).then === "function") {
                result = await result;
            }
            return result as QueryStreamInstance;
        };

        let streamResult: QueryStreamInstance | undefined;
        let lastError: unknown;

        // Error recovery: retry the initial query execution up to `retries` times.
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                streamResult = await executeStream();
                lastError = undefined;
                break;
            } catch (error) {
                lastError = error;
                if (attempt < retries && !aborted) {
                    continue;
                }
                release(error instanceof Error ? error : new Error(String(error)));
                throw error;
            }
        }

        if (!streamResult) {
            throw lastError ?? new Error("Stream execution failed");
        }

        // Progress reporting: count data events and invoke the callback.
        if (onProgress) {
            streamResult.on("data", () => {
                rowsStreamed++;
                onProgress({
                    rows: rowsStreamed,
                    elapsedMs: Date.now() - startTime,
                });
            });
        }

        const timeoutMs = timeout ?? 0;
        if (timeoutMs > 0) {
            timer = setTimeout(() => {
                const error = new TimeoutError(`Stream timed out after ${timeoutMs}ms`, {
                    timeout: timeoutMs,
                });
                streamResult.destroy?.(error);
                release(error);
            }, timeoutMs);
        }

        streamResult.once("end", () => release());
        streamResult.once("close", () => release());
        streamResult.once("error", (error: unknown) => {
            (logger as { error?: (meta: object, msg: string) => void })?.error?.({ err: error }, "Database stream failed");
            release(error instanceof Error ? error : new Error(String(error)));
        });

        return streamResult;
    }

    return {
        stream,
    };
}
