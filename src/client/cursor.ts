import type { Pool, QueryResultRow, Submittable } from "pg";
import type { CursorApi, CursorHandle } from "../types.js";
import { isConnectionError } from "./errors.js";

type CursorConstructor = new (text: string, values?: unknown[], options?: Record<string, unknown>) => Submittable;

let cursorCtor: CursorConstructor | undefined;
let cursorLoad: Promise<CursorConstructor> | undefined;

async function loadCursor(): Promise<CursorConstructor> {
    if (cursorCtor) {
        return cursorCtor;
    }
    if (!cursorLoad) {
        cursorLoad = import("pg-cursor")
            .then((mod) => {
                const ctor = (mod as unknown as { default?: CursorConstructor }).default ?? (mod as unknown as CursorConstructor);
                cursorCtor = ctor;
                return ctor;
            })
            .catch((error: unknown) => {
                cursorLoad = undefined;
                throw new Error(
                    'Cursors require the optional "pg-cursor" peer dependency. Install it with: npm install pg-cursor',
                    { cause: error instanceof Error ? error : undefined },
                );
            });
    }
    return cursorLoad;
}

interface CursorInstance {
    read(count: number, callback: (error: Error | null, rows: QueryResultRow[]) => void): void;
    close(callback: (error: Error | null) => void): void;
}

type PoolResolver = (shardKey?: string, shardId?: string) => Pool;

export function createCursor(pool: Pool, resolvePool?: PoolResolver): CursorApi {
    async function cursor(
        text: string,
        values: unknown[] = [],
        options: Record<string, unknown> = {},
    ): Promise<CursorHandle> {
        const Cursor = await loadCursor();
        const targetPool = resolvePool
            ? resolvePool(options.shardKey as string | undefined, options.shardId as string | undefined)
            : pool;
        const client = await targetPool.connect();
        let instance: CursorInstance;

        try {
            const cursorInstance = new Cursor(text, values, options);
            const queryResult = client.query(cursorInstance as unknown as Submittable);

            // Handle both Promise and direct return
            if (queryResult && typeof (queryResult as { then?: unknown }).then === "function") {
                instance = await queryResult as unknown as CursorInstance;
            } else {
                instance = queryResult as unknown as CursorInstance;
            }
        } catch (error) {
            client.release?.(error instanceof Error ? error : new Error(String(error)));
            throw error;
        }

        let released = false;
        let closed = false;

        const CURSOR_TIMEOUT = 300000; // 5 minutes
        // Configurable batch size for read/iterate operations
        const defaultBatchSize = (options.batchSize as number | undefined) ?? 100;

        // Progress reporting
        const onProgress = options.onProgress as
            | ((info: { rows: number; elapsedMs: number }) => void)
            | undefined;
        let rowsRead = 0;
        const startTime = Date.now();

        // Set timeout to warn about long-running cursors
        const cursorTimeout = setTimeout(() => {
            console.warn(
                `[database] Cursor held for more than 5 minutes. ` +
                `This may indicate a leak. Stack: ${new Error().stack}`
            );
        }, CURSOR_TIMEOUT);

        const release = (error?: Error): void => {
            if (released) {
                return;
            }
            released = true;
            clearTimeout(cursorTimeout);
            if (error && isConnectionError(error)) {
                client.release?.(error);
            } else {
                client.release?.();
            }
        };

        function close(error?: Error): Promise<void> {
            return new Promise((resolve, reject) => {
                if (closed) {
                    release(error);
                    return resolve();
                }

                closed = true;
                clearTimeout(cursorTimeout);

                instance.close((closeError) => {
                    release(error ?? (closeError ?? undefined));

                    if (closeError) {
                        reject(closeError);
                        return;
                    }

                    resolve();
                });
            });
        }

        // Cancel the cursor — close it and release the connection.
        async function cancel(): Promise<void> {
            await close();
        }

        async function read(count = defaultBatchSize): Promise<QueryResultRow[]> {
            return new Promise((resolve, reject) => {
                const readTimeout = setTimeout(() => {
                    reject(new Error("Cursor read timed out"));
                }, 30000); // 30 seconds timeout

                instance.read(count, (error, rows) => {
                    clearTimeout(readTimeout);
                    if (error) {
                        close(error).catch(() => {});
                        reject(error);
                        return;
                    }

                    if (rows && onProgress) {
                        rowsRead += rows.length;
                        onProgress({
                            rows: rowsRead,
                            elapsedMs: Date.now() - startTime,
                        });
                    }

                    resolve(rows);
                });
            });
        }

        async function* iterate(batchSize = defaultBatchSize): AsyncGenerator<QueryResultRow, void, unknown> {
            try {
                for (;;) {
                    const rows = await read(batchSize);

                    if (!rows || rows.length === 0) {
                        break;
                    }

                    for (const row of rows) {
                        yield row;
                    }
                }
            } finally {
                clearTimeout(cursorTimeout);
                await close().catch(() => {});
            }
        }

        return {
            read,
            close,
            cancel,
            [Symbol.asyncIterator]() {
                return iterate();
            },
        };
    }

    return {
        cursor,
    };
}
