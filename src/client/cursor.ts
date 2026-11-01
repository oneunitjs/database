import type { Pool, PoolClient, QueryResultRow, Submittable } from "pg";
import type { CursorApi, CursorHandle } from "../types.js";
import { isConnectionError } from "./errors.js";
import Cursor from "pg-cursor";

interface CursorInstance {
    read(count: number, callback: (error: Error | null, rows: QueryResultRow[]) => void): void;
    close(callback: (error: Error | null) => void): void;
}

type PoolResolver = (shardKey?: string, shardId?: string) => Pool;

export function createCursor(pool: Pool, resolvePool?: PoolResolver): CursorApi {
    async function cursor(text: string, values: unknown[] = [], options: Record<string, unknown> = {}): Promise<CursorHandle> {
        const targetPool = resolvePool
            ? resolvePool(options.shardKey as string | undefined, options.shardId as string | undefined)
            : pool;
        const client = await targetPool.connect();
        let instance: CursorInstance;

        try {
            const cursor = new Cursor(text, values, options);
            const queryResult = client.query(cursor as unknown as Submittable);
            
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

        // Track cursor creation time for leak detection
        const createdAt = Date.now();
        const CURSOR_TIMEOUT = 300000; // 5 minutes
        
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

        async function read(count = 100): Promise<QueryResultRow[]> {
            return new Promise((resolve, reject) => {
                const readTimeout = setTimeout(() => {
                    reject(new Error('Cursor read timed out'));
                }, 30000); // 30 seconds timeout
                
                instance.read(count, (error, rows) => {
                    clearTimeout(readTimeout);
                    if (error) {
                        close(error).catch(() => {});
                        reject(error);
                        return;
                    }

                    resolve(rows);
                });
            });
        }

        async function* iterate(batchSize = 100): AsyncGenerator<QueryResultRow, void, unknown> {
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
            [Symbol.asyncIterator]() {
                return iterate();
            },
        };
    }

    return {
        cursor,
    };
}