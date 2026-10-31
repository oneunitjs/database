declare module "pg-cursor" {
    import type { PoolClient, QueryResult } from "pg";

    interface CursorOptions {
        rows?: number;
        [key: string]: unknown;
    }

    class Cursor {
        constructor(text: string, values?: unknown[], options?: CursorOptions);
        read(count: number, callback: (error: Error | null, rows: unknown[]) => void): void;
        close(callback: (error: Error | null) => void): void;
    }

    export = Cursor;
}
