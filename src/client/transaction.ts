import type { Pool, PoolClient, TransactionOptions, TransactionHooks, RetryOptions } from "../types.js";
import { isConnectionError, normalizeError } from "./errors.js";
import { withRetry } from "./retry.js";

const ISOLATION_LEVELS = new Set([
    "READ UNCOMMITTED",
    "READ COMMITTED",
    "REPEATABLE READ",
    "SERIALIZABLE",
]);

const SAVEPOINT_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

function buildBeginStatement(options: TransactionOptions = {}): string {
    const modes: string[] = [];

    if (options.isolation) {
        const level = String(options.isolation).toUpperCase();
        if (!ISOLATION_LEVELS.has(level)) {
            throw new TypeError(`Invalid isolation level: ${options.isolation}`);
        }
        modes.push(`ISOLATION LEVEL ${level}`);
    }

    if (options.readOnly === true) {
        modes.push("READ ONLY");
    } else if (options.readOnly === false) {
        modes.push("READ WRITE");
    }

    if (options.deferrable === true) {
        modes.push("DEFERRABLE");
    } else if (options.deferrable === false) {
        modes.push("NOT DEFERRABLE");
    }

    return modes.length > 0 ? `BEGIN ${modes.join(" ")}` : "BEGIN";
}

function assertSavepointName(name: string): void {
    if (typeof name !== "string" || !SAVEPOINT_NAME.test(name)) {
        throw new TypeError(`Invalid savepoint name: ${name}`);
    }
}

function normalizeRetryOption(option: unknown): RetryOptions | null {
    if (option === true) {
        return {};
    }
    if (typeof option === "number") {
        return { retries: option };
    }
    if (option && typeof option === "object" && !Array.isArray(option)) {
        return option as RetryOptions;
    }
    return null;
}

export function createTransaction({
    pool,
    logger,
    hooks,
    retry,
}: {
    pool: Pool;
    logger?: unknown;
    hooks?: TransactionHooks;
    retry?: RetryOptions | boolean | number;
} = { pool: null as unknown as Pool }): {
    transaction: <T>(callback: (client: PoolClient) => Promise<T> | T, txOptions?: TransactionOptions) => Promise<T>;
    savepoint: <T>(client: PoolClient, name: string, callback: (client: PoolClient) => Promise<T> | T) => Promise<T>;
} {
    const retryDefaults = normalizeRetryOption(retry);

    async function transaction<T>(
        callback: (client: PoolClient) => Promise<T> | T,
        txOptions: TransactionOptions = {},
    ): Promise<T> {
        if (typeof callback !== "function") {
            throw new TypeError("transaction(callback) requires a callback function");
        }

        const beginStatement = buildBeginStatement(txOptions);
        const timeout = txOptions.timeout;

        if (timeout !== undefined) {
            const parsed = Math.floor(Number(timeout));
            if (!Number.isFinite(parsed) || parsed <= 0) {
                throw new TypeError(`Invalid transaction timeout: ${timeout}`);
            }
        }

        const execute = async (): Promise<T> => {
            const client = await pool.connect();
            let destroy = false;
            let caught: Error | undefined;

            try {
                await client.query(beginStatement);

                if (timeout) {
                    await client.query(`SET LOCAL statement_timeout = ${Math.floor(Number(timeout))}`);
                }

                const result = await callback(client);

                await client.query("COMMIT");
                hooks?.metrics?.recordTransaction?.(false);
                return result;
            } catch (error) {
                caught = error instanceof Error ? error : new Error(String(error));

                try {
                    await client.query("ROLLBACK");
                } catch (rollbackError) {
                    destroy = true;
                    (logger as { error?: (meta: object, msg: string) => void })?.error?.(
                        { err: rollbackError },
                        "Failed to rollback transaction",
                    );
                }

                if (isConnectionError(error)) {
                    destroy = true;
                }

                hooks?.metrics?.recordTransaction?.(true);
                throw error;
            } finally {
                if (destroy) {
                    client.release?.(normalizeError(caught ?? new Error("destroying connection")));
                } else {
                    client.release?.();
                }
            }
        };

        const retryOption = normalizeRetryOption(txOptions.retry) ?? retryDefaults;

        if (!retryOption) {
            return execute();
        }

        return withRetry(execute, {
            ...retryOption,
            onRetry: (info) => {
                hooks?.recordRetry?.(info);
                retryOption.onRetry?.(info);
            },
        });
    }

    async function savepoint<T>(client: PoolClient, name: string, callback: (client: PoolClient) => Promise<T> | T): Promise<T> {
        assertSavepointName(name);

        await client.query(`SAVEPOINT ${name}`);

        try {
            const result = await callback(client);

            await client.query(`RELEASE SAVEPOINT ${name}`);

            return result;
        } catch (error) {
            await client.query(`ROLLBACK TO SAVEPOINT ${name}`);

            throw error;
        }
    }

    return {
        transaction,
        savepoint,
    };
}