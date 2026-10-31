import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { pathToFileURL } from "node:url";
import type { Pool, PoolClient, QueryResult } from "pg";
import type { Logger, MigrationDefinition, Migrator } from "../types.js";
import { quoteIdent } from "../query/sql.js";

const DEFAULT_TABLE = "_writer_migrations";
const DEFAULT_LOCK_KEY = 727_274;
const MIGRATION_EXTENSIONS = new Set([".sql", ".js", ".mjs", ".cjs"]);

function checksum(value: string): string {
    return crypto.createHash("sha256").update(value).digest("hex");
}

function normalizeId(value: unknown): string {
    return String(value).trim();
}

function asExecutable(
    migration: MigrationDefinition,
    context: { client: PoolClient; query: PoolClient["query"]; logger?: Logger },
): () => Promise<unknown> {
    const up = migration.up;

    if (typeof up === "function") {
        return () => up(context);
    }

    if (typeof up === "string") {
        return () => context.query(up);
    }

    throw new TypeError(`Migration ${migration.id} does not expose a valid up()`);
}

function normalizeMigrations(list: MigrationDefinition[]): MigrationDefinition[] {
    return list.map((entry, index) => {
        if (typeof entry === "function") {
            const fn = entry as Function;
            const name = fn.name && fn.name !== "" ? fn.name : `migration_${index + 1}`;
            return {
                id: name,
                name,
                up: entry,
                down: undefined,
                source: String(entry),
            };
        }

        const id = normalizeId(entry.id ?? entry.name ?? `migration_${index + 1}`);
        const up = entry.up ?? entry.sql;
        return {
            id,
            name: entry.name ?? id,
            up,
            down: entry.down,
            source: [String(up ?? ""), String(entry.down ?? "")].join("\n"),
        };
    });
}

async function loadDirectory(directory: string): Promise<MigrationDefinition[]> {
    const entries = (await fs.readdir(directory, { withFileTypes: true }))
        .filter((entry) => entry.isFile() && MIGRATION_EXTENSIONS.has(path.extname(entry.name)))
        .sort((a, b) => a.name.localeCompare(b.name));

    const migrations: MigrationDefinition[] = [];

    for (const entry of entries) {
        const fullPath = path.join(directory, entry.name);
        const extension = path.extname(entry.name);
        const id = entry.name.slice(0, -extension.length);

        if (extension === ".sql") {
            const content = await fs.readFile(fullPath, "utf8");
            migrations.push({ id, name: id, up: content });
            continue;
        }

        const module = await import(pathToFileURL(fullPath).href);
        const definition = module.default ?? module;
        const up = definition.up ?? (typeof definition === "function" ? definition : "");
        migrations.push({
            id,
            name: definition.name ?? id,
            up,
            down: definition.down,
            source: JSON.stringify({
                up: String(definition.up ?? ""),
                down: String(definition.down ?? ""),
            }),
        });
    }

    return migrations;
}

async function resolveSource(source: unknown): Promise<MigrationDefinition[]> {
    if (!source) {
        return [];
    }
    if (Array.isArray(source)) {
        return normalizeMigrations(source as MigrationDefinition[]);
    }
    if (typeof source === "string") {
        return loadDirectory(source);
    }
    if (typeof source === "object" && source !== null) {
        const src = source as { migrations?: MigrationDefinition[]; directory?: string };
        if (Array.isArray(src.migrations)) {
            return normalizeMigrations(src.migrations);
        }
        if (typeof src.directory === "string") {
            return loadDirectory(src.directory);
        }
    }
    return [];
}

/**
 * Portable migration runner. It keeps an advisory lock for the duration of a
 * run, records every applied migration with a checksum, and executes each
 * migration inside its own transaction.
 */
export function createMigrator(
    pool: Pool,
    {
        logger,
        table = DEFAULT_TABLE,
        lockKey = DEFAULT_LOCK_KEY,
        source,
        migrations,
        directory,
    }: {
        logger?: Logger;
        table?: string;
        lockKey?: number;
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
    } = {},
): Migrator {
    const tableIdent = quoteIdent(table);
    const defaultSource = source ?? migrations ?? directory;

    async function ensureTable(client: PoolClient): Promise<void> {
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableIdent} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                execution_ms INTEGER NOT NULL DEFAULT 0,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        `);
    }

    async function applied(client: PoolClient): Promise<Array<{ id: string; name: string; checksum: string; applied_at: unknown }>> {
        const { rows } = await client.query(
            `SELECT id, name, checksum, applied_at FROM ${tableIdent} ORDER BY applied_at ASC, id ASC`,
        );
        return rows;
    }

    async function withClient<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
        const client = await pool.connect();
        let locked = false;
        try {
            await client.query("SELECT pg_advisory_lock($1)", [lockKey]);
            locked = true;
            return await fn(client);
        } finally {
            if (locked) {
                await client.query("SELECT pg_advisory_unlock($1)", [lockKey]).catch(() => {});
            }
            client.release();
        }
    }

    async function run(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<string[]> {
        const list = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);

        return withClient(async (client) => {
            await ensureTable(client);
            const done = await applied(client);
            const doneIds = new Set(done.map((row) => row.id));
            const appliedNow: string[] = [];

            for (const migration of list) {
                const migrationId = migration.id!;
                if (doneIds.has(migrationId)) {
                    continue;
                }

                const context = {
                    client,
                    query: client.query.bind(client),
                    logger,
                };

                const startedAt = Date.now();

                await client.query("BEGIN");
                try {
                    await asExecutable(migration, context)();
                    await client.query(
                        `INSERT INTO ${tableIdent} (id, name, checksum, execution_ms) VALUES ($1, $2, $3, $4)`,
                        [migrationId, migration.name!, checksum(migration.source ?? ""), Date.now() - startedAt],
                    );
                    await client.query("COMMIT");
                    appliedNow.push(migrationId);
                    logger?.info?.({ migration: migrationId }, "Migration applied");
                } catch (error) {
                    await client.query("ROLLBACK").catch(() => {});
                    throw error;
                }
            }

            return appliedNow;
        });
    }

    async function status(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<Array<{ id: string; name: string; applied: boolean; appliedAt: unknown }>> {
        const list = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);

        return withClient(async (client) => {
            await ensureTable(client);
            const done = await applied(client);
            const doneMap = new Map(done.map((row) => [row.id, row]));

            return list.map((migration) => ({
                id: migration.id!,
                name: migration.name!,
                applied: doneMap.has(migration.id!),
                appliedAt: doneMap.get(migration.id!)?.applied_at ?? null,
            }));
        });
    }

    async function rollback(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; steps?: number } = {}): Promise<string[]> {
        const count = Math.max(1, Number(options.steps ?? 1));
        const list = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const byId = new Map(list.map((migration) => [migration.id!, migration]));

        return withClient(async (client) => {
            await ensureTable(client);
            const done = await applied(client);
            const targets = done.slice(-count).reverse();
            const rolledBack: string[] = [];

            for (const record of targets) {
                const migration = byId.get(record.id);
                if (!migration?.down) {
                    throw new Error(`Cannot roll back ${record.id}: no down() migration provided`);
                }

                const context = {
                    client,
                    query: client.query.bind(client),
                    logger,
                };

                await client.query("BEGIN");
                try {
                    if (typeof migration.down === "function") {
                        await migration.down(context);
                    } else {
                        await client.query(migration.down);
                    }
                    await client.query(`DELETE FROM ${tableIdent} WHERE id = $1`, [record.id]);
                    await client.query("COMMIT");
                    rolledBack.push(record.id);
                } catch (error) {
                    await client.query("ROLLBACK").catch(() => {});
                    throw error;
                }
            }

            return rolledBack;
        });
    }

    return {
        run,
        status,
        rollback,
        table,
    };
}