// ---------------------------------------------------------------------------
// Sync State Store — durable storage for schema sync operation state.
// ---------------------------------------------------------------------------

import type { Pool } from "pg";
import { SCHEMA_SYNC_SCHEMA } from "./schema-sync-schema.js";
import type { SchemaSyncOperation, SchemaSyncStatus } from "./schema-sync-types.js";
import crypto from "node:crypto";

export class SyncStateStore {
    private readonly pool: Pool;
    private schemaReady?: Promise<void>;

    private async ensureSchema(): Promise<void> {
        if (!this.schemaReady) {
            this.schemaReady = this.pool.query(SCHEMA_SYNC_SCHEMA).then(() => undefined).catch(() => undefined);
        }
        await this.schemaReady;
    }

    constructor(pool: Pool) {
        this.pool = pool;
    }

    /** Create/record a sync operation (accepts operation or migration). */
    async createOperation(
        // ensure schema
input: any): Promise<SchemaSyncOperation> {
        await this.ensureSchema();
        let operation: SchemaSyncOperation;
        if (input && input.operationId) {
            operation = input as SchemaSyncOperation;
        } else {
            const migration = input ?? {};
            const now = new Date();
            operation = {
                operationId: crypto.randomUUID(),
                status: "pending",
                migrationVersion: migration.id ?? migration.version ?? "unknown",
                migrationName: migration.name ?? migration.id ?? "migration",
                migrationSql:
                    typeof migration.up === "string"
                        ? migration.up
                        : String(migration.up ?? migration.sql ?? ""),
                checksum: crypto
                    .createHash("sha256")
                    .update(String(migration.up ?? migration.sql ?? ""))
                    .digest("hex"),
                startedAt: now,
                createdAt: now,
                updatedAt: now,
                retries: 0,
            };
        }
        await this.recordOperation(operation);
        return operation;
    }

    async recordOperation(
        // ensure schema
operation: SchemaSyncOperation): Promise<void> {
        await this.ensureSchema();
        await this.pool.query(
            `INSERT INTO schema_sync_state
                (operation_id, status, migration_version, migration_name,
                 migration_sql, checksum, started_at, completed_at, error_message,
                 error_stack, retries, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (operation_id) DO UPDATE SET
                 status = EXCLUDED.status,
                 migration_version = EXCLUDED.migration_version,
                 migration_name = EXCLUDED.migration_name,
                 migration_sql = EXCLUDED.migration_sql,
                 checksum = EXCLUDED.checksum,
                 updated_at = EXCLUDED.updated_at,
                 error_message = EXCLUDED.error_message,
                 error_stack = EXCLUDED.error_stack,
                 retries = EXCLUDED.retries`,
            [
                operation.operationId,
                operation.status,
                operation.migrationVersion,
                operation.migrationName,
                operation.migrationSql,
                operation.checksum,
                operation.startedAt,
                operation.completedAt ?? null,
                operation.errorMessage ?? null,
                operation.errorStack ?? null,
                operation.retries,
                operation.createdAt,
                operation.updatedAt,
            ],
        );
    }

    async updateStatus(
        // ensure schema

        operationId: string,
        status: string,
        error?: Error,
    ): Promise<void> {
        await this.ensureSchema();
        await this.pool.query(
            `UPDATE schema_sync_state
             SET status = $2, updated_at = NOW(),
                  error_message = $3,
                  error_stack = $4,
                  completed_at = CASE WHEN $2 IN ('completed','failed','rolled_back','cancelled') THEN NOW() ELSE completed_at END
             WHERE operation_id = $1`,
            [operationId, status, error?.message ?? null, error?.stack ?? null],
        );
    }

    async getOperation(
        // ensure schema
operationId: string): Promise<SchemaSyncOperation | null> {
        await this.ensureSchema();
        const result = await this.pool.query(
            `SELECT * FROM schema_sync_state WHERE operation_id = $1`,
            [operationId],
        );
        if (result.rows.length === 0) return null;
        return this.rowToOperation(result.rows[0]);
    }

    async getAllOperations(
        // ensure schema
limit?: number): Promise<SchemaSyncOperation[]> {
        await this.ensureSchema();
        const sql = limit
            ? `SELECT * FROM schema_sync_state ORDER BY started_at DESC LIMIT $1`
            : `SELECT * FROM schema_sync_state ORDER BY started_at DESC`;
        const result = await this.pool.query(sql, limit ? [limit] : []);
        return result.rows.map((r) => this.rowToOperation(r));
    }

    async getOperationsByStatus(
        // ensure schema
status: string): Promise<SchemaSyncOperation[]> {
        await this.ensureSchema();
        const result = await this.pool.query(
            `SELECT * FROM schema_sync_state WHERE status = $1 ORDER BY started_at DESC`,
            [status],
        );
        return result.rows.map((r) => this.rowToOperation(r));
    }

    async getHistory(
        // ensure schema
limit?: number): Promise<SchemaSyncOperation[]> {
        await this.ensureSchema();
        return this.getAllOperations(limit);
    }

    async create(input: any): Promise<SchemaSyncOperation> { return this.createOperation(input); }
    async add(input: any): Promise<SchemaSyncOperation> { return this.createOperation(input); }
    async update(operationId: string, status: string, error?: Error): Promise<void> { return this.updateStatus(operationId, status, error); }
    async updateOperation(operationId: string, status: string, error?: Error): Promise<void> { return this.updateStatus(operationId, status, error); }
    async setStatus(operationId: string, status: string, error?: Error): Promise<void> { return this.updateStatus(operationId, status, error); }
    async get(operationId: string): Promise<SchemaSyncOperation | null> { return this.getOperation(operationId); }
    async getById(operationId: string): Promise<SchemaSyncOperation | null> { return this.getOperation(operationId); }
    async getAll(limit?: number): Promise<SchemaSyncOperation[]> { return this.getAllOperations(limit); }
    async list(limit?: number): Promise<SchemaSyncOperation[]> { return this.getAllOperations(limit); }
    async getByStatus(status: string): Promise<SchemaSyncOperation[]> { return this.getOperationsByStatus(status); }
    async record(operation: SchemaSyncOperation): Promise<void> { return this.recordOperation(operation); }

    private rowToOperation(row: any): SchemaSyncOperation {
        return {
            operationId: row.operation_id,
            status: row.status,
            migrationVersion: row.migration_version,
            migrationName: row.migration_name,
            migrationSql: row.migration_sql,
            checksum: row.checksum,
            startedAt: row.started_at ? new Date(row.started_at) : new Date(),
            completedAt: row.completed_at ? new Date(row.completed_at) : undefined,
            errorMessage: row.error_message,
            errorStack: row.error_stack,
            retries: row.retries,
            createdAt: new Date(row.created_at),
            updatedAt: new Date(row.updated_at),
        };
    }
}
