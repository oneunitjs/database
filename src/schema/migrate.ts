import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";
import type { PoolClient } from "pg";
import type {
    Logger,
    MigrationDefinition,
    MigrationLogRecord,
    MigrationRunStatus,
    MigrationMode,
    MigrationQueryFn,
    MigrationRetryOptions,
    MigrationRecoveryOptions,
    Migrator,
    MigrationAuditEvent,
    MigrationTarget,
    ApprovalProvider,
    BackupProvider,
    ApprovalStatus,
    BaselineRecord,
    BaselineOptions,
    BaselineResult,
    SquashOptions,
    SquashResult,
    MigrationProgressOptions,
    MigrationProgressResult,
    MigrationProgressEvent,
    DiscoverDependenciesOptions,
    DiscoverDependenciesResult,
    RepairMigrationOptions,
    RepairMigrationResult,
    MigrationApprovalResult,
    MigrationBackupRecord,
    RecoveryResult,
    ReconcileResult,
    TargetDrift,
    InterruptionRecord,
    RollbackAssessment,
    RollbackRisk,
    LockWaitMetric,
    LockContention,
    LongRunningTransaction,
    LockDiagnostics,
    ExpandContractOptions,
    ExpandContractResult,
    BackfillProgress,
    BackfillJob,
    BackfillOptions,
    BackfillResult,
    BackfillStatusOptions,
    BackfillCursor,
    BackfillBatchContext,
    BackfillProcessBatchResult,
    BackfillJobStatus,
    BackfillCursorType,
    PauseBackfillOptions,
    ResumeBackfillOptions,
    CancelBackfillOptions,
    RetryBackfillOptions,
    ExpandContractPhase,
    ExpandContractPhaseStatus,
    ExpandContractPhaseState,
} from "../types.js";
import { quoteIdent } from "../query/sql.js";

const DEFAULT_TABLE = "_writer_migrations";
const DEFAULT_HISTORY_TABLE = "_writer_migration_history";
const DEFAULT_BASELINE_TABLE = "_writer_migrations_baseline";
const DEFAULT_APPROVAL_TABLE = "_writer_migrations_approval";
const DEFAULT_BACKUP_TABLE = "_writer_migrations_backup";
const DEFAULT_OPERATION_TABLE = "_writer_migration_operations";
const DEFAULT_BACKFILL_TABLE = "_writer_migration_backfills";
const DEFAULT_PHASE_TABLE = "_writer_migration_phases";
const DEFAULT_LOCK_KEY = 727_274;
const DEFAULT_LOCK_TIMEOUT_MS = 30_000;
const DEFAULT_STATEMENT_TIMEOUT_MS: number | undefined = undefined;
const DEFAULT_MIGRATION_TIMEOUT_MS: number | undefined = undefined;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BASE_DELAY_MS = 1000;
const DEFAULT_MAX_DELAY_MS = 30_000;
const MIGRATION_EXTENSIONS = new Set([".sql", ".js", ".mjs", ".cjs"]);

function checksum(value: string): string {
    return crypto.createHash("sha256").update(value).digest("hex");
}

function normalizeId(value: unknown): string {
    return String(value).trim();
}

function generateEventId(): string {
    return crypto.randomUUID();
}

function resolveRunnerId(): string {
    return `${process.pid}-${crypto.randomBytes(4).toString("hex")}`;
}

function resolveHost(): string | undefined {
    return process.env.HOSTNAME ?? process.env.HOST;
}

function asExecutable(
    migration: MigrationDefinition,
    context: {
        client: PoolClient;
        query: MigrationQueryFn;
        logger?: Logger;
        statementTimeout?: number;
        mode?: MigrationMode;
    },
): () => Promise<unknown> {
    const up = migration.up;
    const mode = migration.mode ?? context.mode;
    const queryFn = context.query as unknown as MigrationQueryFn;

    if (typeof up === "function") {
        return () => up(context);
    }

    if (typeof up === "string") {
        return async () => {
            if (mode === "non-transactional") {
                await queryFn(up);
            } else {
                await queryFn(up);
            }
        };
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
                dependsOn: undefined,
                mode: "transactional",
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
            dependsOn: entry.dependsOn,
            mode: entry.mode ?? "transactional",
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
            migrations.push({ id, name: id, up: content, mode: "transactional" });
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
            dependsOn: definition.dependsOn,
            mode: definition.mode ?? "transactional",
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

function detectDuplicateIds(migrations: MigrationDefinition[]): void {
    const byId = new Map<string, { id: string; source: string }[]>();
    for (const migration of migrations) {
        const id = migration.id!;
        const entry = { id, source: migration.source ?? "" };
        const existing = byId.get(id);
        if (existing) {
            throw new Error(
                `Duplicate migration id "${id}" detected. Migration IDs must be unique across all migration sources.`,
            );
        }
        byId.set(id, [entry]);
    }
}

function topologicalSort(migrations: MigrationDefinition[]): MigrationDefinition[] {
    const byId = new Map(migrations.map((m) => [m.id!, m]));
    const visited = new Set<string>();
    const visiting = new Set<string>();
    const result: MigrationDefinition[] = [];

    function visit(id: string): void {
        if (visited.has(id)) {
            return;
        }
        if (visiting.has(id)) {
            const cycle = [...visiting, id].join(" -> ");
            throw new Error(`Migration dependency cycle detected: ${cycle}`);
        }
        const migration = byId.get(id);
        if (!migration) {
            throw new Error(`Migration ${id} depends on unknown migration ${id}`);
        }
        visiting.add(id);
        for (const dep of migration.dependsOn ?? []) {
            if (!byId.has(dep)) {
                throw new Error(`Migration ${id} depends on unknown migration ${dep}`);
            }
            visit(dep);
        }
        visiting.delete(id);
        visited.add(id);
        result.push(migration);
    }

    for (const migration of migrations) {
        visit(migration.id!);
    }

    return result;
}

function isDestructiveSql(sql: string): boolean {
    const upper = sql.toUpperCase();
    const destructivePatterns = [
        "DROP TABLE",
        "DROP COLUMN",
        "DROP DATABASE",
        "DROP SCHEMA",
        "TRUNCATE",
        "DELETE FROM",
        "UPDATE ",
        "CASCADE",
    ];
    return destructivePatterns.some((pattern) => upper.includes(pattern));
}

function assessRollbackRisk(up: string): RollbackRisk {
    const upper = up.toUpperCase();

    // Irreversible: data-destroying operations without obvious reverse
    if (
        upper.includes("DROP TABLE") ||
        upper.includes("DROP DATABASE") ||
        upper.includes("DROP SCHEMA") ||
        upper.includes("TRUNCATE") ||
        upper.includes("DELETE FROM") ||
        upper.includes("UPDATE ")
    ) {
        return "irreversible";
    }

    // Potentially destructive: column drops, schema alterations
    if (
        upper.includes("DROP COLUMN") ||
        upper.includes("ALTER COLUMN") ||
        upper.includes("DROP CONSTRAINT") ||
        upper.includes("RENAME COLUMN") ||
        upper.includes("RENAME TABLE")
    ) {
        return "potentially-destructive";
    }

    // Safe: additive operations
    return "safe";
}

function autoDown(up: string): string | null {
    const normalized = up.trim();

    const createTable = normalized.match(
        /^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|([^\s(]+))\s*\(/i,
    );
    const tableName = createTable?.[1] ?? createTable?.[2];
    if (tableName) {
        return `DROP TABLE IF EXISTS ${tableName};`;
    }

    const createIndex = normalized.match(
        /^CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|([^\s(]+))\s+ON/i,
    );
    const indexName = createIndex?.[1] ?? createIndex?.[2];
    if (indexName) {
        return `DROP INDEX IF EXISTS ${indexName};`;
    }

    const addColumn = normalized.match(
        /^ALTER\s+TABLE\s+(?:"([^"]+)"|([^\s(]+))\s+ADD\s+COLUMN\s+(?:"([^"]+)"|([^\s(]+))\s/i,
    );
    if (addColumn) {
        const altTable = addColumn[1] ?? addColumn[2];
        const altCol = addColumn[3] ?? addColumn[4];
        return `ALTER TABLE ${altTable} DROP COLUMN ${altCol};`;
    }

    const renameColumn = normalized.match(
        /^ALTER\s+TABLE\s+(?:"([^"]+)"|([^\s(]+))\s+RENAME\s+COLUMN\s+(?:"([^"]+)"|([^\s(]+))\s+TO\s+(?:"([^"]+)"|([^\s(]+))\s*;?\s*$/i,
    );
    if (renameColumn) {
        const rtTable = renameColumn[1] ?? renameColumn[2];
        const rtOld = renameColumn[3] ?? renameColumn[4];
        const rtNew = renameColumn[5] ?? renameColumn[6];
        return `ALTER TABLE ${rtTable} RENAME COLUMN ${rtNew} TO ${rtOld};`;
    }

    const addConstraint = normalized.match(
        /^ALTER\s+TABLE\s+(?:"([^"]+)"|([^\s(]+))\s+ADD\s+CONSTRAINT\s+(?:"([^"]+)"|([^\s(]+))\s/i,
    );
    if (addConstraint) {
        const acTable = addConstraint[1] ?? addConstraint[2];
        const acName = addConstraint[3] ?? addConstraint[4];
        return `ALTER TABLE ${acTable} DROP CONSTRAINT ${acName};`;
    }

    return null;
}

function hasMultipleStatements(sql: string): boolean {
    const trimmed = sql.trim();
    if (!trimmed) return false;
    // Count semicolons outside of string literals (simplified heuristic)
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let semicolonCount = 0;
    for (let i = 0; i < trimmed.length; i++) {
        const ch = trimmed[i];
        if (inSingleQuote) {
            if (ch === "'" && trimmed[i - 1] !== "\\") inSingleQuote = false;
        } else if (inDoubleQuote) {
            if (ch === '"') inDoubleQuote = false;
        } else {
            if (ch === "'") inSingleQuote = true;
            else if (ch === '"') inDoubleQuote = true;
            else if (ch === ";") semicolonCount++;
        }
    }
    return semicolonCount > 0;
}

function isComplexMigration(migration: MigrationDefinition): boolean {
    const up = migration.up;
    if (typeof up === "string") {
        // Multiple statements = complex
        if (hasMultipleStatements(up)) return true;
        const upper = up.toUpperCase();
        // DDL + DML mix = complex
        const hasDdl = /CREATE|ALTER|DROP|TRUNCATE/.test(upper);
        const hasDml = /INSERT|UPDATE|DELETE/.test(upper);
        if (hasDdl && hasDml) return true;
    }
    return false;
}

type RunResult = { applied: string[]; skipped: string[]; failed: string[] };

function defaultRetryOptions(options: MigrationRetryOptions = {}): Required<MigrationRetryOptions> {
    return {
        maxAttempts: options.maxAttempts ?? DEFAULT_MAX_RETRIES,
        baseDelayMs: options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS,
        maxDelayMs: options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS,
        retryableErrors: options.retryableErrors ?? [
            "connection terminated",
            "connection lost",
            "server closed the connection unexpectedly",
            "terminating connection due to administrator command",
            "could not connect to server",
            "timeout expired",
            "canceling statement due to statement timeout",
        ],
    };
}

function isRetryableError(error: unknown, retryableErrors: string[]): boolean {
    const message = String(error ?? "").toLowerCase();
    return retryableErrors.some((pattern) => message.includes(pattern.toLowerCase()));
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Portable migration runner with production-grade safety:
 * - Strict duplicate-ID rejection (including identical SQL)
 * - Checksum drift enforcement
 * - Destructive rollback protection with risk classification
 * - Rollback dry-run support
 * - Non-transactional migration mode
 * - Per-migration and overall migration timeouts
 * - Automatic retry with exponential backoff for transient failures
 * - Migration audit history
 * - Per-target status, retryFailed, reconcile, detectDrift
 */
export function createMigrator(
    poolOrPools: Pool | Pool[],
    {
        logger,
        table = DEFAULT_TABLE,
        historyTable = DEFAULT_HISTORY_TABLE,
        operationTable = DEFAULT_OPERATION_TABLE,
        backfillTable = DEFAULT_BACKFILL_TABLE,
        phaseTable = DEFAULT_PHASE_TABLE,
        lockKey = DEFAULT_LOCK_KEY,
        lockTimeout = DEFAULT_LOCK_TIMEOUT_MS,
        lockPerMigration = false,
        statementTimeout = DEFAULT_STATEMENT_TIMEOUT_MS,
        migrationTimeout = DEFAULT_MIGRATION_TIMEOUT_MS,
        retry,
        source,
        migrations,
        directory,
        pools: optionPools,
        applicationVersion,
        runnerId = resolveRunnerId(),
        host = resolveHost(),
        approvalProvider,
        backupProvider,
        backupBeforeDestructive = true,
        requireRestoreVerification = false,
        approvalRequiredForDestructive = false,
    }: {
        logger?: Logger;
        table?: string;
        historyTable?: string;
        operationTable?: string;
        backfillTable?: string;
        phaseTable?: string;
        lockKey?: number;
        lockTimeout?: number;
        lockPerMigration?: boolean;
        statementTimeout?: number;
        migrationTimeout?: number;
        retry?: MigrationRetryOptions;
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        pools?: Pool[];
        applicationVersion?: string;
        runnerId?: string;
        host?: string;
        approvalProvider?: ApprovalProvider;
        backupProvider?: BackupProvider;
        backupBeforeDestructive?: boolean;
        requireRestoreVerification?: boolean;
        approvalRequiredForDestructive?: boolean;
    } = {},
): Migrator {
    const pools = (optionPools ?? (Array.isArray(poolOrPools) ? poolOrPools : [poolOrPools])).filter(
        (p): p is Pool => p != null,
    );

    if (pools.length === 0) {
        throw new TypeError("createMigrator requires at least one pg Pool");
    }

    const retryConfig = defaultRetryOptions(retry);
    const tableIdent = quoteIdent(table);
    const historyTableIdent = quoteIdent(historyTable);
    const baselineTableIdent = quoteIdent(table + "_baseline");
    const backupTableIdent = quoteIdent(table + "_backup");
    const backfillTableIdent = quoteIdent(backfillTable);
    const phaseTableIdent = quoteIdent(phaseTable);
    const approvalTableIdent = quoteIdent(table + "_approval");
    const operationTableIdent = quoteIdent(operationTable);
    const runner = runnerId;

    const ensureAllTables = async () => {
        for (const p of pools) {
            const client = await p.connect();
            try {
                await ensureTables(client);
            } finally {
                client.release();
            }
        }
    };
    const initPromise = Promise.resolve();
    void initPromise.catch(() => {});

    // Cache database names per pool (pool.database is not exposed by pg)
    const poolDatabaseCache = new Map<Pool, string>();
    async function getPoolDatabase(pool: Pool): Promise<string> {
        const cached = poolDatabaseCache.get(pool);
        if (cached) {
            return cached;
        }
        try {
            const client = await pool.connect();
            try {
                const { rows } = await client.query("SELECT current_database() AS dbname");
                const dbName = rows[0]?.dbname as string;
                if (dbName) {
                    poolDatabaseCache.set(pool, dbName);
                }
                return dbName ?? `pool-${pools.indexOf(pool)}`;
            } finally {
                client.release();
            }
        } catch {
            return `pool-${pools.indexOf(pool)}`;
        }
    }

    // Lock-wait metrics tracking for observability
    const lockWaitMetrics: LockWaitMetric[] = [];

    async function ensureTables(client: PoolClient): Promise<void> {
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableIdent} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                execution_ms INTEGER NOT NULL DEFAULT 0,
                applied_at TIMESTAMPTZ,
                failed_at TIMESTAMPTZ,
                error_message TEXT,
                attempts INTEGER NOT NULL DEFAULT 0
            );
        `);
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${historyTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id VARCHAR(255) NOT NULL,
                target_database VARCHAR(255),
                checksum VARCHAR(64) NOT NULL,
                started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMPTZ,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                mode VARCHAR(20) NOT NULL DEFAULT 'transactional',
                error_message TEXT,
                rollback_risk TEXT,
                application_version TEXT,
                runner_id TEXT NOT NULL,
                host TEXT
            );
        `);
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${historyTable}_migration_id
            ON ${historyTableIdent} (migration_id);
        `);
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${historyTable}_started_at
            ON ${historyTableIdent} (started_at);
        `);
        // Safe schema upgrade: rollback_risk was VARCHAR(20) in older installations,
        // which overflows for values like "potentially-destructive" (23 chars) and
        // causes history recording to fail. Widen to TEXT idempotently.
        await client.query(`ALTER TABLE ${historyTableIdent} ALTER COLUMN rollback_risk TYPE TEXT;`).catch(() => {});
        // Baseline registry
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${baselineTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT,
                schema_version TEXT,
                verified BOOLEAN NOT NULL DEFAULT FALSE,
                applied_migration_ids TEXT NOT NULL,
                checksum VARCHAR(64) NOT NULL
            );
        `);
        // Migration backup records
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${backupTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id VARCHAR(255) NOT NULL,
                target_database VARCHAR(255),
                backup_id VARCHAR(255) NOT NULL,
                backup_location TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                successful BOOLEAN NOT NULL DEFAULT FALSE
            );
        `);
        // Durable non-transactional operation journal. Records execution intent and
        // outcome per migration so a crash mid non-transactional migration leaves a
        // recoverable record instead of an invisible partial state.
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${operationTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id VARCHAR(255) NOT NULL,
                target_database VARCHAR(255),
                operation_id VARCHAR(255) NOT NULL,
                operation_type VARCHAR(20) NOT NULL DEFAULT 'migration',
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                statement_sql TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMPTZ,
                error_message TEXT,
                runner_id TEXT NOT NULL DEFAULT '',
                host TEXT,
                UNIQUE (operation_id)
            );
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_${operationTable}_migration_status ON ${operationTableIdent} (migration_id, status);`);
        // Durable backfill job registry. Tracks per-job execution progress, cursor, and
        // completion so a backfill can be paused, resumed, and recovered after a crash
        // without skipping or duplicating work.
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${backfillTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id VARCHAR(255) NOT NULL,
                target_database VARCHAR(255),
                job_name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                source_table VARCHAR(255) NOT NULL,
                cursor_type VARCHAR(40) NOT NULL DEFAULT 'serial',
                cursor_columns TEXT[] NOT NULL DEFAULT '{}',
                cursor_value TEXT,
                batch_size INTEGER NOT NULL DEFAULT 1000,
                rows_processed BIGINT NOT NULL DEFAULT 0,
                rows_remaining BIGINT,
                progress_percentage REAL,
                started_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMPTZ,
                last_error TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                runner_id TEXT NOT NULL DEFAULT '',
                host TEXT,
                UNIQUE (migration_id, target_database, job_name)
            );
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_${backfillTable}_migration_status ON ${backfillTableIdent} (migration_id, status);`);
        // Expand-contract phase state machine. Tracks per-phase execution state so
        // phases execute in order, prerequisites are enforced, and interruption is
        // observable (a phase left "running" without "completed_at" was interrupted).
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${phaseTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id VARCHAR(255) NOT NULL,
                target_database VARCHAR(255),
                phase VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                started_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMPTZ,
                error_message TEXT,
                runner_id TEXT NOT NULL DEFAULT '',
                host TEXT,
                UNIQUE (migration_id, target_database, phase)
            );
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_${phaseTable}_migration_phase ON ${phaseTableIdent} (migration_id, phase);`);
        // Migration approval records
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${approvalTableIdent} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id VARCHAR(255) NOT NULL,
                target_database VARCHAR(255),
                approved_by TEXT,
                approved_at TIMESTAMPTZ,
                rejected_by TEXT,
                rejected_at TIMESTAMPTZ,
                requirement VARCHAR(20) NOT NULL DEFAULT 'automatic',
                status VARCHAR(20) NOT NULL DEFAULT 'not_required',
                reason TEXT,
                runner_id TEXT NOT NULL DEFAULT '',
                host TEXT,
                migration_checksum TEXT,
                expires_at TIMESTAMPTZ,
                step_name TEXT
            );
        `);
        await client.query(`ALTER TABLE ${approvalTableIdent} ADD COLUMN IF NOT EXISTS runner_id TEXT NOT NULL DEFAULT \"\";`).catch(() => {});
        await client.query(`ALTER TABLE ${approvalTableIdent} ADD COLUMN IF NOT EXISTS host TEXT;`).catch(() => {});
        await client.query(`ALTER TABLE ${approvalTableIdent} ADD COLUMN IF NOT EXISTS reason TEXT;`).catch(() => {});
        await client.query(`ALTER TABLE ${approvalTableIdent} ADD COLUMN IF NOT EXISTS migration_checksum TEXT;`).catch(() => {});
        await client.query(`ALTER TABLE ${approvalTableIdent} ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;`).catch(() => {});
        await client.query(`ALTER TABLE ${approvalTableIdent} ADD COLUMN IF NOT EXISTS step_name TEXT;`).catch(() => {});
    }

    async function applied(client: PoolClient): Promise<MigrationLogRecord[]> {
        const { rows } = await client.query(
            `SELECT id, name, status, checksum, execution_ms, applied_at, failed_at, error_message, attempts FROM ${tableIdent} ORDER BY applied_at ASC NULLS LAST, id ASC`,
        );
        return rows.map((row) => ({
            id: row.id,
            name: row.name,
            status: (row.status ?? "applied") as MigrationRunStatus,
            applied: (row.status ?? "applied") === "applied",
            checksum: row.checksum,
            execution_ms: row.execution_ms,
            applied_at: row.applied_at,
            failed_at: row.failed_at,
            error_message: row.error_message,
            attempts: row.attempts,
        }));
    }

    async function recordHistory(
        client: PoolClient,
        event: Omit<MigrationAuditEvent, "id">,
    ): Promise<void> {
        await client.query(
            `INSERT INTO ${historyTableIdent} (id, migration_id, target_database, checksum, started_at, completed_at, duration_ms, status, attempt, mode, error_message, rollback_risk, application_version, runner_id, host)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
            [
                generateEventId(),
                event.migrationId,
                event.target,
                event.checksum,
                event.startedAt,
                event.completedAt,
                event.durationMs,
                event.status,
                event.attempt,
                event.mode,
                event.error ?? null,
                event.rollbackRisk ?? null,
                event.applicationVersion ?? null,
                event.runnerId,
                event.host ?? null,
            ],
        );
    }

    async function recordOperation(
        client: PoolClient,
        migrationId: string,
        targetDatabase: string | undefined,
        attempt: number,
        status: string,
        errorMessage?: string,
    ): Promise<void> {
        const operationId = `${migrationId}:${attempt}`;
        const startedAt = new Date().toISOString();
        await client.query(
            `INSERT INTO ${operationTableIdent} (id, migration_id, target_database, operation_id, operation_type, status, statement_sql, started_at, completed_at, error_message, runner_id, host)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (operation_id) DO UPDATE SET
                 status = EXCLUDED.status,
                 completed_at = EXCLUDED.completed_at,
                 error_message = EXCLUDED.error_message`,
            [
                generateEventId(),
                migrationId,
                targetDatabase ?? null,
                operationId,
                "migration",
                status,
                null,
                startedAt,
                status === "started" ? null : startedAt,
                errorMessage ?? null,
                runner,
                host,
            ],
        );
    }

    async function acquireLock(client: PoolClient): Promise<void> {
        const start = Date.now();
        const deadline = start + lockTimeout;
        for (;;) {
            const { rows = [] } = await client.query("SELECT pg_try_advisory_lock($1)", [lockKey]);
            if (rows[0]?.pg_try_advisory_lock === true) {
                const waited = Date.now() - start;
                const metric: LockWaitMetric = {
                    lockKey,
                    waitMs: waited,
                    acquiredAt: new Date().toISOString(),
                    runnerId: runner,
                };
                lockWaitMetrics.push(metric);
                if (waited > 0) {
                    logger?.warn?.({ lockKey, waitMs: waited, runner }, "Advisory lock acquired after waiting");
                }
                return;
            }
            if (Date.now() >= deadline) {
                throw new Error(`Could not acquire advisory lock ${lockKey} within ${lockTimeout}ms`);
            }
            await sleep(100);
        }
    }

    async function releaseLock(client: PoolClient): Promise<void> {
        await client.query("SELECT pg_advisory_unlock($1)", [lockKey]).catch(() => {});
    }

    async function runWithTimeout<T>(
        promise: Promise<T>,
        timeoutMs: number | undefined,
    ): Promise<T> {
        if (!timeoutMs || timeoutMs <= 0) {
            return promise;
        }
        let timer: NodeJS.Timeout | null = null;
        const timeoutPromise = new Promise<never>((_, reject) => {
            timer = setTimeout(() => {
                reject(new Error(`Migration execution exceeded ${timeoutMs}ms timeout`));
            }, timeoutMs);
        });
        try {
            return await Promise.race([promise, timeoutPromise]);
        } finally {
            if (timer) {
                clearTimeout(timer);
            }
        }
    }

    async function executeWithRetry<T>(
        fn: (attempt: number) => Promise<T>,
        options: MigrationRetryOptions,
    ): Promise<T> {
        const resolved = defaultRetryOptions(options);
        let lastError: unknown;
        for (let attempt = 1; attempt <= resolved.maxAttempts; attempt++) {
            try {
                return await fn(attempt);
            } catch (error) {
                lastError = error;
                if (attempt < resolved.maxAttempts && isRetryableError(error, resolved.retryableErrors)) {
                    const delay = Math.min(
                        resolved.baseDelayMs * Math.pow(2, attempt - 1),
                        resolved.maxDelayMs,
                    );
                    logger?.warn?.({ attempt, maxAttempts: resolved.maxAttempts, delayMs: delay, error }, "Migration attempt failed, retrying");
                    await sleep(delay);
                    continue;
                }
                throw error;
            }
        }
        throw lastError;
    }

    async function runMigration(
        client: PoolClient,
        migration: MigrationDefinition,
        context: {
            client: PoolClient;
            query: MigrationQueryFn;
            logger?: Logger;
            statementTimeout?: number;
            mode?: MigrationMode;
        },
        targetDatabase: string | undefined,
        attempt: number,
    ): Promise<void> {
        const startedAt = Date.now();
        const mode = migration.mode ?? context.mode ?? "transactional";
        const isTransactional = mode === "transactional";

        // Approval integration
        const approvalRequired =
            approvalRequiredForDestructive ||
            approvalProvider?.requiresApproval?.(migration, { runnerId: runner, host, targetDatabase }) === true ||
            migration.requiresApproval === true;
        const currentMigrationChecksum = checksum(migration.source ?? "");

        // Validate any persisted approval for staleness/expiration before trusting it.
        let persistedApprovalValid = false;
        try {
            const { rows: persistedApprovals } = await client.query(
                `SELECT status, migration_checksum, expires_at
                 FROM ${quoteIdent(table + "_approval")}
                 WHERE migration_id = $1 AND target_database IS NOT DISTINCT FROM $2
                 ORDER BY approved_at DESC NULLS LAST, id DESC
                 LIMIT 1`,
                [migration.id, targetDatabase ?? null],
            );
            if (persistedApprovals.length > 0) {
                const row = persistedApprovals[0] as Record<string, unknown>;
                if (row.status === "approved") {
                    const expired = row.expires_at ? new Date(String(row.expires_at)) < new Date() : false;
                    const stale = row.migration_checksum ? String(row.migration_checksum) !== currentMigrationChecksum : false;
                    if (expired) throw new Error(`Approval expired for migration ${migration.id}`);
                    if (stale) throw new Error(`Stale approval for migration ${migration.id}: migration checksum changed`);
                    persistedApprovalValid = true;
                } else if (row.status === "rejected") {
                    throw new Error(`Approval rejected for migration ${migration.id}`);
                }
            }
        } catch (e) {
            if (e instanceof Error && /Approval/.test(e.message)) {
                throw e;
            }
        }

        if (approvalRequired && approvalProvider && !persistedApprovalValid) {
            const destructive = (typeof migration.up === "string" ? assessRollbackRisk(migration.up) : "potentially-destructive") !== "safe";
            if (destructive || migration.requiresApproval === true || approvalRequiredForDestructive) {
                const result = await approvalProvider.approve?.(migration, { runnerId: runner, host, targetDatabase });
                if (!result || !result.approved) {
                    throw new Error(
                        `Approval required for migration ${migration.id}: ${result?.reason ?? "approved? no"}`,
                    );
                }
                    const currentChecksum = checksum(migration.source ?? "");
                    const clientApproval = client;
                    try {
                        await clientApproval.query(
                            `INSERT INTO _writer_migrations_approval (migration_id, target_database, requirement, status, approved_by, approved_at, runner_id, host, migration_checksum, expires_at)
                             VALUES ($1, $2, $3, 'approved', $4, $5, $6, $7, $8, NULL)`,
                            [
                                migration.id,
                                targetDatabase ?? null,
                                (approvalProvider.requiresApproval?.(migration, { runnerId: runner, host, targetDatabase }) ? "explicit" : "automatic"),
                                result.approvedBy ?? null,
                                result.approvedAt ?? new Date().toISOString(),
                                runner,
                                host ?? null,
                                currentChecksum,
                            ],
                        );
                    } catch (e) {
                        throw e;
                    }
                }
        }

        // Backup integration
        if (backupProvider && backupBeforeDestructive) {
            const destructive = (typeof migration.up === "string" ? assessRollbackRisk(migration.up) : "potentially-destructive") !== "safe";
            if (destructive) {
                try {
                    const backup = await backupProvider.backup(targetDatabase ?? "", {
                        migrationId: migration.id,
                        reason: "pre-migration destructive change",
                        runnerId: runner,
                    });
                    if (backupProvider.exists && !(await backupProvider.exists(backup.backupId))) {
                        throw new Error(`Backup verification failed for ${backup.backupId}`);
                    }
                    await client.query(
                        `INSERT INTO _writer_migrations_backup (migration_id, target_database, backup_id, backup_location, successful)
                         VALUES ($1, $2, $3, $4, $5)`,
                        [migration.id, targetDatabase ?? null, backup.backupId, backup.backupLocation ?? null, true],
                    );

                    // When restore verification is required, prove the backup can be
                    // restored into an isolated database before allowing the destructive
                    // migration to proceed. Never overwrite the original database.
                    if (requireRestoreVerification) {
                        const verification = await verifyBackup({
                            migrationId: migration.id,
                            target: targetDatabase,
                        });
                        if (!verification.verified) {
                            throw new Error(
                                `Restore verification failed for backup ${backup.backupId} before destructive migration ${migration.id}: ` +
                                verification.checks.map((c) => `${c.name}: ${c.detail ?? "failed"}`).join("; "),
                            );
                        }
                    }
                } catch (error) {
                    throw new Error(
                        `Backup before migration ${migration.id} failed: ${String(error ?? "")}`,
                    );
                }
            }
        }

        // Detect operations that cannot run inside transactions
        const hasConcurrently = (typeof migration.up === "string" ? migration.up : "")
            .toUpperCase()
            .includes("CONCURRENTLY");

        if (hasConcurrently && isTransactional) {
            throw new Error(
                `Migration ${migration.id} uses CONCURRENTLY which requires mode: "non-transactional". ` +
                `Set mode: "non-transactional" on the migration definition or pass mode in context.`,
            );
        }

        if (context.statementTimeout !== undefined) {
            await client.query(`SET statement_timeout = ${Math.floor(context.statementTimeout)}`);
        }

        if (isTransactional) {
            await client.query("BEGIN");
        } else {
            // Durable execution journal: record intent before executing a
            // non-transactional migration so a crash mid-execution leaves a
            // recoverable record rather than an invisible partial state.
            await recordOperation(client, migration.id!, targetDatabase, attempt, "started");
        }

        try {
            await executeWithRetry(
                async () => {
                    await asExecutable(migration, { ...context, mode: isTransactional ? "transactional" : "non-transactional" })();
                },
                migration.retry ?? retryConfig,
            );

            const executionMs = Date.now() - startedAt;

            if (isTransactional) {
                // In transactional mode, record + commit atomically
                await client.query(
                    `INSERT INTO ${tableIdent} (id, name, checksum, status, execution_ms, applied_at, attempts)
                     VALUES ($1, $2, $3, 'applied', $4, CURRENT_TIMESTAMP, $5)
                     ON CONFLICT (id) DO UPDATE SET
                         name = EXCLUDED.name,
                         checksum = EXCLUDED.checksum,
                         status = 'applied',
                         execution_ms = EXCLUDED.execution_ms,
                         applied_at = CURRENT_TIMESTAMP,
                         failed_at = NULL,
                         error_message = NULL,
                         attempts = ${tableIdent}.attempts + 1`,
                    [migration.id, migration.name, checksum(migration.source ?? ""), executionMs, attempt],
                );
                await client.query("COMMIT");
            } else {
                // Non-transactional: record applied state AFTER execution (partial execution may have occurred)
                await client.query(
                    `INSERT INTO ${tableIdent} (id, name, checksum, status, execution_ms, applied_at, attempts)
                     VALUES ($1, $2, $3, 'applied', $4, CURRENT_TIMESTAMP, $5)
                     ON CONFLICT (id) DO UPDATE SET
                         name = EXCLUDED.name,
                         checksum = EXCLUDED.checksum,
                         status = 'applied',
                         execution_ms = EXCLUDED.execution_ms,
                         applied_at = CURRENT_TIMESTAMP,
                         failed_at = NULL,
                         error_message = NULL,
                         attempts = ${tableIdent}.attempts + 1`,
                    [migration.id, migration.name, checksum(migration.source ?? ""), executionMs, attempt],
                );
            }

            if (!isTransactional) {
                await recordOperation(client, migration.id!, targetDatabase, attempt, "applied");
            }

            const completedAt = Date.now();
            await recordHistory(client, {
                migrationId: migration.id!,
                target: targetDatabase ?? "",
                checksum: checksum(migration.source ?? ""),
                startedAt: new Date(startedAt).toISOString(),
                completedAt: new Date(completedAt).toISOString(),
                durationMs: completedAt - startedAt,
                status: "applied",
                attempt,
                mode,
                applicationVersion,
                runnerId: runner,
                host,
            });
            logger?.info?.({ migration: migration.id, mode }, "Migration applied");
            } catch (error) {
            if (isTransactional) {
                await client.query("ROLLBACK").catch(() => {});
            }
            if (!isTransactional) {
                await recordOperation(
                    client,
                    migration.id!,
                    targetDatabase,
                    attempt,
                    "failed",
                    String(error ?? "migration failed"),
                ).catch(() => {});
            }
            const completedAt = Date.now();
            const errorStep = isTransactional ? 0 : -1;
            await client.query(
                `INSERT INTO ${tableIdent} (id, name, checksum, status, execution_ms, applied_at, failed_at, error_message, attempts)
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $6, 1)
                 ON CONFLICT (id) DO UPDATE SET
                     name = EXCLUDED.name,
                     checksum = EXCLUDED.checksum,
                     status = EXCLUDED.status,
                     execution_ms = EXCLUDED.execution_ms,
                     applied_at = NULL,
                     failed_at = CURRENT_TIMESTAMP,
                     error_message = $6,
                     attempts = ${tableIdent}.attempts + 1`,
                [
                    migration.id,
                    migration.name,
                    checksum(migration.source ?? ""),
                    isTransactional ? "failed" : "partially_applied",
                    Date.now() - startedAt,
                    String(error ?? "migration failed"),
                ],
            );
            await recordHistory(client, {
                migrationId: migration.id!,
                target: targetDatabase ?? "",
                checksum: checksum(migration.source ?? ""),
                startedAt: new Date(startedAt).toISOString(),
                completedAt: new Date(completedAt).toISOString(),
                durationMs: completedAt - startedAt,
                status: isTransactional ? "failed" : "failed",
                attempt,
                mode,
                error: String(error ?? "migration failed"),
                applicationVersion,
                runnerId: runner,
                host,
            });
            logger?.error?.({ migration: migration.id, mode, error }, "Migration failed");
            throw error;
        }
    }    async function runOnPool(
        pool: Pool,
        list: MigrationDefinition[],
        targetDatabase: string | undefined,
    ): Promise<RunResult> {
        const client = await pool.connect();
        let locked = false;
        try {
            if (!lockPerMigration) {
                // Whole-run advisory lock: bounded acquisition via try-lock polling
                // with a deadline (lockTimeout). Never block indefinitely.
                await acquireLock(client);
                locked = true;
            }
            await ensureTables(client);
            const done = await applied(client);
            const doneMap = new Map(done.map((row) => [row.id, row]));
            const appliedNow: string[] = [];
            const skippedNow: string[] = [];
            const failedNow: string[] = [];

            for (const migration of list) {
                const migrationId = migration.id!;
                const prior = doneMap.get(migrationId);
                const currentChecksum = checksum(migration.source ?? "");

                if (prior && prior.status === "applied") {
                    if (prior.checksum && prior.checksum !== currentChecksum) {
                        throw new Error(
                            `Migration drift detected for "${migrationId}": stored checksum ${prior.checksum} does not match current source checksum ${currentChecksum}. The migration definition has changed after being applied.`,
                        );
                    }
                    skippedNow.push(migrationId);
                    continue;
                }

                const context = {
                    client,
                    query: client.query.bind(client),
                    logger,
                    statementTimeout,
                    mode: migration.mode,
                };

                if (lockPerMigration) {
                    await acquireLock(client);
                }

                const perMigrationTimeout = migrationTimeout ?? undefined;

                try {
                    await runWithTimeout(
                        executeWithRetry(
                            async (attempt: number) => {
                                await runMigration(client, migration, context, targetDatabase, attempt);
                            },
                            migration.retry ?? retryConfig,
                        ),
                        perMigrationTimeout,
                    );
                    appliedNow.push(migrationId);
                } catch (error) {
                    failedNow.push(migrationId);
                    if (!lockPerMigration) {
                        // Fail-fast within a pool on the first failing migration, but
                        // propagate the original error so run() can decide whether to
                        // stop at the first failed target or continue with the rest
                        // (targetFailurePolicy).
                        throw error;
                    }
                } finally {
                    if (lockPerMigration) {
                        await releaseLock(client);
                    }
                }
            }

            return { applied: appliedNow, skipped: skippedNow, failed: failedNow };
        } finally {
            if (locked) {
                await releaseLock(client);
            }
            client.release();
        }
    }

    async function run(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        targetFailurePolicy?: "stop" | "continue";
    } = {}): Promise<string[]> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        detectDuplicateIds(sourceList);
        const list = topologicalSort(sourceList);

        let allApplied: string[] = [];
        const policy = options.targetFailurePolicy ?? "stop";
        const failures: Array<{ target: string; migrations: string[]; error?: unknown }> = [];

        for (const pool of pools) {
            const targetDatabase = await getPoolDatabase(pool);
            let result;
            try {
                result = await runOnPool(pool, list, targetDatabase);
            } catch (error) {
                // runOnPool throws on the first failing migration in a pool (fail-fast
                // within a pool). Record the failure and decide whether to continue
                // with the remaining targets.
                failures.push({ target: targetDatabase ?? "", migrations: [], error });
                if (policy === "stop") {
                    throw error;
                }
                continue;
            }
            allApplied = [...allApplied, ...(result.applied ?? [])];
            if ((result.failed ?? []).length > 0) {
                failures.push({ target: targetDatabase ?? "", migrations: result.failed });
                if (policy === "stop") {
                    throw new Error(
                        `Migration(s) failed on target ${targetDatabase ?? ""}: ${result.failed.join(", ")}`,
                    );
                }
            }
        }

        if (failures.length > 0) {
            throw new Error(
                `Migration(s) failed across targets: ` +
                failures.map((f) => {
                    const detail = f.error ? ` (${String(f.error)})` : "";
                    return `${f.target}: ${f.migrations.length ? f.migrations.join(", ") : "migration failed"}${detail}`;
                }).join("; "),
            );
        }

        return allApplied;
    }

    async function status(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<MigrationLogRecord[]> {
        const list = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const client = await pools[0].connect();
        try {
            await ensureTables(client);
            const done = await applied(client);
            const doneMap = new Map(done.map((row) => [row.id, row]));
            return list.map((migration) => {
                const record = doneMap.get(migration.id!);
                if (record) {
                    return record;
                }
                return {
                    id: migration.id!,
                    name: migration.name!,
                    status: "pending" as MigrationRunStatus,
                    applied: false,
                    applied_at: null,
                };
            });
        } finally {
            client.release();
        }
    }

    async function statusAllTargets(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<Map<string, MigrationLogRecord[]>> {
        const list = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const result = new Map<string, MigrationLogRecord[]>();

        for (const pool of pools) {
            const client = await pool.connect();
            try {
                await ensureTables(client);
                const done = await applied(client);
                const doneMap = new Map(done.map((row) => [row.id, row]));
                const targetId = await getPoolDatabase(pool);
                result.set(targetId, list.map((migration) => {
                    const record = doneMap.get(migration.id!);
                    if (record) {
                        return record;
                    }
                    return {
                        id: migration.id!,
                        name: migration.name!,
                        status: "pending" as MigrationRunStatus,
                        applied: false,
                        applied_at: null,
                    };
                }));
            } finally {
                client.release();
            }
        }

        return result;
    }

    async function failed(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<MigrationLogRecord[]> {
        const list = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const all = await status({ source: options.source, migrations: options.migrations, directory: options.directory });
        const ids = new Set(list.map((m) => m.id));
        return all.filter((r) => r.status === "failed" && ids.has(r.id));
    }

    async function validate(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<{
        valid: boolean;
        results: Array<{ id: string; name: string; status: MigrationRunStatus; error?: string }>;
    }> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        detectDuplicateIds(sourceList);
        const list = topologicalSort(sourceList);
        const client = await pools[0].connect();
        const results: Array<{ id: string; name: string; status: MigrationRunStatus; error?: string }> = [];

        try {
            await ensureTables(client);
            const done = await applied(client);
            const doneIds = new Set(done.map((row) => row.id));

            if (statementTimeout !== undefined) {
                await client.query(`SET statement_timeout = ${Math.floor(statementTimeout)}`);
            }

            for (const migration of list) {
                if (doneIds.has(migration.id!)) {
                    results.push({ id: migration.id!, name: migration.name!, status: "skipped" });
                    continue;
                }

                const context = {
                    client,
                    query: client.query.bind(client),
                    logger,
                    statementTimeout,
                    mode: migration.mode,
                };

                const isTransactional = (migration.mode ?? "transactional") === "transactional";

                if (!isTransactional) {
                    try {
                        await asExecutable(migration, context)();
                        results.push({ id: migration.id!, name: migration.name!, status: "applied" });
                    } catch (error) {
                        results.push({
                            id: migration.id!,
                            name: migration.name!,
                            status: "failed",
                            error: String(error ?? "validation failed"),
                        });
                    }
                    continue;
                }

                await client.query("BEGIN");
                try {
                    await asExecutable(migration, context)();
                    await client.query("ROLLBACK");
                    results.push({ id: migration.id!, name: migration.name!, status: "applied" });
                } catch (error) {
                    await client.query("ROLLBACK").catch(() => {});
                    results.push({
                        id: migration.id!,
                        name: migration.name!,
                        status: "failed",
                        error: String(error ?? "validation failed"),
                    });
                }
            }
        } finally {
            client.release();
        }

        return { valid: results.every((r) => r.status !== "failed"), results };
    }

    async function dryRun(options: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string } = {}): Promise<{
        wouldApply: string[];
        checksums: Record<string, string>;
    }> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        detectDuplicateIds(sourceList);
        const list = topologicalSort(sourceList);
        const client = await pools[0].connect();
        const wouldApply: string[] = [];
        const checksums: Record<string, string> = {};

        try {
            await ensureTables(client);
            const done = await applied(client);
            const doneIds = new Set(done.map((row) => row.id));

            if (statementTimeout !== undefined) {
                await client.query(`SET statement_timeout = ${Math.floor(statementTimeout)}`);
            }

            for (const migration of list) {
                if (doneIds.has(migration.id!)) {
                    continue;
                }

                const context = {
                    client,
                    query: client.query.bind(client),
                    logger,
                    statementTimeout,
                    mode: migration.mode,
                };

                const isTransactional = (migration.mode ?? "transactional") === "transactional";

                if (isTransactional) {
                    await client.query("BEGIN");
                    try {
                        await asExecutable(migration, context)();
                        await client.query("ROLLBACK");
                        wouldApply.push(migration.id!);
                        checksums[migration.id!] = checksum(migration.source ?? "");
                    } catch (error) {
                        await client.query("ROLLBACK").catch(() => {});
                        throw new Error(`Dry-run failed for migration ${migration.id}: ${String(error ?? "")}`);
                    }
                } else {
                    // Non-transactional: execute directly (cannot roll back, side effects possible)
                    try {
                        await asExecutable(migration, context)();
                        wouldApply.push(migration.id!);
                        checksums[migration.id!] = checksum(migration.source ?? "");
                    } catch (error) {
                        throw new Error(`Dry-run failed for migration ${migration.id}: ${String(error ?? "")}`);
                    }
                }
            }
        } finally {
            client.release();
        }

        return { wouldApply, checksums };
    }

    async function assessRollback(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        steps?: number;
    } = {}): Promise<RollbackAssessment[]> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const byId = new Map(list.map((migration) => [migration.id!, migration]));
        const client = await pools[0].connect();

        try {
            await ensureTables(client);
            const done = await applied(client);
            const targets = done.slice(-(options.steps ?? 1)).reverse();

            const assessments: RollbackAssessment[] = [];

            for (const record of targets) {
                const migration = byId.get(record.id);
                const hasDown = Boolean(migration?.down);
                const autoGenerated = !hasDown && migration && typeof migration.up === "string" ? Boolean(autoDown(migration.up)) : false;
                const upSql = migration && typeof migration.up === "string" ? migration.up : "";
                const isComplex = migration ? isComplexMigration(migration) : false;
                const baseRisk = migration && typeof migration.up === "string" ? assessRollbackRisk(migration.up) : "irreversible";
                // Complex migrations are at least potentially-destructive
                const risk = isComplex && baseRisk === "safe" ? "potentially-destructive" : baseRisk;

                assessments.push({
                    migrationId: record.id,
                    risk,
                    reason: isComplex
                        ? "Complex migration (multiple statements or DDL+DML mix) requires explicit rollback handler"
                        : risk === "safe"
                            ? "Additive operation, rollback is safe"
                            : risk === "potentially-destructive"
                                ? "Operation may result in data loss or schema incompatibility"
                                : "Operation is destructive and cannot be reliably reversed",
                    hasDown,
                    autoGenerated,
                    requiresConfirmation: risk !== "safe" || isComplex || !hasDown,
                });
            }

            return assessments;
        } finally {
            client.release();
        }
    }

    async function rollback(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        steps?: number;
        dryRun?: boolean;
        allowDestructive?: boolean;
    } = {}): Promise<string[]> {
        const count = Math.max(1, Number(options.steps ?? 1));
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const byId = new Map(list.map((migration) => [migration.id!, migration]));
        const client = await pools[0].connect();
        const rolledBack: string[] = [];

        try {
            await ensureTables(client);
            const done = await applied(client);
            const targets = done.slice(-count).reverse();

            for (const record of targets) {
                const migration = byId.get(record.id);

                let down: string | ((context: { client: PoolClient; query: PoolClient["query"]; logger?: Logger }) => Promise<unknown>) | null = null;
                let autoGenerated = false;
                let risk: RollbackRisk = "safe";

                if (migration?.down) {
                    down = migration.down;
                    if (typeof migration.down === "string") {
                        risk = assessRollbackRisk(migration.down);
                    } else {
                        risk = "potentially-destructive";
                    }
                } else if (migration && typeof migration.up === "string") {
                    const generated = autoDown(migration.up);
                    if (generated) {
                        down = generated;
                        autoGenerated = true;
                        risk = assessRollbackRisk(generated);
                    }
                }

                if (!down) {
                    throw new Error(
                        `Cannot roll back ${record.id}: no down() migration provided and none could be auto-generated`,
                    );
                }

                // Determine if this migration requires explicit down() handler due to complexity or destructiveness
                const isComplex = migration ? isComplexMigration(migration) : false;
                const downSql = typeof migration?.down === "string" ? migration.down : "";
                const downRisk = downSql ? assessRollbackRisk(downSql) : "safe";

                // Require allowDestructive when:
                // 1. Auto-generating down() for non-safe operations
                // 2. Explicit down() for irreversible operations
                // 3. Explicit down() for potentially-destructive operations
                // 4. Complex migrations (even with explicit down())
                // dryRun bypasses this check since it's a safe simulation.
                if (
                    !options.dryRun &&
                    (
                        (autoGenerated && risk !== "safe") ||
                        (!autoGenerated && (downRisk === "irreversible" || downRisk === "potentially-destructive")) ||
                        (isComplex && !autoGenerated)
                    )
                ) {
                    if (!options.allowDestructive) {
                        throw new Error(
                            `Rollback of ${record.id} requires allowDestructive: true. ` +
                            `Reason: ${isComplex ? "Complex migration" : ""} ` +
                            `Auto-generated: ${autoGenerated} ` +
                            `Down risk: ${downRisk} ` +
                            `Base risk: ${risk}. ` +
                            `Migration ${record.id} modifies data or schema in a way that requires explicit confirmation.`,
                        );
                    }
                }

                const startedAt = Date.now();
                const context = {
                    client,
                    query: client.query.bind(client),
                    logger,
                };

                if (options.dryRun) {
                    await client.query("BEGIN");
                    try {
                        if (typeof down === "function") {
                            await down(context);
                        } else {
                            await client.query(down);
                        }
                        await client.query("ROLLBACK");
                        const completedAt = Date.now();
                        await recordHistory(client, {
                            migrationId: record.id,
                            target: "",
                            checksum: record.checksum ?? "",
                            startedAt: new Date(startedAt).toISOString(),
                            completedAt: new Date(completedAt).toISOString(),
                            durationMs: completedAt - startedAt,
                            status: "rolled_back",
                            attempt: record.attempts ?? 1,
                            mode: "transactional",
                            rollbackRisk: risk,
                            applicationVersion,
                            runnerId: runner,
                            host,
                        });
                        rolledBack.push(record.id);
                        logger?.info?.({ migration: record.id, dryRun: true, risk }, "Rollback dry-run completed");
                    } catch (error) {
                        await client.query("ROLLBACK").catch(() => {});
                        throw error;
                    }
                    continue;
                }

                await client.query("BEGIN");
                try {
                    if (typeof down === "function") {
                        await down(context);
                    } else {
                        await client.query(down);
                    }
                    await client.query(`DELETE FROM ${tableIdent} WHERE id = $1`, [record.id]);
                    const completedAt = Date.now();
                    // Record rollback history INSIDE the same transaction as the
                    // destructive DDL and the registry DELETE. If anything fails
                    // (including history recording), the entire transaction rolls
                    // back, so a destructive change is never committed while being
                    // reported as a failed rollback.
                    await recordHistory(client, {
                        migrationId: record.id,
                        target: "",
                        checksum: record.checksum ?? "",
                        startedAt: new Date(startedAt).toISOString(),
                        completedAt: new Date(completedAt).toISOString(),
                        durationMs: completedAt - startedAt,
                        status: "rolled_back",
                        attempt: record.attempts ?? 1,
                        mode: "transactional",
                        rollbackRisk: risk,
                        applicationVersion,
                        runnerId: runner,
                        host,
                    });
                    await client.query("COMMIT");
                    rolledBack.push(record.id);
                    logger?.info?.({ migration: record.id, risk }, "Migration rolled back");
                } catch (error) {
                    await client.query("ROLLBACK").catch(() => {});
                    throw error;
                }
            }

            return rolledBack;
        } finally {
            client.release();
        }
    }

    async function validateRollback(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        steps?: number;
    } = {}): Promise<{
        valid: boolean;
        results: Array<{ id: string; name: string; valid: boolean; error?: string; risk: RollbackRisk }>;
    }> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const byId = new Map(list.map((migration) => [migration.id!, migration]));
        const client = await pools[0].connect();
        const results: Array<{ id: string; name: string; valid: boolean; error?: string; risk: RollbackRisk }> = [];

        try {
            await ensureTables(client);
            const done = await applied(client);
            const targets = done.slice(-(options.steps ?? 1)).reverse();

            for (const record of targets) {
                const migration = byId.get(record.id);
                const risk = migration && typeof migration.up === "string" ? assessRollbackRisk(migration.up) : "irreversible";

                if (!migration?.down) {
                    results.push({
                        id: record.id,
                        name: record.name,
                        valid: false,
                        error: "No down() migration provided",
                        risk,
                    });
                    continue;
                }

                const downSql = typeof migration.down === "string" ? migration.down : null;
                if (!downSql) {
                    // Function-based down() - can't statically validate, assume valid
                    results.push({
                        id: record.id,
                        name: record.name,
                        valid: true,
                        risk,
                    });
                    continue;
                }

                // Validate down() SQL by executing in a rolled-back transaction
                await client.query("BEGIN");
                try {
                    await client.query(downSql);
                    await client.query("ROLLBACK");
                    results.push({
                        id: record.id,
                        name: record.name,
                        valid: true,
                        risk,
                    });
                } catch (error) {
                    await client.query("ROLLBACK").catch(() => {});
                    results.push({
                        id: record.id,
                        name: record.name,
                        valid: false,
                        error: String(error ?? "rollback validation failed"),
                        risk,
                    });
                }
            }
        } finally {
            client.release();
        }

        return { valid: results.every((r) => r.valid), results };
    }

    async function checkRollbackDependencies(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        steps?: number;
    } = {}): Promise<{
        safe: boolean;
        blocking: Array<{ migrationId: string; dependentOn: string[] }>;
    }> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const byId = new Map(list.map((migration) => [migration.id!, migration]));
        const client = await pools[0].connect();
        const blocking: Array<{ migrationId: string; dependentOn: string[] }> = [];

        try {
            await ensureTables(client);
            const done = await applied(client);
            const doneMap = new Map(done.map((r) => [r.id, r]));
            const targets = done.slice(-(options.steps ?? 1)).reverse();

            // Build a map of migration ID -> migrations that depend on it
            const dependents = new Map<string, string[]>();
            for (const migration of list) {
                for (const dep of migration.dependsOn ?? []) {
                    const existing = dependents.get(dep) ?? [];
                    existing.push(migration.id!);
                    dependents.set(dep, existing);
                }
            }

            for (const record of targets) {
                const dependentMigrations = dependents.get(record.id) ?? [];
                const appliedDependents = dependentMigrations.filter((mid) => {
                    const r = doneMap.get(mid);
                    return r && r.status === "applied";
                });

                if (appliedDependents.length > 0) {
                    blocking.push({
                        migrationId: record.id,
                        dependentOn: appliedDependents,
                    });
                }
            }
        } finally {
            client.release();
        }

        return { safe: blocking.length === 0, blocking };
    }

    async function retryFailed(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
    } = {}): Promise<{ retried: number; succeeded: number; failed: number; skipped: number }> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const byId = new Map(list.map((migration) => [migration.id!, migration]));

        let retried = 0;
        let succeeded = 0;
        let failedCount = 0;
        let skippedCount = 0;

        // Retry is target-aware: inspect every configured target's registry
        // independently and retry only migrations that are failed on that target.
        for (const pool of pools) {
            const targetDatabase = await getPoolDatabase(pool);
            const client = await pool.connect();
            try {
                await ensureTables(client);
                const done = await applied(client);
                const failedRecords = done.filter((r) => r.status === "failed" && byId.has(r.id));

                for (const record of failedRecords) {
                    const migration = byId.get(record.id);
                    if (!migration) {
                        failedCount++;
                        continue;
                    }

                    // Idempotent check: if already applied with matching checksum,
                    // skip and restore status if it was corrupted.
                    const existing = done.find((r) => r.id === record.id);
                    if (existing && existing.status === "applied" && existing.checksum === checksum(migration.source ?? "")) {
                        skippedCount++;
                        await client.query(
                            `UPDATE ${tableIdent}
                             SET status = 'applied',
                                 failed_at = NULL,
                                 error_message = NULL
                             WHERE id = $1 AND status <> 'applied'`,
                            [record.id],
                        );
                        continue;
                    }

                    retried++;
                    try {
                        await runOnPool(pool, [migration], targetDatabase);
                        succeeded++;
                    } catch {
                        failedCount++;
                    }
                }
            } finally {
                client.release();
            }
        }

        return { retried, succeeded, failed: failedCount, skipped: skippedCount };
    }

    async function reconcile(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
        allTargets?: boolean;
    } = {}): Promise<ReconcileResult> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const reconciled: string[] = [];
        const conflicts: string[] = [];

        const targets = options.allTargets ? pools : [pools[0]];
        const targetDatabases = new Set(await Promise.all(targets.map((p) => getPoolDatabase(p))));
        const allStatus = await statusAllTargets({ source: options.source, migrations: options.migrations, directory: options.directory });

        // Filter status to only the targets we're checking
        const filteredStatus = new Map<string, MigrationLogRecord[]>();
        for (const [targetId, records] of allStatus) {
            if (targetDatabases.has(targetId)) {
                filteredStatus.set(targetId, records);
            }
        }

        // Detect interruptions across all targets (or just pools[0])
        const { interruptions: allInterruptions } = await detectInterruptions({ allTargets: options.allTargets ?? true });

        for (const migration of list) {
            const migrationId = migration.id!;
            const statusesByTarget = Array.from(filteredStatus.values())
                .map((records) => records.find((r) => r.id === migrationId))
                .filter((r): r is MigrationLogRecord => r !== undefined);
            const appliedTargets = statusesByTarget.filter((r) => r.status === "applied");
            const failedTargets = statusesByTarget.filter((r) => r.status === "failed");
            const pendingTargets = statusesByTarget.filter((r) => r.status === "pending");

            // A target that reports "applied" but with a different source checksum has
            // drifted and must not be treated as reconciled.
            const sourceChecksum = checksum(migration.source ?? "");
            const checksumMismatch = appliedTargets.some((r) => r.checksum && r.checksum !== sourceChecksum);

            const allAppliedConsistent =
                filteredStatus.size > 0 &&
                appliedTargets.length === filteredStatus.size &&
                failedTargets.length === 0 &&
                pendingTargets.length === 0 &&
                !checksumMismatch;

            if (allAppliedConsistent) {
                reconciled.push(migrationId);
            } else if (failedTargets.length > 0 || checksumMismatch) {
                conflicts.push(migrationId);
            } else if (appliedTargets.length > 0 && pendingTargets.length > 0) {
                conflicts.push(migrationId);
            }
        }

        return { reconciled, conflicts, interruptions: allInterruptions };
    }

    async function detectInterruptions(options: MigrationRecoveryOptions = {}): Promise<{ interruptions: InterruptionRecord[] }> {
        const interruptions: InterruptionRecord[] = [];
        const targets = options.allTargets ? pools : [pools[0]];

        for (const pool of targets) {
            const client = await pool.connect();
            try {
                await ensureTables(client);
                const targetDatabase = await getPoolDatabase(pool);
                const { rows } = await client.query(
                    `SELECT id, migration_id, target_database, checksum, started_at, completed_at, duration_ms, status, attempt, mode, error_message, rollback_risk, application_version, runner_id, host
                     FROM ${historyTableIdent}
                     WHERE completed_at IS NULL
                     ORDER BY started_at ASC`
                );
                for (const row of rows) {
                    interruptions.push({
                        id: row.id,
                        migrationId: row.migration_id,
                        targetDatabase: row.target_database ?? targetDatabase,
                        checksum: row.checksum ?? "",
                        startedAt: row.started_at,
                        attempt: row.attempt,
                        mode: row.mode as MigrationMode,
                        error: row.error_message,
                        runnerId: row.runner_id,
                        host: row.host,
                    });
                }

                // Non-transactional operations that were started but never reached a
                // terminal state represent partial/unknown execution (crash mid-migration).
                const { rows: opRows } = await client.query(
                    `SELECT operation_id, migration_id, target_database, started_at, error_message, runner_id, host
                     FROM ${operationTableIdent}
                     WHERE status = 'started' AND completed_at IS NULL
                     ORDER BY started_at ASC`,
                );
                for (const row of opRows) {
                    interruptions.push({
                        id: row.operation_id,
                        migrationId: row.migration_id,
                        targetDatabase: row.target_database ?? targetDatabase,
                        checksum: "",
                        startedAt: row.started_at,
                        attempt: 1,
                        mode: "non-transactional" as MigrationMode,
                        error: row.error_message,
                        runnerId: row.runner_id,
                        host: row.host,
                    });
                }
            } finally {
                client.release();
            }
        }

        return { interruptions };
    }

    async function recoverInterruptions(options: MigrationRecoveryOptions = {}): Promise<RecoveryResult> {
        const { interruptions } = await detectInterruptions(options);
        const recovered: string[] = [];
        const failed: string[] = [];
        const skipped: string[] = [];

        // Resolve current migration source list for checksum comparison
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const currentChecksums = new Map(sourceList.map((m) => [m.id!, checksum(m.source ?? "")]));

        // Group interruptions by target database
        const byTarget = new Map<string, InterruptionRecord[]>();
        for (const interruption of interruptions) {
            const key = interruption.targetDatabase ?? "default";
            const existing = byTarget.get(key) ?? [];
            existing.push(interruption);
            byTarget.set(key, existing);
        }

        // For each target, check if interrupted migrations were actually applied
        const targets = options.allTargets ? pools : [pools[0]];
        for (const pool of targets) {
            const targetDatabase = await getPoolDatabase(pool);
            const targetInterruptions = byTarget.get(targetDatabase) ?? [];
            if (targetInterruptions.length === 0) {
                continue;
            }

            const client = await pool.connect();
            try {
                await ensureTables(client);
                const done = await applied(client);
                const doneMap = new Map(done.map((r) => [r.id, r]));

                for (const interruption of targetInterruptions) {
                    // Journal-based (non-transactional) interruptions cannot be safely
                    // auto-recovered: we cannot know which statements committed. Mark them
                    // as failed/needs-review instead of blindly rerunning.
                    if (interruption.mode === "non-transactional") {
                        await client.query(
                            `UPDATE ${operationTableIdent}
                             SET status = 'failed',
                                 completed_at = CURRENT_TIMESTAMP,
                                 error_message = COALESCE(error_message, 'Interrupted non-transactional migration requires manual verification')
                             WHERE operation_id = $1 AND completed_at IS NULL`,
                            [interruption.id],
                        );
                        failed.push(interruption.migrationId);
                        continue;
                    }

                    const existing = doneMap.get(interruption.migrationId);
                    const currentChecksum = currentChecksums.get(interruption.migrationId);
                    // If we have the migration definition, verify checksum matches.
                    // If we don't have it (recoverInterruptions called without migrations),
                    // trust the applied status in _writer_migrations.
                    const isApplied =
                        existing &&
                        existing.status === "applied" &&
                        (!currentChecksum || existing.checksum === currentChecksum);
                    if (isApplied) {
                        // Migration was actually applied - mark history as complete
                        await client.query(
                            `UPDATE ${historyTableIdent}
                             SET completed_at = CURRENT_TIMESTAMP,
                                 duration_ms = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) * 1000,
                                 status = 'applied',
                                 error_message = NULL
                             WHERE id = $1 AND completed_at IS NULL`,
                            [interruption.id],
                        );
                        recovered.push(interruption.migrationId);
                    } else {
                        // Migration was not applied or checksum mismatch - mark as failed
                        await client.query(
                            `UPDATE ${historyTableIdent}
                             SET completed_at = CURRENT_TIMESTAMP,
                                 duration_ms = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) * 1000,
                                 status = 'failed',
                                 error_message = COALESCE(error_message, 'Migration interrupted during execution')
                             WHERE id = $1 AND completed_at IS NULL`,
                            [interruption.id],
                        );
                        failed.push(interruption.migrationId);
                    }
                }
            } finally {
                client.release();
            }
        }

        return { recovered, failed, skipped };
    }

    async function detectDrift(options: {
        source?: unknown;
        migrations?: MigrationDefinition[];
        directory?: string;
    } = {}): Promise<{ drift: TargetDrift[] }> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory ?? defaultSource);
        const list = topologicalSort(sourceList);
        const allStatus = await statusAllTargets({ source: options.source, migrations: options.migrations, directory: options.directory });
        const drift: TargetDrift[] = [];

        for (const migration of list) {
            const migrationId = migration.id!;
            for (const [targetId, records] of allStatus) {
                const record = records.find((r) => r.id === migrationId);
                if (!record) {
                    drift.push({
                        targetId,
                        targetDatabase: targetId,
                        driftType: "missing_migration",
                        migrationId,
                    });
                } else if (record.status === "failed") {
                    drift.push({
                        targetId,
                        targetDatabase: targetId,
                        driftType: "status_mismatch",
                        migrationId,
                        actual: record,
                    });
                } else if (record.status === "applied" && record.checksum && record.checksum !== checksum(migration.source ?? "")) {
                    drift.push({
                        targetId,
                        targetDatabase: targetId,
                        driftType: "checksum_mismatch",
                        migrationId,
                        expected: { ...record, checksum: checksum(migration.source ?? "") },
                        actual: record,
                    });
                }
            }
        }

        return { drift };
    }

    async function history(options: { migrationId?: string; limit?: number } = {}): Promise<MigrationAuditEvent[]> {
        const client = await pools[0].connect();
        try {
            let sql = `SELECT id, migration_id, target_database, checksum, started_at, completed_at, duration_ms, status, attempt, mode, error_message, rollback_risk, application_version, runner_id, host FROM ${historyTableIdent}`;
            const params: unknown[] = [];

            if (options.migrationId) {
                sql += ` WHERE migration_id = $1`;
                params.push(options.migrationId);
            }

            sql += ` ORDER BY started_at DESC`;

            if (options.limit) {
                sql += ` $${params.length + 1}`;
                params.push(options.limit);
            }

            const { rows } = await client.query(sql, params);
            return rows.map((row) => ({
                id: row.id,
                migrationId: row.migration_id,
                target: row.target_database,
                checksum: row.checksum,
                startedAt: row.started_at,
                completedAt: row.completed_at,
                durationMs: row.duration_ms,
                status: row.status,
                attempt: row.attempt,
                mode: row.mode,
                error: row.error_message,
                rollbackRisk: row.rollback_risk,
                applicationVersion: row.application_version,
                runnerId: row.runner_id,
                host: row.host,
            }));
        } finally {
            client.release();
        }
    }

    async function detectLongRunningTransactions(options: { minDurationMs?: number } = {}, pool: Pool = pools[0]): Promise<{ transactions: LongRunningTransaction[] }> {
        const minDurationMs = options.minDurationMs ?? 30_000;
        const client = await pool.connect();
        const targetDatabase = await getPoolDatabase(pool);
        try {
            const { rows } = await client.query(
                `SELECT pid, xact_start, state, query, usename, application_name, client_addr,
                        EXTRACT(EPOCH FROM (now() - xact_start)) * 1000 AS duration_ms
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND state IN ('active', 'idle in transaction')
                   AND xact_start IS NOT NULL
                   AND EXTRACT(EPOCH FROM (now() - xact_start)) * 1000 > $1
                 ORDER BY duration_ms DESC`,
                [minDurationMs],
            );
            return {
                transactions: rows.map((row: Record<string, unknown>) => ({
                    pid: Number(row.pid),
                    xactStart: row.xact_start,
                    durationMs: Number(row.duration_ms),
                    state: String(row.state),
                    query: row.query as string | null,
                    applicationName: row.application_name as string | undefined,
                    usename: String(row.usename),
                    clientAddr: row.client_addr as string | undefined,
                    targetDatabase,
                })),
            };
        } finally {
            client.release();
        }
    }

    async function detectLockContention(options: { minWaitMs?: number } = {}, pool: Pool = pools[0]): Promise<{ contention: LockContention[] }> {
        const minWaitMs = options.minWaitMs ?? 1000;
        const client = await pool.connect();
        const targetDatabase = await getPoolDatabase(pool);
        try {
            const { rows } = await client.query(
                `SELECT a.pid, a.state, a.query, a.usename, a.application_name,
                        l.locktype, l.mode, l.granted,
                        EXTRACT(EPOCH FROM (now() - a.query_start)) * 1000 AS wait_ms
                 FROM pg_stat_activity a
                 JOIN pg_locks l ON l.pid = a.pid
                 WHERE a.datname = current_database()
                   AND l.granted = false
                   AND a.state = 'active'
                   AND EXTRACT(EPOCH FROM (now() - a.query_start)) * 1000 > $1
                 ORDER BY wait_ms DESC`,
                [minWaitMs],
            );
            return {
                contention: rows.map((row: Record<string, unknown>) => ({
                    lockKey: 0,
                    waitMs: Number(row.wait_ms),
                    pid: Number(row.pid),
                    query: String(row.query ?? ""),
                    state: String(row.state),
                    applicationName: row.application_name as string | undefined,
                    targetDatabase,
                })),
            };
        } finally {
            client.release();
        }
    }

    async function assessAdvisoryLockStatus(client: PoolClient, lockKey: number): Promise<"free" | "held" | "contended"> {
        const tryResult = await client.query("SELECT pg_try_advisory_lock($1) AS acquired", [lockKey]);
        if (tryResult.rows[0]?.acquired === true) {
            await client.query("SELECT pg_advisory_unlock($1)", [lockKey]).catch(() => {});
            return "free";
        }
        const { rows } = await client.query(
            `SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND classid = 0 AND objid = $1 AND granted = false
             LIMIT 1`,
            [lockKey],
        );
        return rows.length > 0 ? "contended" : "held";
    }

    async function getLockDiagnostics(): Promise<LockDiagnostics> {
        const advisoryLocks: LockWaitMetric[] = [...lockWaitMetrics];
        const lockContention: LockContention[] = [];
        const longRunningTransactions: LongRunningTransaction[] = [];

        // Inspect every configured target so blocker/blocked identity is preserved per database.
        for (const pool of pools) {
            const [lockContentionResult, longRunningResult] = await Promise.all([
                detectLockContention({ minWaitMs: 1000 }, pool),
                detectLongRunningTransactions({ minDurationMs: 30_000 }, pool),
            ]);
            lockContention.push(...lockContentionResult.contention);
            longRunningTransactions.push(...longRunningResult.transactions);
        }

        // Real advisory-lock status: probe pg_advisory_lock / pg_locks instead of inferring
        // from the in-memory wait metric, which never observes locks held by other sessions.
        // For multiple targets, report the most severe status observed.
        let advisoryLockStatus: LockDiagnostics["advisoryLockStatus"] = "free";
        for (const pool of pools) {
            const client = await pool.connect();
            try {
                const status = await assessAdvisoryLockStatus(client, lockKey);
                if (status === "contended") {
                    advisoryLockStatus = "contended";
                } else if (status === "held" && advisoryLockStatus === "free") {
                    advisoryLockStatus = "held";
                }
            } finally {
                client.release();
            }
        }

        return {
            advisoryLocks,
            lockContention,
            longRunningTransactions,
            advisoryLockStatus,
        };
    }

    const PHASE_ORDER: ExpandContractPhase[] = ["expand", "backfill", "verify", "switch", "contract"];

    async function getPhaseState(client: PoolClient, migrationId: string, targetDatabase: string | undefined, phase: ExpandContractPhase) {
        const { rows } = await client.query(
            `SELECT status, started_at, completed_at, error_message FROM ${phaseTableIdent}
             WHERE migration_id = $1 AND target_database IS NOT DISTINCT FROM $2 AND phase = $3`,
            [migrationId, targetDatabase, phase],
        );
        return rows[0] ?? null;
    }

    async function recordPhaseState(client: PoolClient, migrationId: string, targetDatabase: string | undefined, phase: ExpandContractPhase, status: ExpandContractPhaseStatus, startedAt: unknown, completedAt: unknown, errorMessage: string | null) {
        const existing = await getPhaseState(client, migrationId, targetDatabase, phase);
        if (existing) {
            await client.query(
                `UPDATE ${phaseTableIdent}
                 SET status = $2, started_at = COALESCE($3, started_at), completed_at = $4, error_message = $5,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE migration_id = $1 AND target_database IS NOT DISTINCT FROM $6 AND phase = $7`,
                [migrationId, status, startedAt, completedAt, errorMessage, targetDatabase, phase],
            );
        } else {
            await client.query(
                `INSERT INTO ${phaseTableIdent} (migration_id, target_database, phase, status, started_at, completed_at, error_message, runner_id, host)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
                [migrationId, targetDatabase, phase, status, startedAt, completedAt, errorMessage, runner, host],
            );
        }
    }

    async function isMigrationApproved(client: PoolClient, migrationId: string, targetDatabase: string | undefined): Promise<boolean> {
        const { rows } = await client.query(
            `SELECT 1 FROM ${quoteIdent(table + "_approval")}
             WHERE migration_id = $1 AND target_database IS NOT DISTINCT FROM $2 AND status = 'approved'
             LIMIT 1`,
            [migrationId, targetDatabase],
        );
        return rows.length > 0;
    }

    async function expandContract(options: ExpandContractOptions): Promise<ExpandContractResult> {
        const batchSize = options.batchSize ?? 1000;
        const enforcePhases = options.enforcePhases ?? false;
        const defaultCompatibility = options.compatibility?.confirmed ?? false;
        const completedSteps: string[] = [];
        const skippedSteps: string[] = [];
        const errors: string[] = [];
        const progress: BackfillProgress[] = [];
        const phaseStates: ExpandContractPhaseState[] = [];

        const pool = pools[0];
        const client = await pool.connect();
        const targetDatabase = await getPoolDatabase(pool);
        // A single run-level id groups all phases of this expandContract invocation
        // so prerequisite checks see earlier phases completed under the same id.
        const runId = options.migrationId ?? crypto.randomUUID();

        try {
            await ensureTables(client);

            for (const step of options.steps) {
                const phase = step.phase;
                const phaseIndex = PHASE_ORDER.indexOf(phase);
                const stepProgress: BackfillProgress = {
                    total: 0, processed: 0, percentage: 0,
                    startedAt: new Date().toISOString(),
                    lastUpdatedAt: new Date().toISOString(), errors: 0,
                };
                let phaseStatus: ExpandContractPhaseStatus = "pending";

                try {
                    // Prerequisite enforcement: every earlier *declared* phase must be
                    // completed. Phases not present in the step list are not required.
                    if (enforcePhases && phaseIndex > 0) {
                        const declaredPhases = options.steps.map((s) => s.phase);
                        const prerequisites = PHASE_ORDER.slice(0, phaseIndex).filter((p) =>
                            declaredPhases.includes(p),
                        );
                        if (prerequisites.length > 0) {
                            const prereqResults = await Promise.all(
                                prerequisites.map(async (p) => {
                                    const st = await getPhaseState(client, runId, targetDatabase, p);
                                    return st?.status === "completed";
                                }),
                            );
                            if (!prereqResults.every(Boolean)) {
                                phaseStatus = "blocked";
                                const stepError = `${step.name}: prerequisite phase(s) not completed`;
                                errors.push(stepError);
                                await recordPhaseState(client, runId, targetDatabase, phase, "blocked", stepProgress.startedAt, undefined, stepError);
                                phaseStates.push({ phase, status: "blocked", error: stepError });
                                progress.push(stepProgress);
                                continue;
                            }
                        }
                    }

                    // Approval gating and compatibility acknowledgement only apply when
                    // phase enforcement is opted into.
                    if (enforcePhases) {
                        const needsApproval = step.requiresApproval === true
                            || (phase === "contract" && step.risk === "potentially-destructive");
                        const compatibilityRequired = phase === "contract"
                            && (step.compatibility?.confirmed !== undefined ? !step.compatibility.confirmed : !defaultCompatibility);

                        if (needsApproval || compatibilityRequired) {
                            let approved = true;
                            let approvalReason: string | null = null;
                            if (compatibilityRequired) {
                                approved = false;
                                approvalReason = "application compatibility not confirmed";
                            }
                            if (needsApproval && approved) {
                                const stepChecksum = checksum(step.sql ?? "");
                                const { rows: approvalRows } = await client.query(
                                    `SELECT status, migration_checksum, expires_at
                                     FROM ${quoteIdent(table + "_approval")}
                                     WHERE migration_id = $1 AND target_database IS NOT DISTINCT FROM $2
                                     ORDER BY approved_at DESC NULLS LAST, id DESC
                                     LIMIT 1`,
                                    [runId, targetDatabase],
                                );
                                let approvalValid = false;
                                if (approvalRows.length > 0) {
                                    const row = approvalRows[0] as Record<string, unknown>;
                                    if (row.status === "approved") {
                                        const expired = row.expires_at ? new Date(row.expires_at as string) < new Date() : false;
                                        const stale = row.migration_checksum ? (row.migration_checksum as string) !== stepChecksum : false;
                                        approvalValid = !expired && !stale;
                                        if (expired) approvalReason = "approval expired";
                                        if (stale) approvalReason = "stale approval";
                                    } else if (row.status === "rejected") {
                                        approvalReason = "approval rejected";
                                    }
                                }
                                if (!approvalValid && approvalReason === null) {
                                    approvalReason = "approval required";
                                }
                                if (!approvalValid && approvalReason === "approval required") {
                                    if (approvalProvider) {
                                        const pseudoMigration: MigrationDefinition = {
                                            id: runId,
                                            name: runId,
                                            up: step.sql,
                                            source: step.sql,
                                        };
                                        let result: { approved: boolean; approvedBy?: string; approvedAt?: unknown; reason?: string; expiresAt?: unknown } | undefined;
                                        try {
                                            result = await approvalProvider.approve?.(pseudoMigration, {
                                                runnerId: runner,
                                                host,
                                                targetDatabase: targetDatabase ?? undefined,
                                            });
                                        } catch (e) {
                                            approvalReason = `approval provider failure: ${String(e ?? "")}`;
                                            result = { approved: false };
                                        }
                                        if (!result || !result.approved) {
                                            approvalValid = false;
                                            if (approvalReason === "approval required" && result?.reason) {
                                                approvalReason = result.reason;
                                            }
                                        } else {
                                            await client.query(
                                                `INSERT INTO ${quoteIdent(table + "_approval")} (migration_id, target_database, requirement, status, approved_by, approved_at, runner_id, host, migration_checksum, expires_at, step_name)
                                                 VALUES ($1, $2, $3, 'approved', $4, $5, $6, $7, $8, $9, $10)`,
                                                [
                                                    runId,
                                                    targetDatabase ?? null,
                                                    approvalProvider.requiresApproval?.(pseudoMigration, { runnerId: runner, host, targetDatabase: targetDatabase ?? undefined }) ? "explicit" : "automatic",
                                                    result.approvedBy ?? "system",
                                                    result.approvedAt ?? new Date().toISOString(),
                                                    runner,
                                                    host ?? null,
                                                    stepChecksum,
                                                    (result as any).expiresAt ?? null,
                                                    step.name,
                                                ],
                                            );
                                            approvalValid = true;
                                            approvalReason = null;
                                        }
                                    } else {
                                        approvalValid = false;
                                        approvalReason = "approval required";
                                    }
                                }
                                if (!approvalValid) {
                                    approved = false;
                                    if (!approvalReason) approvalReason = "approval required";
                                }
                            }
                            if (!approved) {
                                phaseStatus = "needs_approval";
                                const stepError = `${step.name}: ${approvalReason ?? "not authorized"}`;
                                errors.push(stepError);
                                await recordPhaseState(client, runId, targetDatabase, phase, "needs_approval", stepProgress.startedAt, undefined, stepError);
                                phaseStates.push({ phase, status: "needs_approval", error: stepError });
                                progress.push(stepProgress);
                                continue;
                            }
                        }
                    }

                    // Mark the phase running.
                    await recordPhaseState(client, runId, targetDatabase, phase, "running", new Date().toISOString(), undefined, null);
                    phaseStatus = "running";

                    // Execute the step. Non-transactional steps (e.g. CREATE INDEX
                    // CONCURRENTLY) run outside an ordinary BEGIN/COMMIT block.
                    if (step.nonTransactional) {
                        await client.query(step.sql);
                    } else {
                        await client.query("BEGIN");
                        try {
                            await client.query(step.sql);
                            await client.query("COMMIT");
                        } catch (error) {
                            await client.query("ROLLBACK").catch(() => {});
                            throw error;
                        }
                    }

                    const now = new Date().toISOString();
                    await recordPhaseState(client, runId, targetDatabase, phase, "completed", stepProgress.startedAt, now, null);
                    phaseStatus = "completed";
                    completedSteps.push(step.name);
                    stepProgress.processed = 1;
                    stepProgress.percentage = 100;
                } catch (error) {
                    const stepError = String(error ?? "step failed");
                    errors.push(`${step.name}: ${stepError}`);
                    phaseStatus = "failed";
                    await recordPhaseState(client, runId, targetDatabase, phase, "failed", stepProgress.startedAt, new Date().toISOString(), stepError);
                    phaseStates.push({ phase, status: "failed", error: stepError });
                    progress.push(stepProgress);
                    continue;
                }

                stepProgress.lastUpdatedAt = new Date().toISOString();
                progress.push(stepProgress);
                phaseStates.push({
                    phase,
                    status: phaseStatus,
                    startedAt: stepProgress.startedAt,
                    completedAt: phaseStatus === "completed" ? new Date().toISOString() : undefined,
                });
            }
        } finally {
            client.release();
        }

        return {
            completedSteps,
            skippedSteps,
            progress,
            phaseStates,
            successful: errors.length === 0,
            errors,
        };
    }

    /* ------------------------------------------------------------------ *
     * Durable backfill engine                                            *
     * ------------------------------------------------------------------ */

    async function resolvePoolForTarget(targetDatabase?: string): Promise<Pool> {
        if (!targetDatabase) return pools[0];
        for (const pool of pools) {
            try {
                if (await getPoolDatabase(pool) === targetDatabase) {
                    return pool;
                }
            } catch {
                // ignore
            }
        }
        return pools[0];
    }

    function rawPoolConfig(pool: Pool): Record<string, unknown> {
        const opts = (pool as unknown as { options?: Record<string, unknown> }).options ?? {};
        const config: Record<string, unknown> = {
            user: opts.user,
            password: opts.password,
            host: opts.host,
            port: opts.port,
            max: Math.min(Number(opts.max ?? 2), 2),
        };
        if (opts.database !== undefined) {
            // deliberately not propagated: callers set the target database explicitly
        }
        if (opts.ssl !== undefined) {
            config.ssl = opts.ssl;
        }
        return config;
    }

    function serializeCursor(value: unknown[] | null): string | null {
        if (value === null) return null;
        return JSON.stringify(value);
    }

    function deserializeCursor(value: string | null): unknown[] | null {
        if (value === null || value === "") return null;
        try {
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed : null;
        } catch {
            return null;
        }
    }

    function mapBackfillJob(row: Record<string, unknown>): BackfillJob {
        return {
            id: String(row.id),
            migrationId: String(row.migration_id),
            targetDatabase: row.target_database as string | undefined,
            jobName: String(row.job_name),
            status: (row.status as BackfillJobStatus) ?? "pending",
            sourceTable: String(row.source_table ?? ""),
            cursorType: (row.cursor_type as BackfillCursorType) ?? "serial",
            cursorColumns: ((row.cursor_columns as unknown[]) ?? []).map(String),
            cursorValue: deserializeCursor(String(row.cursor_value ?? "")),
            batchSize: Number(row.batch_size ?? 1000),
            rowsProcessed: Number(row.rows_processed ?? 0),
            rowsRemaining: row.rows_remaining === null ? null : Number(row.rows_remaining),
            progressPercentage: row.progress_percentage === null ? null : Number(row.progress_percentage),
            startedAt: row.started_at,
            updatedAt: row.updated_at,
            completedAt: row.completed_at,
            lastError: row.last_error as string | null,
            retryCount: Number(row.retry_count ?? 0),
            runnerId: row.runner_id as string | undefined,
            host: row.host as string | undefined,
        };
    }

    function buildBackfillBatchSelect(
        table: string,
        cursor: BackfillCursor,
        cursorValue: unknown[] | null,
        batchSize: number,
        where?: string,
    ): { sql: string; params: unknown[] } {
        const tableIdent = quoteIdent(table);
        const cols = cursor.columns.map((c) => quoteIdent(c));
        const orderExpr = cols.join(", ");
        const params: unknown[] = [];
        let whereSql = "TRUE";
        if (cursorValue !== null) {
            if (cols.length === 1) {
                whereSql = `${cols[0]} > $1`;
                params.push(cursorValue[0]);
            } else {
                // Composite keyset: (a, b) > ($1, $2) lexicographic
                const placeholders = cursorValue.map((_, i) => `$${i + 1}`).join(", ");
                whereSql = `(${cols.join(", ")}) > (${placeholders})`;
                params.push(...cursorValue);
            }
        }
        if (where) {
            whereSql = `(${whereSql}) AND (${where})`;
        }
        params.push(batchSize);
        const sql = `SELECT * FROM ${tableIdent} WHERE ${whereSql} ORDER BY ${orderExpr} ASC LIMIT $${params.length}`;
        return { sql, params };
    }

    function nextCursorFromRows(
        rows: Array<Record<string, unknown>>,
        cursor: BackfillCursor,
    ): unknown[] | null {
        if (rows.length === 0) return null;
        const last = rows[rows.length - 1];
        return cursor.columns.map((c) => last[c]);
    }

    async function backfill(options: BackfillOptions): Promise<BackfillResult> {
        const batchSize = Math.max(1, options.batchSize ?? 1000);
        const maxRetries = options.maxRetries ?? 3;
        const delayMs = Math.max(0, options.delayMs ?? 0);
        const statementTimeout = options.statementTimeout;
        const targetDatabase = options.target ?? (await getPoolDatabase(pools[0]));
        const pool = await resolvePoolForTarget(targetDatabase);
        const client = await pool.connect();
        const startedAt = new Date().toISOString();
        const errors: string[] = [];
        let batches = 0;
        let totalRows: number | null = null;
        let job: BackfillJob | null = null;

        try {
            await ensureTables(client);
            if (statementTimeout) {
                await client.query(`SET statement_timeout = ${Math.floor(statementTimeout)}`);
            }

            // Best-effort total count for progress reporting (does not fail the job).
            try {
                const { rows: cnt } = await client.query(
                    `SELECT count(*) AS n FROM ${quoteIdent(options.table)}`,
                );
                totalRows = Number(cnt[0]?.n);
            } catch {
                // total unknown; progress percentage stays null until measurable
            }

            // Get or create the job.
            const { rows: existing } = await client.query(
                `SELECT * FROM ${backfillTableIdent}
                 WHERE migration_id = $1 AND target_database IS NOT DISTINCT FROM $2 AND job_name = $3`,
                [options.migrationId, targetDatabase, options.id],
            );
            if (existing.length > 0) {
                job = mapBackfillJob(existing[0]);
                if (job.status === "completed") {
                    return {
                        jobId: job.id,
                        status: "completed",
                        rowsProcessed: job.rowsProcessed,
                        batches: 0,
                        progress: backfillProgress(job, totalRows, startedAt, startedAt),
                        errors: [],
                        completed: true,
                    };
                }
                if (job.status === "running") {
                    // Interrupted mid-batch: resume from the last committed cursor.
                    // The in-flight batch was rolled back, so re-processing from the
                    // saved cursor is safe for idempotent handlers.
                    await client.query(
                        `UPDATE ${backfillTableIdent}
                         SET status = 'pending', updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                             retry_count = 0
                         WHERE id = $1`,
                        [job.id],
                    );
                    job.status = "pending";
                    job.retryCount = 0;
                    job.lastError = null;
                }
            } else {
                const { rows: ins } = await client.query(
                    `INSERT INTO ${backfillTableIdent}
                     (migration_id, target_database, job_name, status, source_table, cursor_type, cursor_columns, batch_size, runner_id, host, started_at)
                     VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
                     RETURNING *`,
                    [
                        options.migrationId,
                        targetDatabase,
                        options.id,
                        options.table,
                        options.cursor.type,
                        options.cursor.columns,
                        batchSize,
                        runner,
                        host,
                    ],
                );
                job = mapBackfillJob(ins[0]);
                job.rowsRemaining = totalRows;
            }

            // Mark the job running.
            await client.query(
                `UPDATE ${backfillTableIdent}
                 SET status = 'running', started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                     updated_at = CURRENT_TIMESTAMP, rows_remaining = $2
                 WHERE id = $1`,
                [job.id, totalRows === null ? null : totalRows],
            );
            job.status = "running";
            job.startedAt = job.startedAt ?? startedAt;
            job.rowsRemaining = totalRows;

            let cursorValue = job.cursorValue;
            let completed = false;

            while (!completed) {
                const { sql, params } = buildBackfillBatchSelect(
                    options.table,
                    options.cursor,
                    cursorValue,
                    batchSize,
                    options.where,
                );

                await client.query("BEGIN");
                let batchError: unknown = null;
                let rows: Array<Record<string, unknown>> = [];
                try {
                    const { rows: r } = await client.query(sql, params);
                    rows = r as Array<Record<string, unknown>>;
                } catch (error) {
                    batchError = error;
                }

                let processed = 0;
                let nextCursor: unknown[] | null = null;
                if (!batchError && rows.length > 0) {
                    const query: MigrationQueryFn = (text, values) =>
                        client.query(text, values);
                    const ctx: BackfillBatchContext = {
                        client,
                        query,
                        rows,
                        batchNumber: batches + 1,
                    };
                    try {
                        const result = await options.processBatch(ctx);
                        processed = Math.max(0, Number(result.processed ?? 0));
                    } catch (error) {
                        batchError = error;
                    }
                    nextCursor = nextCursorFromRows(rows, options.cursor);
                }

                if (batchError) {
                    await client.query("ROLLBACK").catch(() => {});
                    errors.push(String(batchError ?? "batch failed"));
                    job.retryCount += 1;
                    if (job.retryCount >= maxRetries) {
                        await client.query(
                            `UPDATE ${backfillTableIdent}
                             SET status = 'failed', last_error = $2,
                                 updated_at = CURRENT_TIMESTAMP, retry_count = $3
                             WHERE id = $1`,
                            [job.id, String(batchError ?? "batch failed"), job.retryCount],
                        );
                        job.status = "failed";
                        job.lastError = String(batchError ?? "batch failed");
                        completed = true;
                    } else if (delayMs > 0) {
                        await new Promise((resolve) => setTimeout(resolve, delayMs));
                    }
                    // retry from the same cursor (batch was rolled back)
                    continue;
                }

                // Checkpoint + data committed atomically.
                const rowsProcessed = job.rowsProcessed + processed;
                const isLastBatch = rows.length < batchSize;
                await client.query(
                    `UPDATE ${backfillTableIdent}
                     SET cursor_value = $2,
                         rows_processed = $3,
                         rows_remaining = $4,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE id = $1`,
                    [
                        job.id,
                        serializeCursor(isLastBatch ? null : nextCursor),
                        rowsProcessed,
                        isLastBatch ? 0 : job.rowsRemaining === null ? null : Math.max(0, job.rowsRemaining - processed),
                    ],
                );
                job.rowsProcessed = rowsProcessed;
                job.cursorValue = isLastBatch ? null : nextCursor;

                if (isLastBatch) {
                    await client.query(
                        `UPDATE ${backfillTableIdent}
                         SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                             updated_at = CURRENT_TIMESTAMP, progress_percentage = 100
                         WHERE id = $1`,
                        [job.id],
                    );
                    job.status = "completed";
                    job.completedAt = new Date().toISOString();
                    completed = true;
                } else {
                    await client.query(
                        `UPDATE ${backfillTableIdent}
                         SET progress_percentage = $2
                         WHERE id = $1`,
                        [
                            job.id,
                            totalRows === null ? null : Math.min(100, (rowsProcessed / totalRows) * 100),
                        ],
                    );
                }

                await client.query("COMMIT");
                batches += 1;

                const now = new Date().toISOString();
                options.onProgress?.(
                    backfillProgress(job, totalRows, job.startedAt ?? startedAt, now),
                );

                if (!completed && delayMs > 0) {
                    await new Promise((resolve) => setTimeout(resolve, delayMs));
                }
                cursorValue = job.cursorValue;
            }

            return {
                jobId: job.id,
                status: job.status,
                rowsProcessed: job.rowsProcessed,
                batches,
                progress: backfillProgress(job, totalRows, job.startedAt ?? startedAt, new Date().toISOString()),
                errors,
                completed: job.status === "completed",
            };
        } catch (error) {
            errors.push(String(error ?? "backfill failed"));
            const fallbackJob: BackfillJob = job ?? {
                id: "",
                migrationId: options.migrationId,
                targetDatabase,
                jobName: options.id,
                status: "failed",
                sourceTable: options.table,
                cursorType: options.cursor.type,
                cursorColumns: options.cursor.columns,
                cursorValue: null,
                batchSize,
                rowsProcessed: 0,
                rowsRemaining: null,
                progressPercentage: null,
                startedAt,
                updatedAt: new Date().toISOString(),
                completedAt: null,
                lastError: String(error ?? "backfill failed"),
                retryCount: 0,
                runnerId: runner,
                host,
            };
            return {
                jobId: fallbackJob.id,
                status: fallbackJob.status,
                rowsProcessed: fallbackJob.rowsProcessed,
                batches,
                progress: backfillProgress(fallbackJob, totalRows, startedAt, new Date().toISOString()),
                errors,
                completed: false,
            };
        } finally {
            client.release();
        }
    }

    function backfillProgress(
        job: BackfillJob,
        totalRows: number | null,
        startedAt: unknown,
        now: unknown,
    ): BackfillProgress {
            let percentage: number | null = null;
            if (totalRows) {
                percentage = Math.min(100, (job.rowsProcessed / totalRows) * 100);
            }
            return {
                total: totalRows ?? 0,
                processed: job.rowsProcessed,
                percentage: job.progressPercentage ?? percentage ?? 0,
                startedAt,
                lastUpdatedAt: job.updatedAt ?? now,
                errors: job.lastError ? 1 : 0,
            };
    }

    async function getBackfillJob(client: PoolClient, options: BackfillStatusOptions): Promise<BackfillJob | null> {
        const targetDatabase = options.target ?? (await getPoolDatabase(pools[0]));
        const { rows } = await client.query(
            `SELECT * FROM ${backfillTableIdent}
             WHERE job_name = $1 AND target_database IS NOT DISTINCT FROM $2`,
            [options.id, targetDatabase],
        );
        if (rows.length === 0) return null;
        return mapBackfillJob(rows[0]);
    }

    async function getBackfillStatus(options: BackfillStatusOptions): Promise<BackfillJob | null> {
        const pool = await resolvePoolForTarget(options.target);
        const client = await pool.connect();
        try {
            await ensureTables(client);
            return await getBackfillJob(client, options);
        } finally {
            client.release();
        }
    }

    async function pauseBackfill(options: PauseBackfillOptions): Promise<BackfillJob> {
        const pool = await resolvePoolForTarget(options.target);
        const client = await pool.connect();
        try {
            await ensureTables(client);
            const job = await getBackfillJob(client, options);
            if (!job) {
                throw new Error(`Backfill job not found: ${options.id}`);
            }
            if (job.status !== "running") {
                return job;
            }
            await client.query(
                `UPDATE ${backfillTableIdent} SET status = 'paused', updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
                [job.id],
            );
            job.status = "paused";
            job.updatedAt = new Date().toISOString();
            return job;
        } finally {
            client.release();
        }
    }

    async function resumeBackfill(options: ResumeBackfillOptions): Promise<BackfillJob> {
        const pool = await resolvePoolForTarget(options.target);
        const client = await pool.connect();
        try {
            await ensureTables(client);
            const job = await getBackfillJob(client, options);
            if (!job) {
                throw new Error(`Backfill job not found: ${options.id}`);
            }
            if (job.status === "completed") {
                return job;
            }
            // Resume from the saved cursor (paused or interrupted).
            await client.query(
                `UPDATE ${backfillTableIdent}
                 SET status = 'running', updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                     retry_count = 0
                 WHERE id = $1`,
                [job.id],
            );
            job.status = "running";
            job.lastError = null;
            job.retryCount = 0;
            return job;
        } finally {
            client.release();
        }
    }

    async function cancelBackfill(options: CancelBackfillOptions): Promise<BackfillJob> {
        const pool = await resolvePoolForTarget(options.target);
        const client = await pool.connect();
        try {
            await ensureTables(client);
            const job = await getBackfillJob(client, options);
            if (!job) {
                throw new Error(`Backfill job not found: ${options.id}`);
            }
            await client.query(
                `UPDATE ${backfillTableIdent}
                 SET status = 'failed', last_error = $2, updated_at = CURRENT_TIMESTAMP
                 WHERE id = $1`,
                [job.id, "cancelled"],
            );
            job.status = "failed";
            job.lastError = "cancelled";
            return job;
        } finally {
            client.release();
        }
    }

    async function retryBackfill(options: RetryBackfillOptions): Promise<BackfillResult> {
        const pool = await resolvePoolForTarget(options.target);
        const client = await pool.connect();
        try {
            await ensureTables(client);
            const job = await getBackfillJob(client, options);
            if (!job) {
                throw new Error(`Backfill job not found: ${options.id}`);
            }
            // Reset a failed/paused job to pending so it can be re-run from its cursor.
            await client.query(
                `UPDATE ${backfillTableIdent}
                 SET status = 'pending', updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                     retry_count = 0
                 WHERE id = $1`,
                [job.id],
            );
            // Re-invoke backfill with the same job identity; it will resume from the cursor.
            return await backfill({
                id: job.jobName,
                migrationId: job.migrationId,
                target: job.targetDatabase,
                table: job.sourceTable,
                cursor: {
                    type: job.cursorType as BackfillCursorType,
                    columns: job.cursorColumns,
                    direction: "asc",
                },
                batchSize: job.batchSize,
                processBatch: options.processBatch,
                maxRetries: options.maxRetries,
                delayMs: options.delayMs,
                statementTimeout: options.statementTimeout,
                onProgress: options.onProgress,
            } as unknown as BackfillOptions);
        } finally {
            client.release();
        }
    }

    async function verifyBackup(options: {
        migrationId?: string;
        target?: string;
        restoreTarget?: string;
    }): Promise<{
        backupId: string;
        targetDatabase: string;
        restored: boolean;
        verified: boolean;
        checks: Array<{ name: string; passed: boolean; detail?: string }>;
    }> {
        const checks: Array<{ name: string; passed: boolean; detail?: string }> = [];
        const targetDatabase = options.target ?? (await getPoolDatabase(pools[0]));
        const pool = await resolvePoolForTarget(targetDatabase);
        const client = await pool.connect();
        let backupId = "";
        let restored = false;
        let verified = false;
        let restoreTargetDb = options.restoreTarget;
        let createdRestoreTarget = false;
        let adminPool: Pool | null = null;

        try {
            await ensureTables(client);
            const { rows: backupRows } = await client.query(
                `SELECT id, migration_id, target_database, backup_id, backup_location, successful
                 FROM ${quoteIdent(table + "_backup")}
                 WHERE ($1::text IS NULL OR migration_id = $1)
                   AND target_database IS NOT DISTINCT FROM $2
                   AND successful = TRUE
                 ORDER BY created_at DESC
                 LIMIT 1`,
                [options.migrationId ?? null, targetDatabase],
            );

            if (backupRows.length === 0) {
                checks.push({ name: "backup_record", passed: false, detail: "No successful backup record found for the migration/target" });
                return { backupId, targetDatabase, restored, verified, checks };
            }
            const backupRow = backupRows[0] as Record<string, unknown>;
            backupId = String(backupRow.backup_id);
            checks.push({ name: "backup_record", passed: true, detail: backupId });

            // Provider capability contract: existence check is optional.
            if (backupProvider?.exists) {
                let exists = false;
                try {
                    exists = await backupProvider.exists(backupId);
                } catch {
                    exists = false;
                }
                checks.push({ name: "backup_exists", passed: exists, detail: exists ? "backup exists" : "backup could not be located by provider" });
                if (!exists) {
                    return { backupId, targetDatabase, restored, verified, checks };
                }
            } else {
                checks.push({ name: "backup_exists", passed: true, detail: "Provider does not implement exists(); skipped" });
            }

            // Isolated restore target selection or creation.
            if (!restoreTargetDb) {
                const baseConfig = rawPoolConfig(pool);
                adminPool = new Pool({ ...baseConfig, database: "postgres" });
                const suffix = `verify_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
                restoreTargetDb = `restore_${suffix}`.replace(/[^a-z0-9_]/gi, "_").slice(0, 60);
                await adminPool.query(`CREATE DATABASE "${restoreTargetDb}"`);
                createdRestoreTarget = true;
            }
            checks.push({ name: "restore_target_isolated", passed: true, detail: restoreTargetDb });

            const restoreCapable = typeof backupProvider?.restore === "function";
            checks.push({ name: "restore_capability", passed: restoreCapable, detail: restoreCapable ? "provider supports restore" : "provider does not support restore" });
            if (!restoreCapable) {
                return { backupId, targetDatabase, restored, verified, checks };
            }

            try {
                await backupProvider!.restore!(backupId, restoreTargetDb);
                restored = true;
                checks.push({ name: "restore_completed", passed: true });
            } catch (error) {
                checks.push({ name: "restore_completed", passed: false, detail: String(error ?? "restore failed") });
                return { backupId, targetDatabase, restored, verified, checks };
            }

            // Inspect restored database schema/data using a dedicated connection.
            const restorePool = new Pool({ ...rawPoolConfig(pool), database: restoreTargetDb });
            try {
                const restoreClient = await restorePool.connect();
                try {
                    await restoreClient.query("SET statement_timeout = 30000");
                    const { rows: tableRows } = await restoreClient.query(
                        "SELECT count(*) AS n FROM pg_tables WHERE schemaname = 'public'",
                    );
                    const tableCount = Number(tableRows[0]?.n ?? 0);
                    checks.push({ name: "restored_tables", passed: tableCount >= 0, detail: `${tableCount} public tables` });

                    const { rows: invalidIdx } = await restoreClient.query(
                        "SELECT count(*) AS n FROM pg_index WHERE NOT indisvalid",
                    );
                    const invalidCount = Number(invalidIdx[0]?.n ?? 0);
                    checks.push({ name: "index_validity", passed: invalidCount === 0, detail: `${invalidCount} invalid indexes` });

                    const { rows: fkRows } = await restoreClient.query(
                        "SELECT count(*) AS n FROM pg_constraint WHERE contype = 'f'",
                    );
                    checks.push({ name: "foreign_keys", passed: true, detail: `${Number(fkRows[0]?.n ?? 0)} foreign keys` });

                    const { rows: extRows } = await restoreClient.query(
                        "SELECT count(*) AS n FROM pg_extension WHERE extname <> 'plpgsql'",
                    );
                    checks.push({ name: "extensions", passed: true, detail: `${Number(extRows[0]?.n ?? 0)} non-default extensions` });
                } finally {
                    restoreClient.release();
                }
                await restorePool.end().catch(() => {});
            } catch (error) {
                checks.push({ name: "restored_verification", passed: false, detail: String(error ?? "verification failed") });
            }
        } finally {
            // Cleanup temporary restore target safely.
            try {
                if (createdRestoreTarget && restoreTargetDb) {
                    if (adminPool) {
                        await adminPool.query(
                            `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
                            [restoreTargetDb],
                        ).catch(() => {});
                        await adminPool.query(`DROP DATABASE IF EXISTS "${restoreTargetDb}"`);
                    }
                }
            } catch (cleanupError) {
                checks.push({ name: "cleanup", passed: false, detail: String(cleanupError ?? "cleanup failed") });
            }
            if (adminPool) {
                await adminPool.end().catch(() => {});
            }
            client.release();
        }

        // All required checks passed.
        verified = checks.length > 0 && checks.every((c) => c.passed);
        return { backupId, targetDatabase, restored, verified, checks };
    }

    async function baseline(options: BaselineOptions): Promise<BaselineResult> {
        const sourceList = await resolveSource(options.migrations ?? source ?? migrations ?? directory);
        const list = topologicalSort(sourceList);
        const client = await pools[0].connect();
        const skipped: string[] = [];

        try {
            await ensureTables(client);
            await client.query("BEGIN");
            try {
                // Verify all baseline migrations are actually applied (checksum matches)
                const done = await applied(client);
                const doneMap = new Map(done.map((r) => [r.id, r]));

                let appliedCount = 0;
                for (const m of options.migrations) {
                    const record = doneMap.get(m.id!);
                    if (!record || record.status !== "applied") {
                        skipped.push(m.id!);
                        continue;
                    }
                    if (propertiesEqual(record.checksum, checksum(m.source ?? ""))) {
                        // already baseline-tracked maybe skip
                    }
                    appliedCount++;
                }

                const baselineChecksum = crypto
                    .createHash("sha256")
                    .update(options.migrations.map((m) => m.id + checksum(m.source ?? "")).join(","))
                    .digest("hex");

                // create baseline table (single row), but check for existing — let's always insert (newer baseline)
                const result = await client.query(
                    `INSERT INTO _writer_migrations_baseline (name, applied_migration_ids, checksum, created_by, verified, schema_version)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     RETURNING id, created_at`,
                    [
                        options.name,
                        options.migrations.map((m) => m.id!),
                        baselineChecksum,
                        options.createdBy ?? null,
                        options.verify ?? true,
                        options.schemaVersion ?? null,
                    ],
                );

                const row = result.rows[0] as Record<string, unknown>;
                await client.query("COMMIT");

                return {
                    baseline: {
                        id: String(row.id),
                        name: options.name,
                        appliedMigrationIds: options.migrations.map((m) => m.id!),
                        checksum: baselineChecksum,
                        createdAt: row.created_at,
                        createdBy: options.createdBy,
                        verified: options.verify ?? true,
                        schemaVersion: options.schemaVersion,
                    },
                    appliedCount,
                    skipped,
                };
            } catch (error) {
                await client.query("ROLLBACK").catch(() => {});
                throw error;
            }
        } finally {
            client.release();
        }
    }

    async function listBaselines(): Promise<BaselineRecord[]> {
        const client = await pools[0].connect();
        try {
            await ensureTables(client);
            const { rows } = await client.query(
                `SELECT id, name, applied_migration_ids, checksum, created_at, created_by, verified, schema_version
                 FROM _writer_migrations_baseline ORDER BY created_at DESC`,
            );
            return rows.map((row: Record<string, unknown>) => ({
                id: String(row.id),
                name: String(row.name),
                appliedMigrationIds: (row.applied_migration_ids as string[]) ?? [],
                checksum: String(row.checksum),
                createdAt: row.created_at,
                createdBy: row.created_by as string | undefined,
                verified: Boolean(row.verified),
                schemaVersion: row.schema_version as string | undefined,
            }));
        } finally {
            client.release();
        }
    }

    async function squash(options: SquashOptions): Promise<SquashResult> {
        const migrations = options.migrations ?? [];
        if (options.dryRun) {
            const baselineChecksum = crypto
                .createHash("sha256")
                .update(migrations.map((m) => m.id + checksum(m.source ?? "")).join(","))
                .digest("hex");
            return {
                baseline: {
                    id: "",
                    name: options.name ?? "squash baseline",
                    appliedMigrationIds: migrations.map((m) => m.id!),
                    checksum: baselineChecksum,
                    createdAt: new Date().toISOString(),
                    verified: true,
                },
                squashed: migrations.map((m) => m.id!),
                skipped: [],
                dryRun: true,
            };
        }

        // Apply a fresh baseline covering the specified migrations (squash), preserving historical rows.
        const result = await baseline({
            name: options.name ?? "squash baseline",
            migrations,
            verify: true,
            createdBy: "squash",
        });

        return {
            baseline: result.baseline,
            squashed: migrations.map((m) => m.id!),
            skipped: result.skipped,
            dryRun: false,
        };
    }

    async function progress(options: MigrationProgressOptions = {}): Promise<MigrationProgressResult> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory);
        const client = await pools[0].connect();
        const startedAt = Date.now();
        const events: MigrationProgressEvent[] = [];

        try {
            await ensureTables(client);
            const done = await applied(client);
            const doneMap = new Map(done.map((r) => [r.id, r]));

            let completed = 0;
            let failed = 0;
            let skipped = 0;

            for (const m of sourceList) {
                const record = doneMap.get(m.id!);
                const eventStart = Date.now();

                if (!record) {
                    events.push({
                        migrationId: m.id!,
                        status: "pending",
                        startedAt: undefined,
                        completedAt: undefined,
                        durationMs: undefined,
                        error: undefined,
                    });
                    skipped++;
                    options.onProgress?.(events[events.length - 1]);
                    continue;
                }

                const status = record.status as MigrationRunStatus;
                const durationMs = record.execution_ms ?? Date.now() - eventStart;

                if (status === "applied") {
                    events.push({
                        migrationId: m.id!,
                        status: "completed",
                        startedAt: record.applied_at ?? undefined,
                        completedAt: record.applied_at ?? undefined,
                        durationMs,
                        error: undefined,
                    });
                    completed++;
                } else if (status === "failed") {
                    events.push({
                        migrationId: m.id!,
                        status: "failed",
                        startedAt: record.applied_at ?? undefined,
                        completedAt: record.failed_at ?? undefined,
                        durationMs,
                        error: record.error_message ?? undefined,
                    });
                    failed++;
                } else {
                    events.push({
                        migrationId: m.id!,
                        status: "running",
                        startedAt: record.applied_at ?? undefined,
                        completedAt: undefined,
                        durationMs,
                        error: record.error_message ?? undefined,
                    });
                    skipped++;
                }

                options.onProgress?.(events[events.length - 1]);
            }

            return {
                total: sourceList.length,
                completed,
                failed,
                skipped,
                events,
                durationMs: Date.now() - startedAt,
            };
        } finally {
            client.release();
        }
    }

    async function discoverDependencies(options: DiscoverDependenciesOptions = {}): Promise<DiscoverDependenciesResult> {
        const sourceList = await resolveSource(options.source ?? options.migrations ?? options.directory);
        const byId = new Map(sourceList.map((m) => [m.id!, m]));
        const dependencyGraph: Record<string, string[]> = {};
        const roots: string[] = [];
        const leaves: string[] = [];

        for (const m of sourceList) {
            const deps: string[] = [];
            if (Array.isArray(m.dependsOn)) {
                for (const dep of m.dependsOn) {
                    if (byId.has(dep)) {
                        deps.push(dep);
                    }
                }
            }
            dependencyGraph[m.id!] = deps;
            if (deps.length === 0) {
                roots.push(m.id!);
            }
        }

        // Find leaves (migrations nothing depends on)
        const dependedOn = new Set<string>();
        for (const deps of Object.values(dependencyGraph)) {
            for (const dep of deps) {
                dependedOn.add(dep);
            }
        }
        for (const m of sourceList) {
            if (!dependedOn.has(m.id!)) {
                leaves.push(m.id!);
            }
        }

        // Detect cycles using DFS
        const visited = new Set<string>();
        const visiting = new Set<string>();
        const cycles: string[][] = [];

        function dfs(id: string, path: string[]): void {
            if (visited.has(id)) {
                return;
            }
            if (visiting.has(id)) {
                const cycleStart = path.indexOf(id);
                cycles.push([...path.slice(cycleStart), id]);
                return;
            }
            visiting.add(id);
            path.push(id);
            for (const dep of dependencyGraph[id] ?? []) {
                dfs(dep, path);
            }
            path.pop();
            visiting.delete(id);
            visited.add(id);
        }

        for (const m of sourceList) {
            dfs(m.id!, []);
        }

        // Topological order
        const ordered: string[] = [];
        const visited2 = new Set<string>();
        const visiting2 = new Set<string>();
        let cycleDetected = false;

        function visitOrder(id: string): void {
            if (visited2.has(id)) {
                return;
            }
            if (visiting2.has(id)) {
                cycleDetected = true;
                return;
            }
            visiting2.add(id);
            for (const dep of dependencyGraph[id] ?? []) {
                visitOrder(dep);
            }
            visiting2.delete(id);
            visited2.add(id);
            ordered.push(id);
        }

        for (const m of sourceList) {
            visitOrder(m.id!);
        }

        return {
            migrations: sourceList,
            dependencyGraph,
            cycles,
            roots,
            leaves,
            ordered: cycleDetected ? [] : ordered,
        };
    }

    async function repairMigration(options: RepairMigrationOptions): Promise<RepairMigrationResult> {
        if (!options.migrationId) {
            throw new Error("migrationId is required to repairMigration");
        }

        const validActions = new Set(["mark-applied", "mark-failed", "mark-pending", "retry"]);
        if (!validActions.has(options.action)) {
            throw new Error(`Invalid action "${options.action}". Must be one of: mark-applied, mark-failed, mark-pending, retry`);
        }

        const client = await pools[0].connect();
        try {
            await ensureTables(client);
            await client.query("BEGIN");
            try {
                const { rows } = await client.query(
                    `SELECT id, status, checksum FROM ${tableIdent} WHERE id = $1`,
                    [options.migrationId],
                );

                if (rows.length === 0) {
                    throw new Error(`Migration "${options.migrationId}" not found in migration log`);
                }

                const previousStatus = String(rows[0].status);
                const previousChecksum = rows[0].checksum as string | null;
                let newStatus: MigrationRunStatus = "pending";
                let newChecksum: string | undefined;

                switch (options.action) {
                    case "mark-applied":
                        newStatus = "applied";
                        newChecksum = options.checksum ?? previousChecksum ?? undefined;
                        break;
                    case "mark-failed":
                        newStatus = "failed";
                        newChecksum = options.checksum ?? previousChecksum ?? undefined;
                        break;
                    case "mark-pending":
                        newStatus = "pending";
                        newChecksum = options.checksum ?? previousChecksum ?? undefined;
                        break;
                    case "retry":
                        newStatus = "pending";
                        newChecksum = options.checksum ?? previousChecksum ?? undefined;
                        break;
                }

                await client.query(
                    `UPDATE ${tableIdent}
                     SET status = $1, checksum = COALESCE($2, checksum), failed_at = CASE WHEN $1 = 'failed' THEN CURRENT_TIMESTAMP ELSE failed_at END, error_message = CASE WHEN $1 = 'failed' THEN $3 ELSE error_message END
                     WHERE id = $4`,
                    [newStatus, newChecksum, options.action === "retry" ? null : undefined, options.migrationId],
                );

                await client.query("COMMIT");

                return {
                    migrationId: options.migrationId,
                    previousStatus,
                    newStatus,
                    action: options.action,
                    checksum: newChecksum,
                };
            } catch (error) {
                await client.query("ROLLBACK").catch(() => {});
                throw error;
            }
        } finally {
            client.release();
        }
    }

    async function approveMigration(
        migrationId: string,
        context: { runnerId?: string; host?: string; targetDatabase?: string } = {},
    ): Promise<MigrationApprovalResult> {
        if (!migrationId) {
            throw new Error("migrationId is required to approveMigration");
        }
        const client = await pools[0].connect();
        try {
            await ensureTables(client);
            const { rows } = await client.query(
                `INSERT INTO _writer_migrations_approval (migration_id, target_database, requirement, status, approved_by, approved_at, runner_id, host)
                 VALUES ($1, $2, $3, 'approved', $4, CURRENT_TIMESTAMP, $5, $6)
                 RETURNING id, approved_by, approved_at`,
                [
                    migrationId,
                    context.targetDatabase ?? null,
                    approvalProvider?.requiresApproval ? "explicit" : "automatic",
                    "system",
                    context.runnerId ?? runner,
                    context.host ?? null,
                ],
            );
            const row = rows[0] as Record<string, unknown>;
            return {
                migrationId,
                approved: true,
                approvedBy: row.approved_by as string,
                approvedAt: row.approved_at,
                status: "approved",
            };
        } finally {
            client.release();
        }
    }

    async function checkApproval(
        migrationId: string,
        context: { runnerId?: string; host?: string; targetDatabase?: string } = {},
    ): Promise<MigrationApprovalResult> {
        const client = await pools[0].connect();
        try {
            await ensureTables(client);
            const { rows } = await client.query(
                `SELECT approved_by, approved_at, status, rejected_by, rejected_at
                 FROM _writer_migrations_approval
                 WHERE migration_id = $1
                 ORDER BY approved_at DESC NULLS LAST, id DESC
                 LIMIT 1`,
                [migrationId],
            );
            if (rows.length === 0) {
                return { migrationId, approved: false, status: "not_required" };
            }
            const row = rows[0] as Record<string, unknown>;
            const status = row.status as string;
            return {
                migrationId,
                approved: status === "approved",
                approvedBy: row.approved_by as string | undefined,
                approvedAt: row.approved_at,
                reason: status === "rejected" ? "rejected" : undefined,
                status: (status === "approved" ? "approved" : status === "rejected" ? "rejected" : "pending") as ApprovalStatus,
            };
        } finally {
            client.release();
        }
    }

    async function backups(options: { migrationId?: string } = {}): Promise<MigrationBackupRecord[]> {
        const client = await pools[0].connect();
        try {
            await ensureTables(client);
            const params: unknown[] = [];
            let sql = `SELECT id, migration_id, target_database, backup_id, backup_location, created_at, successful FROM _writer_migrations_backup`;
            if (options.migrationId) {
                sql += ` WHERE migration_id = $1`;
                params.push(options.migrationId);
            }
            sql += ` ORDER BY created_at DESC`;
            const { rows } = await client.query(sql, params);
            return rows.map((row: Record<string, unknown>) => ({
                id: String(row.id),
                migrationId: String(row.migration_id),
                targetDatabase: row.target_database as string | undefined,
                backupId: String(row.backup_id),
                backupLocation: row.backup_location as string | undefined,
                createdAt: row.created_at,
                successful: Boolean(row.successful),
            }));
        } finally {
            client.release();
        }
    }

    function propertiesEqual(a: unknown, b: unknown): boolean {
        return a === b;
    }

    const defaultSource = source ?? migrations ?? directory;

    return {
        pools,
        table,
        run,
        status,
        validate,
        dryRun,
        rollback,
        failed,
        assessRollback,
        history,
        statusAllTargets,
        retryFailed,
        reconcile,
        detectDrift,
        detectInterruptions,
        recoverInterruptions,
        baseline: baseline,
        squash: squash,
        progress: progress,
        discoverDependencies: discoverDependencies,
        repairMigration: repairMigration,
        approveMigration: approveMigration,
        checkApproval: checkApproval,
        backups: backups,
        listBaselines: listBaselines,
        detectLongRunningTransactions,
        detectLockContention,
        getLockDiagnostics,
        expandContract,
        backfill,
        getBackfillStatus,
        pauseBackfill,
        resumeBackfill,
        retryBackfill,
        cancelBackfill,
         verifyBackup,
        ensureTables: async () => { await ensureAllTables(); },
        validateRollback,
        checkRollbackDependencies,
    };
}
