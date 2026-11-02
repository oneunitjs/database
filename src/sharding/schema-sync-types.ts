import type { MigrationDefinition } from "../types.js";
export type Migration = MigrationDefinition;

// ---------------------------------------------------------------------------
// Schema Auto-Sync Layer — Type Definitions
//
// Defines interfaces for cross-shard schema synchronization, including state
// tracking, validation results, sync operations, and configuration options.
// ---------------------------------------------------------------------------

export type SchemaSyncStatus =
  | "pending"
  | "applying"
  | "validating"
  | "completed"
  | "failed"
  | "rolled_back"
  | "cancelling"
  | "cancelled";

export type SchemaDiffType =
  | "create_table"
  | "drop_table"
  | "alter_table"
  | "add_column"
  | "drop_column"
  | "alter_column"
  | "add_index"
  | "drop_index"
  | "add_constraint"
  | "drop_constraint";

export type ShardSchemaVersion = ShardSchemaInfo;
export interface ShardSchemaInfo {
  shardId: string;
  schemaVersion: string;
  version?: string;
  state?: string;
  checksum: string;
  tables: Record<string, TableSchemaInfo>;
  appliedAt: Date;
  isConsistent?: boolean;
  lastVerifiedAt?: Date;
}

export interface TableSchemaInfo {
  name: string;
  columns: ColumnSchemaInfo[];
  indexes: IndexSchemaInfo[];
  constraints: ConstraintSchemaInfo[];
}

export interface ColumnSchemaInfo {
  name: string;
  dataType: string;
  isNullable: boolean;
  defaultValue?: string | null;
  characterMaximumLength?: number | null;
  numericPrecision?: number | null;
  numericScale?: number | null;
}

export interface IndexSchemaInfo {
  name: string;
  columns: string[];
  unique: boolean;
}

export interface ConstraintSchemaInfo {
  name: string;
  type: "primary_key" | "foreign_key" | "unique" | "check";
  columns: string[];
  definition?: string;
}

export interface SchemaSyncOptions {
  /** Maximum time to wait for the entire sync to complete (default: 300000ms / 5 min) */
  timeoutMs?: number;
  /** Timeout for each shard migration (default: 60000ms / 1min) */
  shardTimeoutMs?: number;
  /** Maximum number of retry attempts per shard (default: 3) */
  maxRetries?: number;
  /** Base delay for exponential backoff (default: 1000ms) */
  retryBaseDelayMs?: number;
  /** Maximum retry delay (default: 30000ms) */
  retryMaxDelayMs?: number;
  /** Apply migrations to shards in parallel (default: false) */
  parallel?: boolean;
  /** Maximum number of shards to apply in parallel (default: 10) */
  maxParallelShards?: number;
  /** Alias for maxParallelShards */
  maxConcurrentShards?: number;
  /** Alias for maxParallelShards */
  maxConcurrent?: number;
  /** Alias for maxParallelShards */
  maxConcurrency?: number;
  /** Skip shards that are unavailable rather than failing entirely (default: false) */
  skipUnavailableShards?: boolean;
  /** Continue on error rather than stopping at first failure (default: false) */
  continueOnError?: boolean;
  /** Validate schema consistency after applying migrations (default: true) */
  validateConsistency?: boolean;
  /** Validate schema but do not apply migrations (default: false) */
  dryRun?: boolean;
  /** Block writes to shards with stale schemas (default: false) */
  blockWritesOnInconsistency?: boolean;
  /** Lock TTL in milliseconds (default: 300000ms / 5 min) */
  syncLockTtlMs?: number;
  /** Maximum time to wait to acquire sync lock (default: 60000ms / 1min) */
  syncLockAcquireTimeoutMs?: number;
}

export interface SchemaSyncResult {
  operationId: string;
  status: SchemaSyncStatus;
  migrationVersion: string;
  migrationName: string;
  totalShards: number;
  invalidatedCount: number;
  activeTransactionCount: number;
  errors: string[];
  // Compatibility with the plan's SchemaSyncResult
  successfulShards?: number;
  failedShards?: number;
  shardResults?: ShardSyncResult[];
  durationMs?: number;
  error?: Error;
  checksum?: string;
}

export interface ShardSyncResult {
  shardId: string;
  success: boolean;
  error?: Error;
  durationMs: number;
  retries: number;
  checksum?: string;
}

export interface SchemaConsistencyReport {
  isConsistent: boolean;
  expectedVersion: string;
  expectedChecksum: string;
  shards: ShardSchemaInfo[];
  inconsistentShards: ShardSchemaInfo[];
  missingShards: string[];
}

export interface SchemaSyncHooks {
  onSyncStart?: (operation: SchemaSyncOperation) => Promise<void>;
  onSyncComplete?: (
    operation: SchemaSyncOperation,
    result: SchemaSyncResult,
  ) => Promise<void>;
  onSyncFail?: (operation: SchemaSyncOperation, error: Error) => Promise<void>;
  onShardStart?: (
    operation: SchemaSyncOperation,
    shardId: string,
  ) => Promise<void>;
  onShardComplete?: (
    operation: SchemaSyncOperation,
    shardId: string,
    result: ShardSyncResult,
  ) => Promise<void>;
  onShardFail?: (
    operation: SchemaSyncOperation,
    shardId: string,
    error: Error,
  ) => Promise<void>;
  onValidationStart?: (operation: SchemaSyncOperation) => Promise<void>;
  onValidationComplete?: (
    operation: SchemaSyncOperation,
    report: SchemaConsistencyReport,
  ) => Promise<void>;
  onRollbackStart?: (operation: SchemaSyncOperation) => Promise<void>;
  onRollbackComplete?: (
    operation: SchemaSyncOperation,
  ) => Promise<void>;
}

export interface SchemaSyncOperation {
  operationId: SyncOperationId;
  status: SchemaSyncStatus;
  migrationVersion: string;
  migrationName: string;
  migrationSql: string;
  checksum: string;
  startedAt: Date;
  completedAt?: Date;
  errorMessage?: string;
  errorStack?: string;
  retries: number;
  createdAt: Date;
  updatedAt: Date;
}

export type SyncOperationId = string;

export interface SyncStateStore {
  loadState(shardId: string): Promise<ShardSchemaInfo | null>;
  saveState(
    shardId: string,
    state: ShardSchemaInfo,
    expectedVersion: number,
  ): Promise<boolean>;
  transitionState(
    shardId: string,
    fromState: string,
    toState: string,
    reason: string,
    context: { timestamp: Date; initiatedBy: string },
  ): Promise<boolean>;
  listStates(): Promise<Map<string, ShardSchemaInfo>>;
  acquireLock(shardId: string, ttlMs: number): Promise<boolean>;
  releaseLock(shardId: string): Promise<void>;
  getStateHistory(shardId: string, limit?: number): Promise<
    {
      version: number;
      shardId: string;
      state: any;
      previousVersion: number;
      changedBy: string;
      changedAt: Date;
      reason?: string;
    }[]
  >;
}

export type FailoverStateStore = SyncStateStore;