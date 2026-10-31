import type { Pool, PoolClient, QueryResult, QueryResultRow, PoolConfig } from "pg";
import type { Readable } from "node:stream";

export type Logger = {
    debug?(payload: unknown, message?: string): void;
    info?(payload: unknown, message?: string): void;
    warn?(payload: unknown, message?: string): void;
    error?(payload: unknown, message?: string): void;
};

export type SslConfig =
    | false
    | {
          rejectUnauthorized?: boolean;
          ca?: string;
      };

export interface DatabaseConfig extends PoolConfig {
    connectionString?: string;
    host?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;
    max?: number;
    min?: number;
    idleTimeoutMillis?: number;
    connectionTimeoutMillis?: number;
    statement_timeout?: number;
    query_timeout?: number;
    keepAlive?: boolean;
    application_name?: string;
    ssl?: SslConfig;
}

export interface RetryOptions {
    retries?: number;
    maxRetries?: number;
    baseDelay?: number;
    maxDelay?: number;
    factor?: number;
    jitter?: number;
    retryOnTimeout?: boolean;
    signal?: AbortSignal;
    shouldRetry?(error: unknown): boolean;
    onRetry?(info: RetryInfo): void;
    sleep?(ms: number): Promise<void>;
}

export interface RetryInfo {
    error: DatabaseError;
    attempt: number;
    delay: number;
    retries: number;
}

export interface QueryOptions {
    timeout?: number;
    retry?: boolean | number | RetryOptions;
    destroyOnError?: boolean;
    values?: unknown[];
    [key: string]: unknown;
}

export interface TransactionOptions {
    isolation?: "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE" | string;
    readOnly?: boolean;
    deferrable?: boolean;
    timeout?: number;
    retry?: boolean | number | RetryOptions;
}

export interface InstrumentationOptions {
    logQueries?: boolean;
    logParameters?: boolean;
    slowQueryMs?: number;
    onQuery?(event: {
        sql?: string;
        parameters?: unknown;
        durationMs: number;
        rowCount?: number;
        success?: boolean;
    }): void;
    onError?(event: { error: unknown; sql?: string; durationMs: number }): void;
    onRetry?(info: RetryInfo): void;
}

export interface CreateDatabaseOptions extends DatabaseConfig {
    logger?: Logger;
    instrumentation?: InstrumentationOptions;
    hooks?: Hooks;
    retry?: boolean | number | RetryOptions;
    queryTimeout?: number;
    timeout?: number;
    connectTimeout?: number;
}

export interface PoolStats {
    total: number;
    idle: number;
    waiting: number;
}

export interface HealthCheckResult {
    status: "up" | "down";
    latency: { value: number; unit: "ms" };
    pool?: PoolStats;
    error?: string;
    code?: string;
}

export interface HealthResult extends HealthCheckResult {
    healthy: boolean;
    checkedAt: string;
    error: string | null;
    errorCode: string | null;
}

export interface SqlFragment {
    text: string;
    values: unknown[];
}

export interface PreparedStatement {
    name: string;
    text: string;
    execute(values?: unknown[], options?: QueryOptions): Promise<QueryResult>;
}

export interface CursorHandle {
    read(count?: number): Promise<QueryResultRow[]>;
    close(error?: unknown): Promise<void>;
    [Symbol.asyncIterator](): AsyncIterator<QueryResultRow>;
}

export interface InsertManyOptions {
    columns?: string[];
    returning?: string | string[];
    chunkSize?: number;
    onConflict?:
        | true
        | "nothing"
        | {
              columns?: string[];
              constraint?: string;
              update?: string[];
              do?: "nothing" | string;
              action?: "nothing" | string;
              id?: string;
          };
    transaction?: boolean;
    client?: Pick<PoolClient, "query">;
    queryOptions?: QueryOptions;
}

export interface BatchApi {
    insertMany(table: string, rows: Record<string, unknown>[], options?: InsertManyOptions): Promise<unknown>;
}

export interface QueryBuilder {
    select(...columns: Array<string | string[] | SqlFragment>): QueryBuilder;
    distinct(value?: boolean): QueryBuilder;
    insert(values: Record<string, unknown> | Record<string, unknown>[]): QueryBuilder;
    values(values: Record<string, unknown> | Record<string, unknown>[]): QueryBuilder;
    update(values: Record<string, unknown>): QueryBuilder;
    delete(): QueryBuilder;
    returning(...columns: Array<string | string[]>): QueryBuilder;
    where(column: string | Record<string, unknown> | SqlFragment, operator?: unknown, value?: unknown): QueryBuilder;
    orWhere(column: string, operator?: unknown, value?: unknown): QueryBuilder;
    whereNull(column: string): QueryBuilder;
    whereNotNull(column: string): QueryBuilder;
    whereIn(column: string, values: unknown[]): QueryBuilder;
    whereRaw(fragment: SqlFragment): QueryBuilder;
    join(table: string, on: string | SqlFragment): QueryBuilder;
    leftJoin(table: string, on: string | SqlFragment): QueryBuilder;
    groupBy(...columns: Array<string | string[]>): QueryBuilder;
    having(column: string, operator: string, value: unknown): QueryBuilder;
    orderBy(column: string, direction?: string): QueryBuilder;
    limit(value: number): QueryBuilder;
    offset(value: number): QueryBuilder;
    onConflict(options: InsertManyOptions["onConflict"]): QueryBuilder;
    toSQL(): { text: string; values: unknown[] };
    execute(options?: QueryOptions): Promise<QueryResult>;
    first(options?: QueryOptions): Promise<QueryResultRow | null>;
    then<TResult1 = QueryResult, TResult2 = never>(
        onfulfilled?: ((value: QueryResult) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ): Promise<TResult1 | TResult2>;
}

export interface ModelFindOptions extends QueryOptions {
    columns?: string[];
    orderBy?: string | Array<[string, string?]>;
    direction?: string;
    limit?: number;
    offset?: number;
    client?: Pick<PoolClient, "query">;
    returning?: string[];
}

export interface Model {
    table: string;
    idColumn: string;
    query(): QueryBuilder;
    builder(): QueryBuilder;
    select(): QueryBuilder;
    find(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    findOne(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    findById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    insert(data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    insertMany(rows: Record<string, unknown>[], options?: ModelFindOptions): Promise<QueryResultRow[]>;
    update(where: Record<string, unknown>, data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    updateById(id: unknown, data: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    delete(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    deleteById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    remove(where: Record<string, unknown>, options?: ModelFindOptions): Promise<QueryResultRow[]>;
    removeById(id: unknown, options?: ModelFindOptions): Promise<QueryResultRow | null>;
    count(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<number>;
    exists(where?: Record<string, unknown>, options?: ModelFindOptions): Promise<boolean>;
}

export interface MigrationDefinition {
    id?: string;
    name?: string;
    up: string | ((context: { client: PoolClient; query: PoolClient["query"]; logger?: Logger }) => unknown);
    down?: string | ((context: { client: PoolClient; query: PoolClient["query"]; logger?: Logger }) => unknown);
    sql?: string;
}

export interface Migrator {
    table: string;
    run(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<string[]>;
    status(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Promise<Array<{
        id: string;
        name: string;
        applied: boolean;
        appliedAt: unknown;
    }>>;
    rollback(options?: { source?: unknown; migrations?: MigrationDefinition[]; directory?: string; steps?: number }): Promise<string[]>;
}

export interface SchemaCreate {
    table(name: string, columns: Record<string, string>): Promise<void>;
    index(name: string, table: string, columns: string | string[]): Promise<void>;
}

export interface SchemaAlter {
    addColumns(table: string, columns: Record<string, string>): Promise<void>;
    dropColumns(table: string, columns: string | string[]): Promise<void>;
    renameColumn(table: string, oldName: string, newName: string): Promise<void>;
    alterColumn(table: string, column: string, type: string): Promise<void>;
    renameTable(oldName: string, newName: string): Promise<void>;
    unique(table: string, constraint: string, columns: string | string[]): Promise<void>;
}

export interface SchemaDrop {
    table(name: string, options?: { cascade?: boolean }): Promise<void>;
    index(name: string, options?: { cascade?: boolean }): Promise<void>;
}

export interface SchemaConstrain {
    unique(tableName: string, constraint: string, columns: string | string[]): Promise<void>;
    foreignKey(
        tableName: string,
        constraint: string,
        columns: string | string[],
        referenceTable: string,
        referenceColumns: string | string[],
        options?: { onDelete?: string; onUpdate?: string },
    ): Promise<void>;
    check(tableName: string, constraint: string, expression: string): Promise<void>;
    dropConstraint(tableName: string, constraint: string): Promise<void>;
}

export interface SchemaManager {
    create: SchemaCreate;
    alter: SchemaAlter;
    drop: SchemaDrop;
    constrain: SchemaConstrain;
}

export interface Metrics {
    recordQuery(durationMs?: number): void;
    recordError(): void;
    recordSlowQuery(): void;
    recordRetry(): void;
    recordTransaction(rolledBack?: boolean): void;
    snapshot(): Record<string, number>;
    reset(): void;
}

export interface Hooks {
    metrics: Metrics;
    beforeQuery(context: { sql?: string; parameters?: unknown; startedAt?: number; rowCount?: number }): void;
    afterQuery(context: { sql?: string; parameters?: unknown; startedAt?: number; rowCount?: number }): void;
    onQueryError(context: { sql?: string; parameters?: unknown; startedAt?: number }, error: DatabaseError): void;
    recordRetry(info: RetryInfo): void;
    config: InstrumentationOptions;
}

export interface RetryApi {
    run<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
    retry<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
    withRetry<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
    isTransient(error: unknown): boolean;
}

export interface Database {
    pool: Pool;
    getClient(options?: { timeout?: number }): Promise<PoolClient>;
    query<T extends QueryResultRow = QueryResultRow>(text: string | object, values?: unknown[] | QueryOptions, options?: QueryOptions): Promise<QueryResult<T>>;
    queryOne<T extends QueryResultRow = QueryResultRow>(text: string | object, values?: unknown[] | QueryOptions, options?: QueryOptions): Promise<T | null>;
    releaseClient(client?: PoolClient | null, error?: unknown): void;
    prepare(name: string, text: string): PreparedStatement;
    prepared(name: string, text: string, values?: unknown[], options?: QueryOptions): Promise<QueryResult>;
    transaction<T>(callback: (client: PoolClient) => Promise<T> | T, options?: TransactionOptions): Promise<T>;
    savepoint<T>(client: PoolClient, name: string, callback: (client: PoolClient) => Promise<T> | T): Promise<T>;
    stream(text: string, values?: unknown[], options?: object): Promise<Readable>;
    cursor(text: string, values?: unknown[], options?: object): Promise<CursorHandle>;
    check(options?: { timeout?: number }): Promise<HealthCheckResult>;
    health(options?: { timeout?: number }): Promise<HealthResult>;
    shutdown(options?: { timeout?: number }): Promise<void>;
    batch: BatchApi;
    from(table: string): QueryBuilder;
    table(table: string): QueryBuilder;
    model(table: string, options?: { idColumn?: string }): Model;
    schema: SchemaManager;
    migrate: Migrator;
    hooks: Hooks;
    metrics: Metrics;
    retry: RetryApi;
}

export class DatabaseError extends Error {
    name: "DatabaseError";
    code?: string;
    detail?: string;
    hint?: string;
    constraint?: string;
    table?: string;
    column?: string;
    schema?: string;
    dataType?: string;
    severity?: string;
    position?: string;
    where?: string;
    routine?: string;
    sqlState?: string;
    transient: boolean;
    cause?: unknown;
    toJSON(): Record<string, unknown>;
}

export class TimeoutError extends DatabaseError {
    name: "TimeoutError";
    timeout?: number;
}

export const dbConfig: DatabaseConfig;

export function createDatabase(options?: CreateDatabaseOptions): Database;
export function createPool(options?: { logger?: Logger; config?: DatabaseConfig; onError?(error: unknown): void }): Pool;
export function createClient(pool: Pool, logger?: Logger, options?: CreateDatabaseOptions): Pick<Database, "getClient" | "query" | "queryOne" | "releaseClient" | "prepare" | "prepared" | "hooks">;
export function createTransaction(options: { pool: Pool; logger?: Logger; hooks?: Hooks; retry?: boolean | number | RetryOptions }): Pick<Database, "transaction" | "savepoint">;
export function createStream(pool: Pool, logger?: Logger, defaults?: { timeout?: number }): Pick<Database, "stream">;
export function createCursor(pool: Pool): Pick<Database, "cursor">;
export function createCheck(pool: Pool): Pick<Database, "check" | "health">;
export function createShutdown(pool: Pool, options?: { logger?: Logger; timeout?: number }): { shutdown: Database["shutdown"]; readonly isClosed: boolean };
export function createBatch(options?: { query?: Database["query"]; transaction?: Database["transaction"] }): BatchApi;
export function createSchemaManager(pool: Pool): SchemaManager;
export function createMigrator(pool: Pool, options?: { logger?: Logger; table?: string; lockKey?: number; source?: unknown; migrations?: MigrationDefinition[]; directory?: string }): Migrator;
export function createModel(options: { query: Database["query"]; table: string; idColumn?: string; logger?: Logger; transaction?: Database["transaction"] }): Model;
export function createModelFactory(options?: { query?: Database["query"]; logger?: Logger; transaction?: Database["transaction"] }): Database["model"];
export function createQueryBuilder(options?: { query?: Database["query"]; logger?: Logger }): Database["from"];
export function createHooks(options?: { logger?: Logger; metrics?: Metrics; instrumentation?: InstrumentationOptions } & InstrumentationOptions): Hooks;
export function createMetrics(): Metrics;
export function createRetry(defaults?: RetryOptions): RetryApi;
export function loadDatabaseConfig(overrides?: DatabaseConfig, env?: NodeJS.ProcessEnv): DatabaseConfig;
export function parseSslConfig(env?: NodeJS.ProcessEnv): SslConfig;
export function withRetry<T>(fn: (attempt: number) => Promise<T>, options?: RetryOptions): Promise<T>;
export function normalizeError(error: unknown): DatabaseError;
export function isDatabaseError(error: unknown): error is DatabaseError;
export function isTransientError(error: unknown): boolean;
export function isUniqueViolation(error: unknown): boolean;
export function isForeignKeyViolation(error: unknown): boolean;
export function isNotNullViolation(error: unknown): boolean;
export function isCheckViolation(error: unknown): boolean;
export function isExclusionViolation(error: unknown): boolean;
export function isSerializationFailure(error: unknown): boolean;
export function isDeadlock(error: unknown): boolean;
export function isConnectionError(error: unknown): boolean;
export function isConstraintViolation(error: unknown): boolean;

export function sql(strings: TemplateStringsArray, ...values: unknown[]): SqlFragment;
export namespace sql {
    function raw(text: string): SqlFragment;
    function identifier(name: string): SqlFragment;
    function quoteIdent(name: string): string;
    function quoteIdentList(names: string | string[]): string;
    function isFragment(value: unknown): boolean;
    function join(values: unknown[], separator?: string): SqlFragment;
}

export function raw(text: string): SqlFragment;
export function identifier(name: string): SqlFragment;
export function quoteIdent(name: string): string;
export function quoteIdentList(names: string | string[]): string;

export function id(): { id: string };
export function bigIntId(): { id: string };
export function uuid(columnName?: string): Record<string, string>;
export function timestamp(...columnNames: string[]): Record<string, string>;

export function encode(value: unknown): Buffer;
export function decode(buffer: Buffer | Uint8Array | string): unknown;
export function encodeToString(value: unknown): string;
export function decodeFromString(value: string): unknown;
export function serializeRow(row: unknown): Buffer;
export function deserializeRow(buffer: Buffer | Uint8Array | string): unknown;
export function serializeQueryResult(result: Pick<QueryResult, "rows" | "rowCount" | "fields">): Buffer;
export function deserializeQueryResult(buffer: Buffer | Uint8Array | string): unknown;
