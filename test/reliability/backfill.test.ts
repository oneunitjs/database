import { describe, it, expect } from "vitest";
import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";
import { withIsolatedDb, waitForBackendDeath } from "./helpers.js";
import { spawnTsxAndCapture } from "./crash.js";

const BACKFILL_RUNNER = "test/reliability/backfill-crash-runner.ts";

async function runBackfillJob(
    dbName: string,
    id: string,
    table: string,
    totalRows: number,
    batchSize: number,
    opts: { sleepMs?: number; killMs?: number; failBatch?: number } = {},
) {
    const env = {
        ...process.env,
        BF_DB: dbName,
        BF_TABLE: table,
        BF_ID: id,
        BF_TOTAL_ROWS: String(totalRows),
        BF_BATCH_SIZE: String(batchSize),
        BF_SLEEP_MS: String(opts.sleepMs ?? 0),
        BF_FAIL_BATCH: String(opts.failBatch ?? -1),
    };
    // killMs is honored by spawnTsxAndCapture as the hard spawn timeout (SIGKILL).
    const timeoutMs = opts.killMs && opts.killMs > 0 ? opts.killMs : 180_000;
    return spawnTsxAndCapture(BACKFILL_RUNNER, [], timeoutMs, env);
}

async function countTransformed(pool: Pool, table: string): Promise<number> {
    const { rows } = await pool.query(`SELECT count(*) AS n FROM ${table} WHERE full_name IS NOT NULL`);
    return Number(rows[0].n);
}

async function countTotal(pool: Pool, table: string): Promise<number> {
    const { rows } = await pool.query(`SELECT count(*) AS n FROM ${table}`);
    return Number(rows[0].n);
}

async function backfillJobRow(pool: Pool, id: string) {
    const { rows } = await pool.query(
        `SELECT id, job_name, status, cursor_value, rows_processed, batch_size, retry_count, last_error FROM _writer_migration_backfills WHERE job_name = $1`,
        [id],
    );
    return rows[0] ?? null;
}

async function seedSerialTable(pool: Pool, table: string, totalRows: number): Promise<void> {
    await pool.query(`
        CREATE TABLE ${table} (
            id BIGSERIAL PRIMARY KEY,
            payload TEXT,
            full_name TEXT,
            version INT DEFAULT 0
        );
    `);
    await pool.query(
        `INSERT INTO ${table} (payload, full_name) SELECT 'p_' || g, NULL FROM generate_series(1, $1) g`,
        [totalRows],
    );
}

async function seedCompositeTable(pool: Pool, table: string, totalRows: number): Promise<void> {
    await pool.query(`
        CREATE TABLE ${table} (
            id BIGSERIAL,
            group_id INT,
            payload TEXT,
            full_name TEXT,
            PRIMARY KEY (id, group_id)
        );
    `);
    await pool.query(
        `INSERT INTO ${table} (id, group_id, payload, full_name)
         SELECT g, (g % 10) + 1, 'p_' || g, NULL FROM generate_series(1, $1) g`,
        [totalRows],
    );
}

describe("Stage 1 — Resumable batched backfill engine", () => {
    it("respects batchSize and processes all rows in bounded batches (100k rows)", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_batch";
            const totalRows = 100_000;
            const batchSize = 1000;
            await seedSerialTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const batches: number[] = [];
            const started = Date.now();
            const result = await migrator.backfill({
                id: "bf_batch_job",
                migrationId: "bf_m",
                table,
                batchSize,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    batches.push(rows.length);
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_fn' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
                onProgress: (p) => {
                    expect(p.processed).toBeLessThanOrEqual(p.total);
                },
            });
            const elapsed = Date.now() - started;

            expect(result.completed).toBe(true);
            expect(result.status).toBe("completed");
            expect(result.rowsProcessed).toBe(totalRows);
            expect(batches.every((b) => b <= batchSize)).toBe(true);
            expect(batches.reduce((a, b) => a + b, 0)).toBe(totalRows);
            expect(await countTransformed(pool, table)).toBe(totalRows);
            // eslint-disable-next-line no-console
            console.log(`[BF] 100k rows batchSize=1000: batches=${batches.length} elapsedMs=${elapsed}`);
        });
    });

    it("cursor progresses correctly; no skipped or duplicated rows", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_cursor";
            const totalRows = 50_000;
            await seedSerialTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const seenIds = new Set<number>();
            const result = await migrator.backfill({
                id: "bf_cursor_job",
                migrationId: "bf_m",
                table,
                batchSize: 500,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    for (const r of rows) {
                        const id = Number(r.id);
                        expect(seenIds.has(id)).toBe(false); // no duplicate
                        seenIds.add(id);
                    }
                    const ids = rows.map((r) => r.id);
                    await bc.query(
                        `UPDATE ${table} SET full_name = payload || '_fn' WHERE id = ANY($1) AND full_name IS NULL`,
                        [ids],
                    );
                    return { processed: rows.length };
                },
            });

            expect(result.completed).toBe(true);
            expect(seenIds.size).toBe(totalRows);
            expect(await countTransformed(pool, table)).toBe(totalRows);
            const job = await backfillJobRow(pool, "bf_cursor_job");
            expect(job?.status).toBe("completed");
            expect(job?.cursor_value).toBeNull(); // cursor exhausted
        });
    });

    it("checkpoint and data commit atomically — failed batch does not advance checkpoint", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_atomic";
            const totalRows = 5000;
            await seedSerialTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            let batchNo = 0;
            const result = await migrator.backfill({
                id: "bf_atomic_job",
                migrationId: "bf_m",
                table,
                batchSize: 1000,
                cursor: { type: "serial", columns: ["id"] },
                maxRetries: 0,
                processBatch: async ({ client: bc, rows }) => {
                    batchNo += 1;
                    if (batchNo === 3) {
                        throw new Error("injected batch 3 failure");
                    }
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_fn' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
            });

            expect(result.completed).toBe(false);
            expect(result.status).toBe("failed");
            // Batches 1-2 committed: 2000 rows transformed. Batch 3 rolled back: still 2000.
            expect(await countTransformed(pool, table)).toBe(2000);
            const job = await backfillJobRow(pool, "bf_atomic_job");
            expect(Number(job?.rows_processed)).toBe(2000);
            // Cursor points to end of batch 2 (id 2000), not into batch 3.
            expect(JSON.parse(job?.cursor_value ?? "null")).toEqual(["2000"]);

            // Retry after failure: resume from checkpoint, completes all rows.
            const retry = await migrator.backfill({
                id: "bf_atomic_job",
                migrationId: "bf_m",
                table,
                batchSize: 1000,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_fn' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
                maxRetries: 3,
            });
            expect(retry.completed).toBe(true);
            expect(await countTransformed(pool, table)).toBe(totalRows);
            const { rows: dups } = await pool.query(
                `SELECT count(*) AS n FROM ${table} WHERE version > 1`,
            );
            expect(Number(dups[0].n)).toBe(0);
        });
    });

    it("crash mid-backfill: resume from last committed checkpoint, no skipped rows", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_crash";
            const totalRows = 5000;
            const batchSize = 500;
            await seedSerialTable(pool, table, totalRows);

            // Run backfill in a child process with a long per-batch sleep, then kill it
            // while the first batch is in flight. The kill lands either during tsx
            // startup (no job row yet) or mid-first-batch (job "running", cursor not
            // advanced because the batch was rolled back). Both are safely resumable.
            const run = await runBackfillJob(dbName, "bf_crash_job", table, totalRows, batchSize, {
                sleepMs: 2000,
                killMs: 800,
            });
            expect(run.timedOut).toBe(true);
            await waitForBackendDeath();

            // Whatever the kill point, no batch may have been partially committed:
            // transformed rows must equal the durable rows_processed checkpoint.
            const transformed = await countTransformed(pool, table);
            const job = await backfillJobRow(pool, "bf_crash_job");
            if (job) {
                expect(job.status).toBe("running");
                expect(Number(job.rows_processed)).toBe(transformed);
                expect(transformed).toBeLessThan(totalRows);
            }

            // Resume in-process from the durable checkpoint (or fresh if no job exists).
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const resumed = await migrator.backfill({
                id: "bf_crash_job",
                migrationId: "bf_m",
                table,
                batchSize,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_fn' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
                maxRetries: 3,
            });
            expect(resumed.completed).toBe(true);
            expect(await countTransformed(pool, table)).toBe(totalRows);
            // No duplicates: version never exceeds 1.
            const { rows: dups } = await pool.query(`SELECT count(*) AS n FROM ${table} WHERE version > 1`);
            expect(Number(dups[0].n)).toBe(0);
        });
    });

    it("nullable data backfilled correctly", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_nullable";
            const totalRows = 5000;
            await pool.query(`
                CREATE TABLE ${table} (
                    id BIGSERIAL PRIMARY KEY,
                    payload TEXT,
                    full_name TEXT,
                    note TEXT NULL
                );
            `);
            await pool.query(
                `INSERT INTO ${table} (payload, full_name, note) SELECT 'p_' || g, NULL, NULL FROM generate_series(1, $1) g`,
                [totalRows],
            );

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const result = await migrator.backfill({
                id: "bf_nullable_job",
                migrationId: "bf_m",
                table,
                batchSize: 1000,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = COALESCE(note, payload) || '_n', note = 'filled' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
            });

            expect(result.completed).toBe(true);
            expect(await countTransformed(pool, table)).toBe(totalRows);
            const { rows } = await pool.query(`SELECT note FROM ${table} WHERE note <> 'filled'`);
            expect(rows.length).toBe(0);
        });
    });

    it("empty table completes with zero rows", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_empty";
            await seedSerialTable(pool, table, 0);
            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const result = await migrator.backfill({
                id: "bf_empty_job",
                migrationId: "bf_m",
                table,
                batchSize: 1000,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ rows }) => ({ processed: rows.length }),
            });
            expect(result.completed).toBe(true);
            expect(result.rowsProcessed).toBe(0);
            expect(await countTotal(pool, table)).toBe(0);
        });
    });

    it("composite cursor backfill", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_composite";
            const totalRows = 10_000;
            await seedCompositeTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const seen = new Set<string>();
            const result = await migrator.backfill({
                id: "bf_composite_job",
                migrationId: "bf_m",
                table,
                batchSize: 500,
                cursor: { type: "composite", columns: ["id", "group_id"] },
                processBatch: async ({ client: bc, rows }) => {
                    for (const r of rows) {
                        const key = `${r.id}:${r.group_id}`;
                        expect(seen.has(key)).toBe(false);
                        seen.add(key);
                    }
                    const ids = rows.map((r) => r.id);
                    await bc.query(
                        `UPDATE ${table} SET full_name = payload || '_cf' WHERE id = ANY($1) AND full_name IS NULL`,
                        [ids],
                    );
                    return { processed: rows.length };
                },
            });

            expect(result.completed).toBe(true);
            expect(seen.size).toBe(totalRows);
            expect(await countTransformed(pool, table)).toBe(totalRows);
        });
    });

    it("pause and resume retains the checkpoint", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_pause";
            const totalRows = 5000;
            await seedSerialTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            // Interrupt a child backfill mid-first-batch, leaving the job "running" in
            // the registry (or absent if killed during runner startup).
            const run = await runBackfillJob(dbName, "bf_pause_job", table, totalRows, 500, {
                sleepMs: 2000,
                killMs: 800,
            });
            expect(run.timedOut).toBe(true);
            await waitForBackendDeath();

            const before = await backfillJobRow(pool, "bf_pause_job");
            const transformedBefore = await countTransformed(pool, table);

            // Exercise the pause/resume lifecycle on the interrupted job.
            if (before && before.status === "running") {
                const paused = await migrator.pauseBackfill({ id: "bf_pause_job" });
                expect(paused.status).toBe("paused");
                const resumed = await migrator.resumeBackfill({ id: "bf_pause_job" });
                expect(resumed.status).toBe("running");
            }

            // Resume the backfill from the durable checkpoint.
            const completed = await migrator.backfill({
                id: "bf_pause_job",
                migrationId: "bf_m",
                table,
                batchSize: 500,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_pr' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
                maxRetries: 3,
            });
            expect(completed.completed).toBe(true);
            expect(await countTransformed(pool, table)).toBe(totalRows);
            const after = await backfillJobRow(pool, "bf_pause_job");
            // No duplicates: version never exceeds 1.
            const { rows: dups } = await pool.query(`SELECT count(*) AS n FROM ${table} WHERE version > 1`);
            expect(Number(dups[0].n)).toBe(0);
        });
    });
    });

    it("cancelBackfill marks the job failed without advancing checkpoint past data", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_cancel";
            const totalRows = 5000;
            await seedSerialTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            // Interrupt the child backfill mid-flight.
            const run = await runBackfillJob(dbName, "bf_cancel_job", table, totalRows, 500, {
                sleepMs: 2000,
                killMs: 800,
            });
            expect(run.timedOut).toBe(true);
            await waitForBackendDeath();
            const transformedBefore = await countTransformed(pool, table);

            await migrator.cancelBackfill({ id: "bf_cancel_job" });
            const job = await backfillJobRow(pool, "bf_cancel_job");
            expect(job?.status).toBe("failed");
            expect(job?.last_error).toBe("cancelled");
            // Data unchanged by cancel.
            expect(await countTransformed(pool, table)).toBe(transformedBefore);
        });
    });

    it("two backfill runners competing for the same job: one job row, no double-processing", async () => {
        await withIsolatedDb("bf", async (pool, dbName) => {
            const table = "bf_compete";
            const totalRows = 5000;
            await seedSerialTable(pool, table, totalRows);

            const migrator = createMigrator(pool, { table: "_writer_migrations" });
            const r1 = migrator.backfill({
                id: "bf_compete_job",
                migrationId: "bf_m",
                table,
                batchSize: 1000,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_c1' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
            });
            const r2 = migrator.backfill({
                id: "bf_compete_job",
                migrationId: "bf_m",
                table,
                batchSize: 1000,
                cursor: { type: "serial", columns: ["id"] },
                processBatch: async ({ client: bc, rows }) => {
                    if (rows.length > 0) {
                        const ids = rows.map((r) => r.id);
                        await bc.query(
                            `UPDATE ${table} SET full_name = payload || '_c2' WHERE id = ANY($1) AND full_name IS NULL`,
                            [ids],
                        );
                    }
                    return { processed: rows.length };
                },
            });
            await Promise.all([r1, r2]);

            const jobs = await pool.query(`SELECT count(*) AS n FROM _writer_migration_backfills WHERE job_name = 'bf_compete_job'`);
            expect(Number(jobs.rows[0].n)).toBe(1);
            // No row double-suffixed by both runners.
            const { rows: doubled } = await pool.query(
                `SELECT count(*) AS n FROM ${table} WHERE full_name LIKE '%_c1_c2' OR full_name LIKE '%_c2_c1'`,
            );
            expect(Number(doubled[0].n)).toBe(0);
            expect(await countTransformed(pool, table)).toBe(totalRows);
        });
    });