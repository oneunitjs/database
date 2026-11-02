import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";

const dbName = process.env.BF_DB ?? "";
const table = process.env.BF_TABLE ?? "";
const id = process.env.BF_ID ?? "";
const totalRows = Number(process.env.BF_TOTAL_ROWS || "1000");
const batchSize = Number(process.env.BF_BATCH_SIZE || "100");
const killMs = Number(process.env.BF_KILL_MS || "0");
const sleepMs = Number(process.env.BF_SLEEP_MS || "10");
const failBatch = Number(process.env.BF_FAIL_BATCH || "-1");

const pool = new Pool({ user: "postgres", database: dbName, host: "/run/postgresql", max: 3 });

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
    try {
        const client = await pool.connect();
    try {
        await client.query("BEGIN");
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${table} (
                id BIGSERIAL PRIMARY KEY,
                payload TEXT,
                full_name TEXT,
                version INT DEFAULT 0
            );
        `);
        const { rows: cnt } = await client.query(`SELECT count(*) AS n FROM ${table}`);
        if (Number(cnt[0].n) === 0) {
            await client.query(
                `INSERT INTO ${table} (payload, full_name) SELECT 'p_' || g, NULL FROM generate_series(1, $1) g`,
                [totalRows],
            );
        }
        await client.query("COMMIT");
    } catch {
        await client.query("ROLLBACK").catch(() => {});
    } finally {
        client.release();
    }

    const migrator = createMigrator(pool, { table: "_writer_migrations" });

    let batchNo = 0;
    const result = await migrator.backfill({
        id,
        migrationId: "bf_test",
        table,
        batchSize,
        cursor: { type: "serial", columns: ["id"] },
        processBatch: async ({ client: bc, rows }) => {
            batchNo += 1;
            if (sleepMs > 0) {
                await new Promise((r) => setTimeout(r, sleepMs));
            }
            if (failBatch > 0 && batchNo === failBatch) {
                throw new Error(`injected batch failure #${batchNo}`);
            }
            if (rows.length > 0) {
                const ids = rows.map((r) => r.id);
                await bc.query(
                    `UPDATE ${table} SET full_name = payload || '_fn', version = version + 1 WHERE id = ANY($1) AND full_name IS NULL`,
                    [ids],
                );
            }
            return { processed: rows.length };
        },
        maxRetries: 1,
        delayMs: 0,
    });

    console.log(JSON.stringify({ result, batchNo, table, id }));
    } finally {
        await pool.end();
    }
}

main().catch((e) => {
    console.error(String(e ?? e));
    process.exit(1);
});
