import { Pool } from "pg";
import { createMigrator } from "../../src/index.js";

const args = Object.fromEntries(
    process.argv.slice(2).map((a) => {
        const i = a.indexOf("=");
        return i === -1 ? [a, ""] : [a.slice(0, i), a.slice(i + 1)];
    }),
);

const dbName = args.db;
const id = args.id;
const sql = args.sql || "";
const sleepSec = Number(args["sleep-sec"] || "0");
const lockKey = Number(args["lock-key"] || "727274");
const mode = args.mode || "transactional";
const failOn = args.fail === "true";

const pool = new Pool({ user: "postgres", database: dbName, host: "/run/postgresql", max: 3 });

async function main(): Promise<void> {
    const migrator = createMigrator(pool, { table: "_writer_migrations", lockKey });
    const fullSql = sleepSec > 0 ? `${sql}; SELECT pg_sleep(${sleepSec})` : sql;
    const migrations = [{ id, up: fullSql, mode: mode as any }];
    try {
        const applied = await migrator.run({ migrations });
        if (failOn) {
            process.exit(1);
        }
        console.log(JSON.stringify({ applied, id, ok: true }));
    } catch (e) {
        console.log(JSON.stringify({ applied: [], id, ok: false, error: String(e ?? "") }));
        process.exit(1);
    } finally {
        await pool.end().catch(() => {});
    }
}

main().catch((e) => {
    console.error(String(e ?? e));
    process.exit(1);
});
