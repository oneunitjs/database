import { Pool } from "pg";

const args = Object.fromEntries(
    process.argv.slice(2).map((a) => {
        const i = a.indexOf("=");
        return i === -1 ? [a, ""] : [a.slice(0, i), a.slice(i + 1)];
    }),
);

const dbName = args.db;
const action = args.action;
const sql = args.sql || "";
const sleepSec = Number(args["sleep-sec"] || "5");
const lockKey = Number(args["lock-key"] || "727274");

const pool = new Pool({ user: "postgres", database: dbName, host: "/run/postgresql", max: 3 });

function sleep(sec) {
    return new Promise((resolve) => setTimeout(resolve, sec * 1000));
}

async function main() {
    if (action === "run-then-sleep") {
        const client = await pool.connect();
        try {
            await client.query("BEGIN");
            await client.query(sql);
            await client.query(`SELECT pg_sleep(${sleepSec})`);
            await client.query("COMMIT");
        } finally {
            try {
                await client.query("ROLLBACK").catch(() => {});
            } catch {
                // ignore
            }
            client.release();
        }
    } else if (action === "hold-lock") {
        const client = await pool.connect();
        try {
            await client.query(`SELECT pg_advisory_lock(${lockKey})`);
            await sleep(sleepSec);
            await client.query(`SELECT pg_advisory_unlock(${lockKey})`);
        } finally {
            client.release();
        }
    } else if (action === "run-migration") {
        const client = await pool.connect();
        try {
            await client.query("BEGIN");
            await client.query(sql);
            await client.query("COMMIT");
        } finally {
            try {
                await client.query("ROLLBACK").catch(() => {});
            } catch {
                // ignore
            }
            client.release();
        }
    } else if (action === "autocommit-then-sleep") {
        const client = await pool.connect();
        try {
            const first = sql.split(";").filter((s) => s.trim())[0];
            if (first) await client.query(first);
            await sleep(sleepSec);
        } finally {
            client.release();
        }
    } else if (action === "autocommit-sequential") {
        // Execute each top-level statement in autocommit (no BEGIN/COMMIT),
        // mimicking non-transactional migration execution. Statements are split
        // on ';' so a pg_sleep can sit between two DDL statements.
        const client = await pool.connect();
        try {
            const statements = sql.split(";").map((s) => s.trim()).filter((s) => s.length > 0);
            for (const stmt of statements) {
                await client.query(stmt);
            }
        } finally {
            client.release();
        }
    }

    await pool.end();
}

main().catch((e) => {
    console.error(String(e ?? e));
    process.exit(1);
});
