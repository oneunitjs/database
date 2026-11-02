import { Pool } from "pg";

const args = Object.fromEntries(
    process.argv.slice(2).map((a) => {
        const i = a.indexOf("=");
        return i === -1 ? [a, ""] : [a.slice(0, i), a.slice(i + 1)];
    }),
);

const dbName = args.db;
const table = args.table;
const id = args.id;

const pool = new Pool({ user: "postgres", database: dbName, host: "/run/postgresql", max: 3 });

async function seed(): Promise<void> {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        await client.query(`
            CREATE TABLE IF NOT EXISTS ${table} (
                id BIGSERIAL PRIMARY KEY,
                payload TEXT,
                full_name TEXT
            );
        `);
        const { rows: cnt } = await client.query(`SELECT count(*) AS n FROM ${table}`);
        if (Number(cnt[0].n) === 0) {
            await client.query(
                `INSERT INTO ${table} (payload, full_name) SELECT 'p_' || g, NULL FROM generate_series(1, 5000) g`,
            );
        }
        await client.query("COMMIT");
    } catch {
        await client.query("ROLLBACK").catch(() => {});
    } finally {
        client.release();
    }
}

async function main(): Promise<void> {
    await seed();

    const migrator = (await import("../../src/index.js")).createMigrator(pool, {
        table: "_writer_migrations",
    });

    // Non-transactional migration with multiple autocommit statements.
    // Runs to completion so the test is deterministic; the interruption path
    // is covered by the manually-seeded journal test below.
    await migrator.run({
        migrations: [
            {
                id,
                mode: "non-transactional",
                up: `UPDATE ${table} SET full_name = payload || '_a' WHERE full_name IS NULL;
                     UPDATE ${table} SET full_name = payload || '_b' WHERE full_name LIKE '%_a';`,
            },
        ],
    });

    console.log(JSON.stringify({ table, id }));
}

main()
    .then(async () => {
        await pool.end().catch(() => {});
    })
    .catch(async (e) => {
        console.error(String(e ?? e));
        await pool.end().catch(() => {});
        process.exit(1);
    });
