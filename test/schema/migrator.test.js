import { describe, it, expect } from "vitest";

import { createMigrator } from "../../src/index.js";

function makeClient() {
    const state = { applied: [], executed: [] };

    const client = {
        released: false,
        async query(text, values = []) {
            const sqlText = String(text).trim();

            if (sqlText.startsWith("SELECT pg_advisory_lock") || sqlText.startsWith("SELECT pg_advisory_unlock")) {
                return {};
            }
            if (sqlText.includes("CREATE TABLE IF NOT EXISTS")) {
                return {};
            }
            if (sqlText.includes("SELECT id, name, checksum, applied_at")) {
                return { rows: state.applied.map((row) => ({ ...row })) };
            }
            if (sqlText.includes("INSERT INTO") && sqlText.includes("_writer_migrations")) {
                state.applied.push({
                    id: values[0],
                    name: values[1],
                    checksum: values[2],
                    applied_at: new Date().toISOString(),
                });
                return {};
            }
            if (sqlText.includes("DELETE FROM") && sqlText.includes("_writer_migrations")) {
                state.applied = state.applied.filter((row) => row.id !== values[0]);
                return {};
            }
            if (["BEGIN", "COMMIT", "ROLLBACK"].includes(sqlText)) {
                return {};
            }

            state.executed.push(sqlText);
            return {};
        },
        release() {
            client.released = true;
        },
    };

    return { client, state };
}

const migrations = [
    {
        id: "001_create_users",
        name: "create users",
        up: "CREATE TABLE users (id serial primary key)",
        down: "DROP TABLE users",
    },
    {
        id: "002_add_email",
        name: "add email index",
        up: async (ctx) => {
            await ctx.query("CREATE INDEX idx_users_email ON users (email)");
        },
        down: async (ctx) => {
            await ctx.query("DROP INDEX idx_users_email");
        },
    },
];

describe("migrator", () => {
    it("applies pending migrations and records them", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        const applied = await migrator.run({ migrations });

        expect(applied).toEqual(["001_create_users", "002_add_email"]);
        expect(state.applied.length).toBe(2);
        expect(state.executed.length).toBe(2);
        expect(client.released).toBe(true);
    });

    it("is idempotent for already applied migrations", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });
        const second = await migrator.run({ migrations });

        expect(second).toEqual([]);
        expect(state.applied.length).toBe(2);
        expect(state.executed.length).toBe(2);
    });

    it("reports status", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });
        const status = await migrator.status({ migrations });

        expect(status).toEqual([
            { id: "001_create_users", name: "create users", applied: true, appliedAt: status[0].appliedAt },
            { id: "002_add_email", name: "add email index", applied: true, appliedAt: status[1].appliedAt },
        ]);
        expect(status[0].appliedAt).toBeTruthy();
    });

    it("rolls back the most recent migration", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });
        const rolledBack = await migrator.rollback({ migrations, steps: 1 });

        expect(rolledBack).toEqual(["002_add_email"]);
        expect(state.applied.length).toBe(1);
        expect(state.executed).toContain("DROP INDEX idx_users_email");
    });

    it("refuses to roll back a migration without down()", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations: [{ id: "001", up: "SELECT 1" }] });

        await expect(
            migrator.rollback({ migrations: [{ id: "001", up: "SELECT 1" }] }),
        ).rejects.toThrow(/no down\(\) migration provided/);
    });
});
