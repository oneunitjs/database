import { describe, it, expect } from "vitest";

import { createMigrator } from "../../src/index.js";

function makeClient() {
    const state = { applied: [], executed: [] };

            const client = {
        released: false,
        async query(text, values = []) {
            const sqlText = String(text).trim();
            const upper = sqlText.toUpperCase();

            if (sqlText.startsWith("SELECT pg_try_advisory_lock")) {
                return { rows: [{ pg_try_advisory_lock: true }] };
            }
            if (sqlText.startsWith("SELECT pg_advisory_lock") || sqlText.startsWith("SELECT pg_advisory_unlock")) {
                return {};
            }
            if (sqlText.includes("CREATE TABLE IF NOT EXISTS")) {
                return {};
            }
            if (sqlText.includes("CREATE INDEX IF NOT EXISTS")) {
                return {};
            }
            if (upper.includes("SELECT") && sqlText.includes("_writer_migrations")) {
                return {
                    rows: state.applied.map((row) => ({
                        id: row.id,
                        name: row.name,
                        status: row.status ?? "applied",
                        checksum: row.checksum,
                        execution_ms: row.execution_ms,
                        applied_at: row.applied_at,
                        failed_at: row.failed_at,
                        error_message: row.error_message,
                        attempts: row.attempts,
                    })),
                };
            }
            if (sqlText.includes("INSERT INTO") && sqlText.includes("_writer_migrations")) {
                state.applied.push({
                    id: values[0],
                    name: values[1],
                    checksum: values[2],
                    status: "applied",
                    applied_at: new Date().toISOString(),
                    attempts: 1,
                });
                return {};
            }
            if (sqlText.includes("DELETE FROM") && sqlText.includes("_writer_migrations")) {
                state.applied = state.applied.filter((row) => row.id !== values[0]);
                return {};
            }
            if (sqlText.includes("UPDATE") && sqlText.includes("_writer_migrations")) {
                const idMatch = sqlText.match(/WHERE id = \$(\d+)/);
                const targetId = idMatch ? values[idMatch[1] - 1] : null;
                const row = state.applied.find((r) => r.id === targetId);
                if (row) {
                    row.status = values[0]; // status is first param
                    if (values[1] !== null && values[1] !== undefined) {
                        row.checksum = values[1];
                    }
                }
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
        expect(state.executed.length).toBeGreaterThanOrEqual(2);
        expect(client.released).toBe(true);
    });

    it("is idempotent for already applied migrations", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });
        const second = await migrator.run({ migrations });

        expect(second).toEqual([]);
        expect(state.applied.length).toBe(2);
        expect(state.executed.length).toBeGreaterThanOrEqual(2);
    });

    it("reports status", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });
        const status = await migrator.status({ migrations });

        expect(status).toEqual([
            {
                id: "001_create_users",
                name: "create users",
                status: "applied",
                applied: true,
                applied_at: status[0].applied_at,
                attempts: 1,
                checksum: status[0].checksum,
            },
            {
                id: "002_add_email",
                name: "add email index",
                status: "applied",
                applied: true,
                applied_at: status[1].applied_at,
                attempts: 1,
                checksum: status[1].checksum,
            },
        ]);
        expect(status[0].applied_at).toBeTruthy();
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

    it("progress() reports migration execution status", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });

        const result = await migrator.progress({ migrations });

        expect(result.total).toBe(2);
        expect(result.completed).toBe(2);
        expect(result.failed).toBe(0);
        expect(result.skipped).toBe(0);
        expect(result.events).toHaveLength(2);
        expect(result.events[0].status).toBe("completed");
        expect(result.events[1].status).toBe("completed");
        expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it("progress() reports pending migrations when none applied", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        const result = await migrator.progress({ migrations });

        expect(result.total).toBe(2);
        expect(result.completed).toBe(0);
        expect(result.skipped).toBe(2);
        expect(result.events.every((e) => e.status === "pending")).toBe(true);
    });

    it("progress() invokes onProgress callback for each migration", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        const events = [];
        await migrator.progress({ migrations, onProgress: (e) => events.push(e) });

        expect(events).toHaveLength(2);
        expect(events[0].migrationId).toBe("001_create_users");
        expect(events[1].migrationId).toBe("002_add_email");
    });

    it("discoverDependencies() returns dependency graph with roots and leaves", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        const result = await migrator.discoverDependencies({ migrations });

        expect(result.migrations).toHaveLength(2);
        expect(result.roots).toContain("001_create_users");
        expect(result.dependencyGraph["002_add_email"]).toEqual([]);
        expect(result.cycles).toEqual([]);
        expect(result.ordered).toEqual(["001_create_users", "002_add_email"]);
    });

    it("discoverDependencies() detects dependency cycles", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        const cyclicMigrations = [
            { id: "cyc-a", dependsOn: ["cyc-b"], up: "SELECT 1" },
            { id: "cyc-b", dependsOn: ["cyc-a"], up: "SELECT 1" },
        ];

        const result = await migrator.discoverDependencies({ migrations: cyclicMigrations });

        expect(result.cycles.length).toBeGreaterThan(0);
        expect(result.ordered).toEqual([]);
    });

    it("discoverDependencies() identifies leaves", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        const result = await migrator.discoverDependencies({ migrations });

        expect(result.leaves).toContain("002_add_email");
    });

    it("repairMigration() marks a pending migration as applied", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });

        const result = await migrator.repairMigration({
            migrationId: "001_create_users",
            action: "mark-applied",
            migrations,
        });

        expect(result.migrationId).toBe("001_create_users");
        expect(result.previousStatus).toBe("applied");
        expect(result.newStatus).toBe("applied");
        expect(result.action).toBe("mark-applied");
    });

    it("repairMigration() marks a migration as failed", async () => {
        const { client, state } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await migrator.run({ migrations });

        const result = await migrator.repairMigration({
            migrationId: "001_create_users",
            action: "mark-failed",
            migrations,
        });

        expect(result.newStatus).toBe("failed");
        expect(result.action).toBe("mark-failed");
    });

    it("repairMigration() retries a failed migration", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        // Manually insert a failed migration
        await client.query(
            `INSERT INTO _writer_migrations (id, name, status, checksum, applied_at, attempts)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            ["001_create_users", "create users", "failed", "abc123", new Date().toISOString(), 1],
        );

        const result = await migrator.repairMigration({
            migrationId: "001_create_users",
            action: "retry",
            migrations,
        });

        expect(result.newStatus).toBe("pending");
        expect(result.action).toBe("retry");
    });

    it("repairMigration() throws when migration not found", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await expect(
            migrator.repairMigration({
                migrationId: "nonexistent",
                action: "mark-applied",
                migrations,
            }),
        ).rejects.toThrow(/not found/);
    });

    it("repairMigration() throws on invalid action", async () => {
        const { client } = makeClient();
        const migrator = createMigrator({ connect: async () => client });

        await expect(
            migrator.repairMigration({
                migrationId: "001_create_users",
                action: "invalid-action",
                migrations,
            }),
        ).rejects.toThrow(/Invalid action/);
    });
});
