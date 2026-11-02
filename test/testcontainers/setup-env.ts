import fs from "node:fs";
import path from "node:path";

const STATE_FILE = path.join(process.cwd(), ".testcontainers.json");

if (fs.existsSync(STATE_FILE)) {
    const state = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    const p = state.primary;

    process.env.PG_HOST = p.host;
    process.env.PG_PORT = String(p.port);
    process.env.PG_USER = p.user;
    process.env.PG_PASSWORD = p.password;

    process.env.PG_TEST_HOST = p.host;
    process.env.PG_TEST_USER = p.user;
    process.env.PG_TEST_PASSWORD = p.password;

    process.env.DATABASE_URL = p.uri;
    process.env.SHARD_0_PORT = String(state.shards.shard0.port);
    process.env.SHARD_1_PORT = String(state.shards.shard1.port);
    process.env.SHARD_2_PORT = String(state.shards.shard2.port);
    process.env.COORDINATION_PORT = String(state.coordination.port);
} else {
    console.warn("[testcontainers] state file not found; integration tests may skip");
}
