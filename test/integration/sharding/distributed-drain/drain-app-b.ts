import { ShardManager } from "../../../../src/sharding/shard-manager.ts";
import { writeFileSync } from "node:fs";

const resultFile = process.argv[2];

const shardConfigs = [
    {
        id: "shard-0",
        connection: {
            host: "127.0.0.1",
            port: 5440,
            user: "postgres",
            database: "shard0",
        },
    },
    {
        id: "shard-1",
        connection: {
            host: "127.0.0.1",
            port: 5441,
            user: "postgres",
            database: "shard1",
        },
    },
];

const coordination = {
    host: "127.0.0.1",
    port: 5443,
    user: "postgres",
    database: "coordination",
};

const result: {
    app: string;
    drainStarted: boolean;
    drainCompleted: boolean;
    drainTimeout: boolean;
    activeOpsWhenDrainStarted: number | null;
    error: string | null;
} = {
    app: "B",
    drainStarted: false,
    drainCompleted: false,
    drainTimeout: false,
    activeOpsWhenDrainStarted: null,
    error: null,
};

function writeResult() {
    writeFileSync(resultFile, JSON.stringify(result, null, 2));
}

async function main() {
    const manager = new ShardManager(shardConfigs, undefined, undefined, undefined, {
        instanceId: "app-b",
        coordinationMode: "strict",
        abandonedOperationTimeoutMs: 5000,
        coordination,
    });

    try {
        await new Promise((resolve) => setTimeout(resolve, 1500));
        result.activeOpsWhenDrainStarted = await manager.countActiveOpsInDb("shard-0");
        const drainStart = Date.now();
        try {
            await manager.removeShard("shard-0");
            result.drainStarted = true;
            result.drainCompleted = true;
        } catch (error) {
            result.drainStarted = true;
            result.drainCompleted = false;
            result.error = error instanceof Error ? error.message : String(error);
            if (result.error.includes("still in flight") || Date.now() - drainStart >= 4000) {
                result.drainTimeout = true;
            }
        }
    } catch (error) {
        result.error = error instanceof Error ? error.message : String(error);
    } finally {
        await manager.shutdown();
        writeResult();
    }
}

main().catch((error) => {
    result.error = error instanceof Error ? error.message : String(error);
    writeResult();
    process.exit(1);
});
