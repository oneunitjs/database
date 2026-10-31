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
    acquired: boolean;
    released: boolean;
    error: string | null;
    activeOpsAtEnd: number | null;
} = {
    app: "A",
    acquired: false,
    released: false,
    error: null,
    activeOpsAtEnd: null,
};

function writeResult() {
    writeFileSync(resultFile, JSON.stringify(result, null, 2));
}

function localOpIds(manager: ShardManager, shardId: string): string[] {
    const lifecycle = (manager as unknown as {
        lifecycle: Map<string, { operationIds: Set<string> }>;
    }).lifecycle;
    const state = lifecycle.get(`shard:${shardId}`);
    return state ? [...state.operationIds] : [];
}

async function main() {
    const manager = new ShardManager(shardConfigs, undefined, undefined, undefined, {
        instanceId: "app-a",
        coordinationMode: "strict",
        abandonedOperationTimeoutMs: 30000,
        coordination,
    });

    try {
        await manager.acquireShard("shard-0");
        result.acquired = true;
        result.activeOpsAtEnd = manager.getActiveOps("shard-0");
        const holdUntil = Date.now() + 10000;
        while (Date.now() < holdUntil) {
            for (const opId of localOpIds(manager, "shard-0")) {
                await manager.heartbeatOperation("shard-0", opId);
            }
            await new Promise((resolve) => setTimeout(resolve, 400));
        }
        await manager.releaseShard("shard-0");
        result.released = true;
        result.activeOpsAtEnd = manager.getActiveOps("shard-0");
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
