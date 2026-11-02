import fs from "node:fs";
import path from "node:path";
import { PostgreSqlContainer } from "@testcontainers/postgresql";

const STATE_FILE = path.join(process.cwd(), ".testcontainers.json");
const IMAGE = "postgres:16";

interface Endpoint {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
    uri: string;
    containerId: string;
}

async function startInstance(database: string, hostPort?: number): Promise<{ endpoint: Endpoint; container: any }> {
    let builder: any = new (PostgreSqlContainer as any)(IMAGE)
        .withDatabase(database)
        .withUsername("postgres")
        .withPassword("postgres");
    if (hostPort) {
        builder = builder.withExposedPorts({ container: 5432, host: hostPort });
    }
    const container = await builder
        .withCommand(["postgres", "-c", "max_prepared_transactions=100", "-c", "max_connections=100"])
        .start();

    const endpoint: Endpoint = {
        host: container.getHost(),
        port: container.getPort(),
        user: container.getUsername(),
        password: container.getPassword(),
        database: container.getDatabase(),
        uri: container.getConnectionUri(),
        containerId: container.getId(),
    };

    return { endpoint, container };
}

export default async function globalSetup(): Promise<() => Promise<void>> {
    const containers: any[] = [];

    try {
        console.log("[testcontainers] starting PostgreSQL instances...");
        const primary = await startInstance("postgres");
        containers.push(primary.container);

        const shard0 = await startInstance("shard0", 5440);
        containers.push(shard0.container);
        const shard1 = await startInstance("shard1", 5441);
        containers.push(shard1.container);
        const shard2 = await startInstance("shard2", 5442);
        containers.push(shard2.container);
        const coordination = await startInstance("coordination", 5443);
        containers.push(coordination.container);

        const state = {
            primary: primary.endpoint,
            shards: {
                shard0: shard0.endpoint,
                shard1: shard1.endpoint,
                shard2: shard2.endpoint,
            },
            coordination: coordination.endpoint,
        };

        fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
        console.log("[testcontainers] PostgreSQL instances ready");

        return async () => {
            for (const container of containers) {
                try {
                    await container.stop();
                } catch {
                    // best effort
                }
            }
            try {
                fs.rmSync(STATE_FILE, { force: true });
            } catch {
                // best effort
            }
        };
    } catch (error) {
        for (const container of containers) {
            try {
                await container.stop();
            } catch {
                // best effort
            }
        }
        try {
            fs.rmSync(STATE_FILE, { force: true });
        } catch {
            // best effort
        }
        throw error;
    }
}
