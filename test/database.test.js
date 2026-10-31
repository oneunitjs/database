import { describe, it, expect } from "vitest";

import { createDatabase } from "../src/index.js";
import { readFileSync } from "node:fs";

describe("createDatabase", () => {
    it("exposes the original and extended surface", async () => {
        const db = createDatabase({
            host: "127.0.0.1",
            port: 5432,
            database: "unused",
            user: "unused",
            password: "unused",
            max: 1,
        });

        expect(typeof db.query).toBe("function");
        expect(typeof db.getClient).toBe("function");
        expect(typeof db.releaseClient).toBe("function");
        expect(typeof db.transaction).toBe("function");
        expect(typeof db.savepoint).toBe("function");
        expect(typeof db.stream).toBe("function");
        expect(typeof db.cursor).toBe("function");
        expect(typeof db.check).toBe("function");
        expect(typeof db.shutdown).toBe("function");
        expect(typeof db.schema.create.table).toBe("function");

        expect(typeof db.queryOne).toBe("function");
        expect(typeof db.prepare).toBe("function");
        expect(typeof db.health).toBe("function");
        expect(typeof db.batch.insertMany).toBe("function");
        expect(typeof db.from).toBe("function");
        expect(typeof db.model).toBe("function");
        expect(typeof db.migrate.run).toBe("function");
        expect(db.metrics).toBeTruthy();

        await db.shutdown();
    });

    it("package.json has no workspace runtime dependencies", () => {
        const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
        const deps = { ...pkg.dependencies, ...pkg.peerDependencies, ...pkg.optionalDependencies };

        for (const [name, version] of Object.entries(deps)) {
            expect(String(version).includes("workspace:")).toBe(false);
        }
    });
});
