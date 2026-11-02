import { defineConfig, Plugin } from "vitest/config";
import path from "path";
import fs from "fs";

function jsToTsPlugin(): Plugin {
    return {
        name: "js-to-ts-resolver",
        async resolveId(source: string, importer: string | undefined, options: any) {
            if (!source.endsWith(".js") || !importer) return null;
            const tsSource = source.replace(/\.js$/, ".ts");
            const resolved = await this.resolve(tsSource, importer, {
                skipSelf: true,
                ...options,
            });
            if (resolved) return resolved;
            return null;
        },
    };
}

export default defineConfig({
    test: {
        include: ["test/**/*.{test,spec}.{js,ts}"],
        testTimeout: 120_000,
        hookTimeout: 120_000,
        fileParallelism: false,
        globalSetup: ["test/testcontainers/global-setup.ts"],
        setupFiles: ["test/testcontainers/setup-env.ts"],
    },
    resolve: {
        extensions: [".ts", ".js", ".mjs", ".json"],
    },
    plugins: [jsToTsPlugin()],
});
