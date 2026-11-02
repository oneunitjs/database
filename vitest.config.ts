import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        include: ["test/**/*.{test,spec}.{js,ts}"],
        testTimeout: 15_000,
        hookTimeout: 15_000,
        fileParallelism: false,
    },
});
