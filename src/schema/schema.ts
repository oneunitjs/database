import type { Pool } from "pg";
import { createSchema } from "./create.js";
import { alterSchema } from "./alter.js";
import { dropSchema } from "./drop.js";
import { constrainSchema } from "./constrain.js";
import { inspectSchema, diffSchema } from "./introspect.js";
import type { SchemaManager, ExtendedTableSchema, SchemaDiffResult } from "../types.js";

export function createSchemaManager(pool: Pool): SchemaManager {
    return {
        create: createSchema(pool),
        alter: alterSchema(pool),
        drop: dropSchema(pool),
        constrain: constrainSchema(pool),
        inspect: {
            schema: (options?: { schemas?: string[] }) => inspectSchema(pool, options),
        },
        diff: {
            schema: (
                current: ExtendedTableSchema[],
                desired: Record<string, Record<string, string>>,
            ): SchemaDiffResult => diffSchema(current, desired),
        },
    };
}

export { inspectSchema, diffSchema };
export type { ExtendedTableSchema, SchemaDiffResult };
