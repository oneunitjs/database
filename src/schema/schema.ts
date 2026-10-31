import type { Pool } from "pg";
import { createSchema } from "./create.js";
import { alterSchema } from "./alter.js";
import { dropSchema } from "./drop.js";
import { constrainSchema } from "./constrain.js";
import type { SchemaManager } from "../types.js";

export function createSchemaManager(pool: Pool): SchemaManager {
    return {
        create: createSchema(pool),
        alter: alterSchema(pool),
        drop: dropSchema(pool),
        constrain: constrainSchema(pool),
    };
}