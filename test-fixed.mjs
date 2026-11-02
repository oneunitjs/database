import { PostgreSqlContainer } from "@testcontainers/postgresql";
const c = await new PostgreSqlContainer("postgres:16")
  .withDatabase("postgres")
  .withUsername("postgres")
  .withPassword("postgres")
  .withExposedPorts({ container: 5432, host: 5499 })
  .withCommand(["postgres", "-c", "max_prepared_transactions=100"])
  .start();
console.log("mapped", c.getMappedPort(5432));
console.log("getPort", c.getPort());
// test connection to fixed host port
import { Pool } from "pg";
const p = new Pool({host:"127.0.0.1", port:5499, user:"postgres", password:"postgres", database:"postgres"});
console.log(await p.query("SELECT 1"));
await p.end();
await c.stop();
console.log("OK");
