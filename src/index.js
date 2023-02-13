import pkg from "pg";
import { synchronize_leads } from "./leads/synchronize-lead.js";
import logger from "./logger.js";
import { synchronize_role } from "./roles/synchronize-role.js";
import { synchronize_user } from "./users/synchronize-user.js";
const { Client, Pool } = pkg;

integrate();

async function integrate() {
  const startInMilis = Date.now();
  logger.info("===============integrate start===============");
  const db = await get_pool().connect();
  try {
    await db.query("BEGIN");

    await synchronize_user(db);
    await synchronize_role(db);
    //await synchronize_leads(db);

    await db.query("COMMIT");

    const durationInMilis = Date.now() - startInMilis;
    logger.info(
      "===============integrate end in",
      durationInMilis,
      "ms",
      "==============="
    );
  } catch (e) {
    logger.error("Error in integrate", e);
    await db.query("ROLLBACK");
    throw e;
  } finally {
    db.release();
  }
}

function get_pool() {
  return new Pool({
    user: process.env.PGUSER,
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: process.env.PGPORT,
  });
}
