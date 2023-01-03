import moment from "moment";
import pkg from "pg";
const { Client, Pool } = pkg;
import logger from "./logger.js";

async function save_integration(
  db,
  module,
  duration,
  insertQty,
  updateQty,
  deleteQty
) {
  try {
    const insertStatement =
      "INSERT INTO public.integrations(date, module, insert_quantity, delete_quantity, update_quantity, process_duration) VALUES ($1, $2, $3, $4, $5, $6);";
    const values = [
      moment().format('YYYY-MM-DD HH:mm:ss'),
      module,
      insertQty,
      deleteQty,
      updateQty,
      duration
    ];
    const insertResult = await db.query(insertStatement, values);

    if (insertResult.rowCount == 0) {
      logger.error("error in insert", lead);
    }
  } catch (error) {
    logger.error("Error in save_integration", error);
  }
}

export default save_integration;
