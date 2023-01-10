import axios from "axios";
import dotenv from "dotenv";
import logger from "../logger.js";
import querystring from "querystring";

dotenv.config();

const ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token";

async function refresh_token(db) {
  logger.info("refresh_token start");

  try {
    const query = "SELECT access_token, refresh_token FROM zoho_creds;";
    const update = "UPDATE public.zoho_creds SET access_token=$1";
    const res = await db.query(query);
    if (res) {
      if (res.rows && res.rows.length > 0) {
        const result = res.rows[0];

        logger.info("result", result);

        const response = await axios.post(
          ZOHO_TOKEN_URL,
          querystring.stringify({
            grant_type: "refresh_token",
            client_id: process.env.ZOHO_CLIENTE_ID,
            client_secret: process.env.ZOHO_CLIENT_SECRET,
            refresh_token: result.refresh_token,
          }),
          {
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
            },
          }
        );
        if (response && response.data) {
          const updateResult = await db.query(update, [
            response.data.access_token,
          ]);
          if (updateResult.rowCount != 1) {
            log.info("No actualizado");
          }
        }
      }
    }
  } catch (error) {
    logger.error("Entro al catch", error);
  }

  logger.info("refresh_token end");
}

async function get_creds(db) {
  const query = "SELECT access_token, refresh_token FROM zoho_creds;";
  const res = await db.query(query);
  return res && res.rows && res.rows.length > 0 ? res.rows[0] : null;
}

export { refresh_token, get_creds };
