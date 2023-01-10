import axios from "axios";
import pkg from "pg";
const { Client, Pool } = pkg;
import dotenv from "dotenv";
import querystring from "querystring";
import { refresh_token, get_creds } from "../api/api.js";
import logger from "../logger.js";
import {
  compare_lists,
  insert_model_db,
  update_model_db,
  delete_model_db,
} from "../util/util.js";
import save_integration from "../integration.js";

dotenv.config();

const ZOHO_USERS_URL = "https://www.zohoapis.com/crm/v3/users";

const fieldList = [
  { label: "id", value: "id", name: "id" },
  { label: "first_name", value: "first_name", name: "first_name" },
  { label: "last_name", value: "last_name", name: "last_name" },
  { label: "email", value: "email", name: "email" },
  { label: "full_name", value: "full_name", name: "full_name" },
  { label: "status", value: "status", name: "status" },
];

async function synchronize_user(db) {
  const startInMilis = Date.now();
  logger.info("===============synchronize_user start===============");
  try {
    const zohoResultList = await get_users(db);
    const zohoUserList = normalize_zoho_list(zohoResultList);
    const dbUserList = await get_user_db(db);

    logger.info("zohoUserList.length", zohoUserList.length);
    logger.info("dbUserList.length", dbUserList.length);
    const { insertList, updateList, deleteList } = compare_lists(
      zohoUserList,
      dbUserList
    );

    logger.info("insertList.length", parseFloat(insertList.length));
    if (insertList.length) {
      await insert_model_db(db, insertList, "users");
    }

    logger.info("updateList.length", parseFloat(updateList.length));
    if (updateList.length) {
      await update_model_db(db, updateList, "users");
    }

    logger.info("deleteList.length", parseFloat(deleteList.length));
    if (deleteList.length) {
      if (deleteList.length > 100) {
        throw new Error("Delete qty is too much");
      }
      await delete_model_db(db, deleteList);
    }

    const durationInMilis = Date.now() - startInMilis;

    logger.info(
      "===============synchronize_user end in",
      durationInMilis,
      "ms",
      "==============="
    );
    
    save_integration(
      db,
      "users",
      durationInMilis,
      insertList.length,
      updateList.length,
      deleteList.length
    );
  } catch (e) {
    logger.error("Error in synchronize_user", e);
    throw e;
  }
}

async function get_users(db) {
  const startInMilis = Date.now();
  logger.info("get_users start");
  const maxQuantity = 10;
  const userList = [];
  try {
    let creds = await get_creds(db);
    if (creds) {
      let continueLoop = true;
      let loopQuantity = 0;
      let page = 1;
      while (continueLoop && loopQuantity < maxQuantity) {
        const startInMilisFetch = Date.now();

        try {
          const response = await get_user_response({ creds, page });
          userList.push(...response.data.users);
          const info = response.data.info;
          continueLoop = info.more_records;
          page++;
          loopQuantity++;
        } catch (internalError) {
          if (internalError.response && internalError.response.data) {
            if (internalError.response.data.code === "INVALID_TOKEN") {
              await refresh_token(db);
              creds = await get_creds(db);
            } else {
              throw internalError;
            }
          } else {
            throw internalError;
          }
        }

        logger.info("fetch_users", Date.now() - startInMilisFetch, "ms");
      }
    }
  } catch (error) {
    if (error.response && error.response.data) {
      logger.error("error.response.data", error.response.data);
    } else {
      logger.error("error", error);
    }
    throw error;
  }
  logger.info("get_users end in", Date.now() - startInMilis, "ms");
  return userList;
}

async function get_user_response({ creds, page_token, page }) {
  const edp = get_user_endpoint({ page_token, page });
  logger.info("edp", edp);
  return await axios.get(edp, {
    headers: {
      "Content-Type": "application/json",
      Authorization: "Zoho-oauthtoken " + creds.access_token,
    },
  });
}

function get_user_endpoint({ page_token, page }) {
  const query = {
    type: "AllUsers",
  };

  if (page_token) query.page_token = page_token;
  if (page) query.page = page;

  return ZOHO_USERS_URL + "?" + querystring.stringify(query);
}

async function get_user_db(db) {
  try {
    const res = await db.query(get_user_db_query());
    if (res.rows && res.rows.length > 0) return res.rows;
  } catch (error) {
    throw error;
  }
  return [];
}

function get_user_db_query() {
  return `SELECT 
    id, 
    first_name, 
    last_name, 
    email, 
    full_name, 
    status
  FROM public.users;`;
}

function normalize_zoho_list(userList) {
  return userList.map((item) => {
    const newItem = {};
    fieldList.forEach((field) => {
      newItem[field.name] = item[field.value];
    });
    return newItem;
  });
}

export { synchronize_user };
