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
import lodash from "lodash";

dotenv.config();

const ZOHO_ROLES_URL = "https://www.zohoapis.com/crm/v3/settings/roles";

const fieldList = [
  { label: "id", value: "id", name: "id" },
  { label: "name", value: "name", name: "name" },
];

async function synchronize_role(db) {
  const startInMilis = Date.now();
  logger.info("===============synchronize_role start===============");
  try {
    const zohoResultList = await get_roles(db);
    const zohoRoleList = normalize_zoho_list(zohoResultList);
    const dbRoleList = await get_role_db(db);

    logger.info("zohoRoleList.length", zohoRoleList.length);
    logger.info("dbRoleList.length", dbRoleList.length);
    const { insertList, updateList, deleteList } = compare_lists(
      zohoRoleList,
      dbRoleList
    );

    logger.info("insertList.length", parseFloat(insertList.length));
    if (insertList.length) {
      await insert_model_db(db, insertList, "roles");
    }

    logger.info("updateList.length", parseFloat(updateList.length));
    if (updateList.length) {
      await update_model_db(db, updateList, "roles");
    }

    logger.info("deleteList.length", parseFloat(deleteList.length));
    if (deleteList.length) {
      if (deleteList.length > 100) {
        throw new Error("Delete qty is too much");
      }
      await delete_model_db(db, deleteList, "roles");
    }

    const durationInMilis = Date.now() - startInMilis;

    logger.info(
      "===============synchronize_role end in",
      durationInMilis,
      "ms",
      "==============="
    );

    save_integration(
      db,
      "roles",
      durationInMilis,
      insertList.length,
      updateList.length,
      deleteList.length
    );
  } catch (e) {
    logger.error("Error in synchronize_role", e);
    throw e;
  }
}

async function get_roles(db) {
  const startInMilis = Date.now();
  logger.info("get_roles start");
  const maxQuantity = 10;
  const roleList = [];
  try {
    let creds = await get_creds(db);
    if (creds) {
      let continueLoop = true;
      let loopQuantity = 0;
      let page = 1;
      while (continueLoop && loopQuantity < maxQuantity) {
        const startInMilisFetch = Date.now();

        try {
          const response = await get_role_response({ creds, page });
          roleList.push(...response.data.roles);
          //const info = response.data.info;
          continueLoop = false;
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

        logger.info("fetch_roles", Date.now() - startInMilisFetch, "ms");
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
  logger.info("get_roles end in", Date.now() - startInMilis, "ms");
  return roleList;
}

async function get_role_response({ creds, page_token, page }) {
  const edp = get_role_endpoint({ page_token, page });
  logger.info("edp", edp);
  return await axios.get(edp, {
    headers: {
      "Content-Type": "application/json",
      Authorization: "Zoho-oauthtoken " + creds.access_token,
    },
  });
}

function get_role_endpoint({ page_token, page }) {
  const query = {};
  if (page_token) query.page_token = page_token;
  if (page) query.page = page;

  return ZOHO_ROLES_URL;
}

async function get_role_db(db) {
  try {
    const res = await db.query(get_role_db_query());
    if (res.rows && res.rows.length > 0) return res.rows;
  } catch (error) {
    throw error;
  }
  return [];
}

function get_role_db_query() {
  return `SELECT 
    id, 
    name
  FROM public.roles;`;
}

function normalize_zoho_list(roleList) {
  return roleList.map((item) => {
    const newItem = {};
    fieldList.forEach((field) => {
      if (lodash.isObject(item[field.value])) {
        if (!lodash.isArray(item[field.value])) {
          newItem[field.name] = item[field.value].id;
        } else {
          if (lodash.isEmpty(item[field.value])) {
            newItem[field.name] = null;
          } else {
            newItem[field.name] = JSON.stringify(item[field.value]);
          }
        }
      } else {
        newItem[field.name] = item[field.value];
      }
    });
    return newItem;
  });
}

export { synchronize_role };
