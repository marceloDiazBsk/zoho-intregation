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
import moment from "moment";

dotenv.config();

const ZOHO_LEADS_URL = "https://www.zohoapis.com/crm/v3/Leads";

const fieldList = [
  { label: "Id", value: "id", name: "id" },
  { label: "Apellidos", value: "Last_Name", name: "last_name" },
  { label: "Empresa", value: "Company", name: "company" },
  { label: "Propietario de Leads", value: "Owner", name: "owner" },
  { label: "Correo electrónico", value: "Email", name: "email" },
  { label: "Móvil", value: "Mobile", name: "mobile" },
  { label: "Fuente de Leads", value: "Lead_Source", name: "lead_source" },
  { label: "Estado de Leads", value: "Lead_Status", name: "lead_status" },
  { label: "Hora de creación", value: "Created_Time", name: "created_at" },
  { label: "Is Converted?", value: "$converted", name: "converted" },
  { label: "Nombre", value: "First_Name", name: "first_name" },
  { label: "Teléfono", value: "Phone", name: "phone" },
  { label: "Creado por", value: "Created_By", name: "created_by" },
  { label: "Modificado por", value: "Modified_By", name: "modified_by" },
  {
    label: "Hora de modificación",
    value: "Modified_Time",
    name: "modified_at",
  },
  { label: "Calle", value: "Street", name: "street" },
  { label: "Ciudad", value: "City", name: "city" },
  { label: "Estado", value: "State", name: "state" },
  { label: "Código postal", value: "Zip_Code", name: "zip_code" },
  { label: "País", value: "Country", name: "country" },
  {
    label: "Hora de la última actividad",
    value: "Last_Activity_Time",
    name: "last_activity_at",
  },
  { label: "C.I. RUC.", value: "C_I_RUC", name: "fiscal_number" },
  {
    label: "Fecha de Nacimiento",
    value: "Fecha_de_Nacimiento",
    name: "birthday",
  },
  { label: "Interesado en", value: "Interesado_en", name: "interested_in" },
  {
    label: "Tipo de Contacto",
    value: "Tipo_de_Contacto",
    name: "contact_type",
  },
  { label: "Campaña Digital", value: "Campa_a_Digital", name: "campaign" },
  { label: "Profesión", value: "Profesi_n", name: "occupation" },
  { label: "Genero", value: "Genero", name: "gender" },
  { label: "Tipo de Cliente", value: "Tipo_de_Cliente", name: "customer_type" },
];

async function synchronize_leads(db) {
  const startInMilis = Date.now();
  logger.info("===============synchronize_leads start===============");
  try {
    const zohoResultList = await get_leads(db);
    const zohoLeadList = normalize_zoho_list(zohoResultList);
    const dbResultList = await get_lead_db(db);
    const dbLeadList = normalize_db_List(dbResultList);

    logger.info("zohoLeadList.length", zohoLeadList.length);
    logger.info("dbLeadList.length", dbLeadList.length);

    const { insertList, updateList, deleteList } = compare_lists(
      zohoLeadList,
      dbLeadList
    );

    logger.info("insertList.length", parseFloat(insertList.length));
    if (insertList.length) {
      await insert_model_db(db, insertList, "leads");
    }

    logger.info("updateList.length", parseFloat(updateList.length));
    if (updateList.length) {
      await update_model_db(db, updateList, "leads");
    }

    logger.info("deleteList.length", parseFloat(deleteList.length));
    if (deleteList.length) {
      if (deleteList.length > 100) {
        throw new Error("Delete qty is too much");
      }
      await delete_model_db(db, deleteList, "leads");
    }

    const durationInMilis = Date.now() - startInMilis;

    logger.info(
      "===============synchronize_leads end in",
      durationInMilis,
      "ms",
      "==============="
    );

    save_integration(
      db,
      "leads",
      durationInMilis,
      insertList.length,
      updateList.length,
      deleteList.length
    );
  } catch (e) {
    logger.error("Error in synchronize_leads", e);
    throw e;
  }
}

function normalize_zoho_list(leadList) {
  return leadList.map((item) => {
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

    newItem.created_at = newItem.created_at
      ? moment(newItem.created_at).format("YYYY-MM-DD HH:mm:ss")
      : null;
    newItem.modified_at = newItem.modified_at
      ? moment(newItem.modified_at).format("YYYY-MM-DD HH:mm:ss")
      : null;
    newItem.last_activity_at = newItem.last_activity_at
      ? moment(newItem.last_activity_at).format("YYYY-MM-DD HH:mm:ss")
      : null;

    return newItem;
  });
}

function normalize_db_List(leadList) {
  return leadList.map((item) => {
    item.created_at = item.created_at
      ? moment(item.created_at).format("YYYY-MM-DD HH:mm:ss")
      : null;
    item.modified_at = item.modified_at
      ? moment(item.modified_at).format("YYYY-MM-DD HH:mm:ss")
      : null;
    item.last_activity_at = item.last_activity_at
      ? moment(item.last_activity_at).format("YYYY-MM-DD HH:mm:ss")
      : null;
    return item;
  });
}

async function get_leads(db) {
  const startInMilis = Date.now();
  logger.info("get_leads start");
  const maxQuantity = 500;
  const leadList = [];
  try {
    let creds = await get_creds(db);
    if (creds) {
      let continueLoop = true;
      let loopQuantity = 0;
      let page_token = null;
      while (continueLoop && loopQuantity < maxQuantity) {
        const startInMilisFetch = Date.now();

        try {
          const response = await get_lead_response({ creds, page_token });
          leadList.push(...response.data.data);
          const info = response.data.info;
          continueLoop = info.more_records;
          page_token = info.next_page_token;
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

        logger.info("fetch_leads", Date.now() - startInMilisFetch, "ms");
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
  logger.info("get_leads end in", Date.now() - startInMilis, "ms");
  return leadList;
}

async function get_lead_response({ creds, page_token, page }) {
  const edp = get_lead_endpoint({ page_token, page });
  return await axios.get(edp, {
    headers: {
      "Content-Type": "application/json",
      Authorization: "Zoho-oauthtoken " + creds.access_token,
    },
  });
}

function get_lead_endpoint({ page_token, page }) {
  const query = {
    fields: fieldList.map((field) => field.value).join(","),
    sort_order: "desc",
    sort_by: "id",
    per_page: 200,
  };

  if (page_token) query.page_token = page_token;
  if (page) query.page = page;

  return ZOHO_LEADS_URL + "?" + querystring.stringify(query);
}

async function get_lead_db(db) {
  try {
    const res = await db.query(get_lead_db_query());
    if (res.rows && res.rows.length > 0) return res.rows;
  } catch (error) {
    throw error;
  }
  return [];
}

function get_lead_db_query() {
  return `SELECT 
    id,
    last_name,
    company,
    owner,
    email,
    mobile,
    lead_source,
    lead_status,
    created_at,
    converted,
    first_name,
    phone,
    created_by,
    modified_by,
    modified_at,
    street,
    city,
    state,
    zip_code,
    country,
    last_activity_at,
    fiscal_number,
    birthday,
    interested_in,
    contact_type,
    campaign,
    occupation,
    gender,
    customer_type
  FROM leads WHERE
  zoho_deleted is false
  and zoho_delete_response is null
  and converted is false
  and source_deleted is false
  order by id desc`;
}

export { synchronize_leads };
