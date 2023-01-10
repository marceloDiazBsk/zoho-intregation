import axios from "axios";
import dotenv from "dotenv";
import querystring from "querystring";
import lodash from "lodash";
import logger from "./logger.js";
import pkg from "pg";
const { Client, Pool } = pkg;
import fs from 'fs';

dotenv.config();

const ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token";
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

async function process_leads() {
  const startInMilis = Date.now();
  logger.info("===============process_leads start===============");
  const db = await get_pool().connect();
  try {
    const zohoResultList = await get_leads(db);


    logger.info('zohoResultList.length', zohoResultList.length);

    fs.writeFileSync('./leads.json', JSON.stringify(zohoResultList));

    await db.query("COMMIT");
  } catch (e) {
    logger.error("Error in process_leads", e);
    await db.query("ROLLBACK");
    throw e;
  } finally {
    db.release();
  }
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
          logger.log("loopQuantity", loopQuantity);
          logger.log("info", info);
          continueLoop = info.more_records;
          if(!continueLoop){
            logger.info('info.more_records', info.more_records);
            logger.info('response.data.data', JSON.stringify(response.data.data));
          }
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

async function get_creds(db) {
  const query = "SELECT access_token, refresh_token FROM zoho_creds;";
  const res = await db.query(query);
  return res && res.rows && res.rows.length > 0 ? res.rows[0] : null;
}

async function work() {
  await process_leads();
}

async function get_lead_response({ creds, page_token, page }) {
  const edp = get_lead_endpoint({ page_token, page });
  logger.info('edp',edp);
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
    //fields: "id,Last_Name,First_Name,Email",
    sort_order: "desc",
    sort_by: "id",
    per_page: 200,
  };

  if (page_token) query.page_token = page_token;
  if (page) query.page = page;

  return ZOHO_LEADS_URL + "?" + querystring.stringify(query);
}

work();

function get_pool() {
  return new Pool({
    user: process.env.PGUSER,
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: process.env.PGPORT,
  });
}
