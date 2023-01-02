const { default: axios } = require("axios");
const { Client, Pool } = require("pg");
const dotenv = require("dotenv");
const querystring = require("querystring");
const moment = require("moment");
const lodash = require("lodash");
const opts = {
  errorEventName: "error",
  logDirectory: "logs", // NOTE: folder must exist and be writable...
  fileNamePattern: "<DATE>.log",
  dateFormat: "YYYY.MM.DD",
};
const log = require("simple-node-logger").createRollingFileLogger(opts);

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

function get_pool() {
  return new Pool({
    user: process.env.PGUSER,
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: process.env.PGPORT,
  });
}

async function refresh_token() {
  log.info("refresh_token start");

  const client = await get_pool().connect();

  try {
    const query = "SELECT access_token, refresh_token FROM zoho_creds;";
    const update = "UPDATE public.zoho_creds SET access_token=$1";
    const res = await client.query(query);
    if (res) {
      if (res.rows && res.rows.length > 0) {
        const result = res.rows[0];

        log.info("result ", result);

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
          const updateResult = await client.query(update, [
            response.data.access_token,
          ]);
          if (updateResult.rowCount != 1) {
            log.info("No actualizado");
          }
        }
      }
    }
  } catch (error) {
    log.error("Entro al catch", error);
  }

  log.info("refresh_token end");
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

async function get_leads(db) {
  const startInMilis = Date.now();
  log.info("get_leads start");
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
              await refresh_token();
              creds = await get_creds(db);
            } else {
              throw internalError;
            }
          } else {
            throw internalError;
          }
        }

        log.info("fetch_leads ", Date.now() - startInMilisFetch, " ms");
      }
    }
  } catch (error) {
    if (error.response && error.response.data) {
      log.error("error.response.data", error.response.data);
    } else {
      log.error("error", error);
    }
    throw error;
  }
  log.info("get_leads end in ", Date.now() - startInMilis, " ms");
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

async function get_creds(db) {
  const query = "SELECT access_token, refresh_token FROM zoho_creds;";
  const res = await db.query(query);
  return res && res.rows && res.rows.length > 0 ? res.rows[0] : null;
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
  and source_deleted is false`;
}

async function process_leads() {
  const startInMilis = Date.now();
  log.info("process_leads start");
  const db = await get_pool().connect();
  try {
    const zohoResultList = await get_leads(db);
    const zohoLeadList = normalize_zoho_list(zohoResultList);
    const dbResultList = await get_lead_db(db);
    const dbLeadList = normalize_db_List(dbResultList);

    const { insertList, updateList, deleteList } = compare_leads(
      zohoLeadList,
      dbLeadList
    );

    await db.query("BEGIN");
    log.info("insertList.length ", parseFloat(insertList.length));
    if (insertList.length) {
      await insert_leads_db(db, insertList);
    }
    log.info("updateList.length ", parseFloat(updateList.length));
    if (updateList.length) {
      await update_leads_db(db, updateList);
    }

    log.info("deleteList.length ", parseFloat(deleteList.length));
    if (deleteList.length) {
      await delete_leads_db(db, deleteList);
    }
    await db.query("COMMIT");
  } catch (e) {
    await db.query("ROLLBACK");
    throw e;
  } finally {
    db.release();
  }
  log.info("process_leads end in ", Date.now() - startInMilis, " ms");
}

async function delete_leads_db(db, leadList) {
  try {
    for (let index = 0; index < leadList.length; index++) {
      const lead = leadList[index];
      const statement = "UPDATE leads SET source_deleted = true where id = $1";

      const updateResult = await db.query(statement, [lead.id]);

      if (updateResult.rowCount == 0) {
        log.error("error in delete", lead);
      }
    }
  } catch (error) {
    log.error("delete_leads_db", error);
    throw error;
  }
}

async function insert_leads_db(db, leadList) {
  try {
    for (let index = 0; index < leadList.length; index++) {
      const lead = leadList[index];
      const { statement, values } = get_insert_obj(lead);

      const insertResult = await db.query(statement, values);

      if (insertResult.rowCount == 0) {
        log.error("error in insert", lead);
      }
    }
  } catch (error) {
    log.error("insert_leads_db", error);
    throw error;
  }
}

function get_insert_obj(lead) {
  let paramQty = 1;
  let firstPart = "INSERT INTO leads( ";
  let secondPart = " ) VALUES ( ";
  let lastPart = " );";
  const values = [];
  for (const key in lead) {
    firstPart += key + ",";
    secondPart += "$" + paramQty + ",";
    values.push(lead[key]);
    paramQty++;
  }
  firstPart = firstPart.substring(0, firstPart.length - 1);
  secondPart = secondPart.substring(0, secondPart.length - 1);
  const statement = firstPart + secondPart + lastPart;
  return { statement, values };
}

async function update_leads_db(db, leadList) {
  try {
    for (let index = 0; index < leadList.length; index++) {
      const lead = leadList[index];
      const { statement, values } = get_update_obj(lead);

      const updateResult = await db.query(statement, values);

      if (updateResult.rowCount == 0) {
        log.error("error in update", lead);
      }
    }
  } catch (error) {
    log.error("update_leads_db", error);
    throw error;
  }
}

function get_update_obj(lead) {
  let paramQty = 1;
  let statement = "UPDATE LEADS SET ";
  const values = [];
  for (const key in lead) {
    if (key !== "id") {
      statement += key + " = $" + paramQty + ",";
      values.push(lead[key]);
      paramQty++;
    }
  }
  statement = statement.substring(0, statement.length - 1);
  statement += " WHERE id = $" + paramQty;
  values.push(lead.id);
  return { statement, values };
}

function compare_leads(zohoLeadList, dbLeadList) {
  const startInMilis = Date.now();
  log.info("compare_leads start");
  const insertList = [];
  const updateList = [];
  const deleteList = [];

  for (let index = 0; index < zohoLeadList.length; index++) {
    const zohoLead = zohoLeadList[index];
    const dbLead = dbLeadList.find((dbLead) => zohoLead.id === dbLead.id);
    if (dbLead) {
      const diffObj = get_diff_in_object(zohoLead, dbLead);
      if (!lodash.isEmpty(diffObj)) {
        diffObj.id = dbLead.id;
        updateList.push(diffObj);
      }
    } else {
      insertList.push(zohoLead);
    }
  }

  for (let index = 0; index < dbLeadList.length; index++) {
    const dbLead = dbLeadList[index];
    const zohoLead = zohoLeadList.find((zohoLead) => zohoLead.id === dbLead.id);
    if (!zohoLead) deleteList.push(dbLead);
  }

  log.info("compare_leads end in ", Date.now() - startInMilis, " ms");
  return { insertList, updateList, deleteList };
}

function get_diff_in_object(zohoLead, dbLead) {
  const diffObj = {};
  for (const key in zohoLead) {
    if (Object.hasOwnProperty.call(zohoLead, key)) {
      const sourceData = zohoLead[key];
      const targetData = dbLead[key];
      if (sourceData != targetData) diffObj[key] = sourceData;
    }
  }
  return diffObj;
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

process_leads();
process.exitCode = 1;