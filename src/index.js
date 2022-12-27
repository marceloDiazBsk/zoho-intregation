const { Client, Pool } = require("pg");
const { default: axios } = require("axios");
const dotenv = require("dotenv");

dotenv.config();

const connectDb = async () => {
  const pool = new Pool({
    user: process.env.PGUSER,
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: process.env.PGPORT,
  });
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const leadQuery = `SELECT 
        "Id" 
      FROM LEADS WHERE 
        extract(year from last_activity_date) = 2021 
        and zoho_deleted = false 
        and zoho_delete_response is null 
        and "Converted Deal" is null 
        and "Converted Time" is null
        and "Campaña Digital" ilike 'Herrera%'
        ORDER BY last_activity_date asc`;
    const leadUpdate =
      'update leads set "zoho_deleted" = $1, "zoho_delete_response" = $2 where "Id" = $3 ;';

    const res = await client.query(leadQuery);

    if (res) {
      if (res.rows && res.rows.length > 0) {
        const leadList = [];

        let leadprocessed = 0;
        for (let index = 0; index < res.rows.length; index++) {
          const row = res.rows[index];
          leadList.push(row.Id);

          if (leadList.length === 100 || res.rows.length === index + 1) {
            const response = await delete_lead(leadList);

            if (response.code === "INVALID_TOKEN") {
              throw "INVALID_TOKEN - Refresh the token again";
            }

            if (response.data && response.data.length > 0) {
              for (let i = 0; i < response.data.length; i++) {
                const item = response.data[i];
                //console.log('item', item);
                const id = item.details.id;
                const code = item.code;
                const result = code === "SUCCESS";

                const values = [result, code, id];
                //console.log('values', values);

                const updateResult = await client.query(leadUpdate, values);
                if (updateResult.rowCount != 1) {
                }

                //console.log("updateResult", updateResult);
              }

              await client.query("COMMIT");
            } else {
              throw "UNEXPECTED_RESPONSE - Validate the code";
            }

            console.log("Deleted count:", leadList.length);
            leadprocessed += leadList.length;
            leadList.splice(0, leadList.length);
          }
        }
      }
    }
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  console.log("Finish connectDb");
};

connectDb();

async function delete_lead(leadList) {
  console.log("url", get_delete_lead_endpoint(leadList));

  try {
    const response = await axios.delete(get_delete_lead_endpoint(leadList), {
      headers: {
        "Content-Type": "application/json",
        Authorization: "Zoho-oauthtoken " + process.env.ZOHO_TOKEN,
      },
    });

    return response.data;
  } catch (error) {
    return error.response.data;
  }
}

function get_delete_lead_endpoint(leadList) {
  return `https://www.zohoapis.com/crm/v3/Leads?ids=${leadList.join(
    ","
  )}&wf_trigger=true`;
}
