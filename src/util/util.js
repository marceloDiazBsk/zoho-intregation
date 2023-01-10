import lodash from "lodash";
import logger from "../logger.js";

function compare_lists(sourceList, targeList) {
  const startInMilis = Date.now();
  logger.info("compare_lists start");
  const insertList = [];
  const updateList = [];
  const deleteList = [];

  for (let index = 0; index < sourceList.length; index++) {
    const sourceItem = sourceList[index];
    const targetItem = targeList.find(
      (targetItem) => sourceItem.id === targetItem.id
    );
    if (targetItem) {
      const diffObj = get_diff_in_object(sourceItem, targetItem);
      if (!lodash.isEmpty(diffObj)) {
        diffObj.id = sourceItem.id;
        updateList.push(diffObj);
      }
    } else {
      insertList.push(sourceItem);
    }
  }

  for (let index = 0; index < targeList.length; index++) {
    const targetItem = targeList[index];
    const sourceItem = sourceList.find(
      (sourceItem) => sourceItem.id === targetItem.id
    );
    if (!sourceItem) deleteList.push(targetItem);
  }

  logger.info("compare_lists end in", Date.now() - startInMilis, "ms");
  return { insertList, updateList, deleteList };
}

function get_diff_in_object(sourceItem, targetItem) {
  const diffObj = {};
  for (const key in sourceItem) {
    if (Object.hasOwnProperty.call(sourceItem, key)) {
      const sourceData = sourceItem[key];
      const targetData = targetItem[key];
      if (sourceData != targetData) diffObj[key] = sourceData;
    }
  }
  return diffObj;
}

async function insert_model_db(db, itemList, tableName) {
  try {
    for (let index = 0; index < itemList.length; index++) {
      const item = itemList[index];
      const { statement, values } = get_insert_model(item, tableName);

      const insertResult = await db.query(statement, values);

      if (insertResult.rowCount == 0) {
        logger.error("error in insert_model_db", item);
      }
    }
  } catch (error) {
    logger.error("insert_model_db", error);
    throw error;
  }
}

function get_insert_model(item, tableName) {
  let paramQty = 1;
  let firstPart = "INSERT INTO " + tableName + "( ";
  let secondPart = " ) VALUES ( ";
  let lastPart = " );";
  const values = [];
  for (const key in item) {
    firstPart += key + ",";
    secondPart += "$" + paramQty + ",";
    values.push(item[key]);
    paramQty++;
  }
  firstPart = firstPart.substring(0, firstPart.length - 1);
  secondPart = secondPart.substring(0, secondPart.length - 1);
  const statement = firstPart + secondPart + lastPart;
  return { statement, values };
}

async function update_model_db(db, itemList, tableName) {
  try {
    for (let index = 0; index < itemList.length; index++) {
      const item = itemList[index];
      const { statement, values } = get_update_model(item, tableName);

      const updateResult = await db.query(statement, values);

      if (updateResult.rowCount == 0) {
        logger.error("error in update", item);
      }
    }
  } catch (error) {
    logger.error("update_model_db", error);
    throw error;
  }
}

function get_update_model(item, tableName) {
  let paramQty = 1;
  let statement = "UPDATE " + tableName + " SET ";
  const values = [];
  for (const key in item) {
    if (key !== "id") {
      statement += key + " = $" + paramQty + ",";
      values.push(item[key]);
      paramQty++;
    }
  }
  statement = statement.substring(0, statement.length - 1);
  statement += " WHERE id = $" + paramQty;
  values.push(item.id);
  return { statement, values };
}

async function delete_model_db(db, itemList, tableName) {
  try {
    for (let index = 0; index < itemList.length; index++) {
      const item = itemList[index];
      const statement =
        "UPDATE " + tableName + " SET source_deleted = true where id = $1";

      const updateResult = await db.query(statement, [item.id]);

      if (updateResult.rowCount == 0) {
        logger.error("error in delete_model_db", item);
      }
    }
  } catch (error) {
    logger.error("delete_model_db", error);
    throw error;
  }
}

export { compare_lists, insert_model_db, update_model_db, delete_model_db };
