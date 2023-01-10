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
    if (!sourceItem) deleteList.push(sourceItem);
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
      console.log('item', item);
      const { statement, values } = get_insert_model(item, tableName);
      console.log('statement', statement);
      logger.info('statement', statement);

      const insertResult = await db.query(statement, values);

      if (insertResult.rowCount == 0) {
        logger.error("error in insert", lead);
      }
    }
  } catch (error) {
    logger.error("insert_leads_db", error);
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

export { compare_lists, insert_model_db };
