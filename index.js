const oracledb = require("oracledb");
const fs = require("fs");
const json2csv = require("json2csv").parse;

async function run() {
  let clientOpts = {
    libDir: "M:\\instantclient_19_19",
  };

  oracledb.initOracleClient(clientOpts);
  const hrPool = {
    user: "apps",
    // password: "a636n36w", // contains the hr schema password
    // connectString: "10.0.0.109:1521/PROD",
    connectString: "10.0.0.113:1531/TEST",
    password: "atsapps", // contains the hr schema password
    poolMin: 15,
    poolMax: 20,
    poolIncrement: 5,
    poolTimeout: 120,
  };

  const connectionPool = await oracledb.createPool(hrPool);
  const connection = await oracledb.getConnection();
  // // const connection = await oracledb.getConnection({
  // //   user: 'apps',
  // //   password: 'a636n36w', // contains the hr schema password
  // //   connectString: '10.0.0.109:1521/PROD',
  // // });CUST_PO_NUMBER

  //   const result = await connection.execute(
  //     `SELECT B.TRX_NUMBER AS ORDER_NUMBER,
  //     SUBSTR(CUST_PO_NUMBER, 5) AS CUST_PO_NUMBER,
  //     (SELECT SUM(NVL(LL.UNIT_SELLING_PRICE, 0) * NVL(LL.ORDERED_QUANTITY, 0))
  //      FROM OE_ORDER_LINES_ALL LL
  //      WHERE LL.HEADER_ID = A.HEADER_ID) AS PRICE,
  //     A.HEADER_ID
  // FROM OE_ORDER_HEADERS_ALL A,
  //   RA_CUSTOMER_TRX_ALL B
  // WHERE B.INTERFACE_HEADER_CONTEXT = 'ORDER ENTRY'
  // AND A.ORDER_NUMBER = B.INTERFACE_HEADER_ATTRIBUTE1
  // AND TRUNC(ORDERED_DATE) >= TRUNC(SYSDATE) - 90
  // AND REGEXP_LIKE(CUST_PO_NUMBER, 'BG1037')`
  //   );

  // const ordersHeadersAll = await connection.execute(
  //   `SELECT * FROM OE_ORDER_LINES_ALL WHERE ORDERED_ITEM='MONISH123'`
  // );
  // console.log(ordersHeadersAll.rows);
  // saveCSV(ordersHeadersAll);

  // const linesAll = await connection.execute(
  //   'SELECT * FROM OE_ORDER_LINES_ALL WHERE HEADER_ID='5743049' OR HEADER_ID='5772083''
  // );
  // saveCSV(linesAll);

  // const insertOrderStatement = `INSERT INTO OE_ORDER_HEADERS_ALL (HEADER_ID, ORG_ID, ORDER_TYPE_ID, ORDER_NUMBER, VERSION_NUMBER, ORIG_SYS_DOCUMENT_REF, ORDERED_DATE, REQUEST_DATE, PRICING_DATE, PRICE_LIST_ID,TAX_EXEMPT_FLAG, TRANSACTIONAL_CURR_CODE, CUST_PO_NUMBER, PAYMENT_TERM_ID, SOLD_FROM_ORG_ID, SOLD_TO_ORG_ID, SHIP_FROM_ORG_ID, SHIP_TO_ORG_ID, INVOICE_TO_ORG_ID, CREATION_DATE, CREATED_BY, LAST_UPDATED_BY, LAST_UPDATE_DATE, LAST_UPDATE_LOGIN, ATTRIBUTE1, ATTRIBUTE2, CANCELLED_FLAG, OPEN_FLAG, BOOKED_FLAG, SALESREP_ID, ORDER_CATEGORY_CODE, FLOW_STATUS_CODE, BOOKED_DATE, LOCK_CONTROL, XML_MESSAGE_ID, TRANSACTION_PHASE_CODE) VALUES ('9999000', '82', '1003', '999999', '0', 'OE_ORDER_HEADERS_ALL9999000', TO_DATE('2023-06-21', 'YYYY-MM-DD'), TO_DATE('2023-06-21', 'YYYY-MM-DD'), TO_DATE('2023-06-21', 'YYYY-MM-DD'), '762713', 'S', 'AED', 'COD.BMON9874', '1006', '82', '999000', '85', '99999', '99999', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '9999', '0', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '-1', '11DXB', '85', 'N', 'Y', 'Y', '999999999', 'MIXED', 'BOOKED', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '4', '9999999', 'F')`;

  // // const insertOrderStatement2 = `INSERT INTO OE_ORDER_HEADERS_ALL ('CUST_PO_NUMBER') VALUES ('COD.BMON9874')`;
  // const insertOrderResult = await connection.execute(insertOrderStatement);
  // console.log(insertOrderResult);
  // console.log(
  //   'Insert Order String',
  //   JSON.stringify(insertOrderResult, null, 4)
  // );

  // const insertLineStatement = `INSERT INTO OE_ORDER_LINES_ALL (LINE_ID, ORG_ID, HEADER_ID, LINE_TYPE_ID, LINE_NUMBER, ORDERED_ITEM, REQUEST_DATE, PROMISE_DATE, SCHEDULE_SHIP_DATE, ORDER_QUANTITY_UOM, PRICING_QUANTITY, PRICING_QUANTITY_UOM, CANCELLED_QUANTITY, SHIPPED_QUANTITY, ORDERED_QUANTITY, FULFILLED_QUANTITY, SHIPPING_QUANTITY, SHIPPING_QUANTITY_UOM, DELIVERY_LEAD_TIME, TAX_EXEMPT_FLAG, SHIP_FROM_ORG_ID, SHIP_TO_ORG_ID, INVOICE_TO_ORG_ID, SOLD_FROM_ORG_ID, SOLD_TO_ORG_ID, CUST_PO_NUMBER, SHIP_TOLERANCE_ABOVE, SHIP_TOLERANCE_BELOW, INVENTORY_ITEM_ID, TAX_DATE, TAX_CODE, INVOICE_INTERFACE_STATUS_CODE, PRICE_LIST_ID, PRICING_DATE, SHIPMENT_NUMBER, PAYMENT_TERM_ID, ORIG_SYS_DOCUMENT_REF, ORIG_SYS_LINE_REF, UNIT_SELLING_PRICE, UNIT_LIST_PRICE, TAX_VALUE, ATTRIBUTE1, CREATION_DATE, CREATED_BY, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, ITEM_TYPE_CODE, VISIBLE_DEMAND_FLAG, LINE_CATEGORY_CODE, ACTUAL_SHIPMENT_DATE, SCHEDULE_ARRIVAL_DATE, SCHEDULE_STATUS_CODE,SOURCE_TYPE_CODE, CANCELLED_FLAG, OPEN_FLAG, BOOKED_FLAG, SALESREP_ID, ITEM_IDENTIFIER_TYPE, SHIPPING_INTERFACED_FLAG, ORDER_SOURCE_ID, FULFILLED_FLAG, INVOICED_QUANTITY, UNIT_SELLING_PERCENT, SHIPPABLE_FLAG, RE_SOURCE_FLAG, FLOW_STATUS_CODE, CALCULATE_PRICE_FLAG, FULFILLMENT_DATE, SHIPPING_QUANTITY2, SHIPPED_QUANTITY2, LOCK_CONTROL, UNIT_LIST_PRICE_PER_PQTY,UNIT_SELLING_PRICE_PER_PQTY, UNIT_COST, TRANSACTION_PHASE_CODE, ORIGINAL_LIST_PRICE, ACTUAL_FULFILLMENT_DATE) VALUES('99999999', '82', '9999000', '1002', '1', 'MONISH123', TO_DATE('2023-06-21', 'YYYY-MM-DD'), TO_DATE('2023-06-21', 'YYYY-MM-DD'), TO_DATE('2023-06-21', 'YYYY-MM-DD'), 'PCS', '1', 'PCS', '0', '1', '1', '1', '1', 'PCS', '0', 'S', '85', '99999', '99999', '82', '999000', 'COD.BMON9874', '0', '0', '22060', TO_DATE('2023-06-21', 'YYYY-MM-DD'), 'VAT_SR_5', 'YES', '762713', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '1', '1006', 'OE_ORDER_HEADERS_ALL9999000', 'OE_ORDER_LINES_ALL99999999', '14.29', '14.29', '0.71', '7878', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '9999', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '0', '20084041', 'STANDARD', 'Y', 'ORDER', TO_DATE('2023-06-21', 'YYYY-MM-DD'), TO_DATE('2023-06-21', 'YYYY-MM-DD'), 'SCHEDULED', 'INTERNAL', 'N', 'N', 'Y', '999999999', 'Supplier Item', 'Y', '0', 'Y', '1', '0', 'Y', 'N', 'CLOSED', 'N', TO_DATE('2023-06-21', 'YYYY-MM-DD'), '0', '0', '19', '14.29', '14.29', '4.000117881', 'F', '11.2', TO_DATE('2023-06-21', 'YYYY-MM-DD'))`;
  // const insertLineResult = await connection.execute(insertLineStatement);
  // console.log(insertLineResult);
  // console.log('Insert Line String', JSON.stringify(insertLineResult, null, 4));

  // console.log('Result is:', JSON.stringify(result.rows));

  // const result = await connection.execute(`SELECT * FROM Xxare_Salesorder_Stg`);
  // saveCSV(result);

  // const insertResult = await connection.execute(
  //   `INSERT INTO Xxare_Salesorder_Stg VALUES('P0.MON9998','STANDARD',TO_DATE('2023-08-10', 'YYYY-MM-DD'),'ZAH01',24031,'ZAH01','AED','A112',864426,'7 DAYS','A1','05500053','PCS',3,30,TO_DATE('2023-08-10', 'YYYY-MM-DD'),TO_DATE('2023-08-10', 'YYYY-MM-DD'),'Standard(Line Invoicing)','N',NULL,NULL,NULL,1)`
  // );

  // console.log("Insert Result", insertResult);

  // const commitResult = await connection.execute(`COMMIT`);
  // console.log("Commit Result", commitResult);

  const result = await connection.execute(
    `SELECT * FROM Xxare_Salesorder_Stg WHERE CUSTOMER_NO='ZAH01'`
  );
  saveCSV(result);

  await connection.close(); // Always close connections
}

function saveCSV(result) {
  const columnNames = result.metaData.map((col) => col.name);
  const csvData = result.rows.map((row) => {
    const rowData = {};
    columnNames.forEach((colName, index) => {
      rowData[colName] = row[index];
    });
    return rowData;
  });
  const csv = json2csv(csvData, { fields: columnNames });

  fs.writeFile("test-insert-staging.csv", csv, (err) => {
    if (err) throw err;
    console.log("CSV file has been saved.");
  });
}

run();

/*
Insert Order:
{ lastRowid: 'AABB3oAGIAAC6+WAAG', rowsAffected: 1 }
Insert Order String {
    'lastRowid': 'AABB3oAGIAAC6+WAAG',
    'rowsAffected': 1
}

Insert Line:
{ lastRowid: 'AABB3nAGRAADCj7AAG', rowsAffected: 1 }
Insert Line String {
    'lastRowid': 'AABB3nAGRAADCj7AAG',
    'rowsAffected': 1
}
*/
