set env = 'DEV';
set db = 'FLOWBRIDGE_'|| $env || '_DB';

USE DATABASE identifier($db);
use role sysadmin;
use schema silver_sch;
use warehouse flowbridge_pipeline_wh;


/*
step 1
create transient table for dead letter
*/

create or replace transient table silver_sch.dead_letter(
raw_data variant,
error_reason string,
file_name string,
file_row_number number,
rejected_at timestamp_ntz default current_timestamp()
)
comment = 'dead letter table = rejected / bad records from bronze.';


/*
step 2
orders table - permanent table
*/


create or replace table silver_sch.stg_orders(
-- order fields
ORDER_ID STRING comment  'unique order identifier',
ORDER_DATE timestamp_ntz comment  'order timestamp',
ORDER_STATUS STRING comment  'current order status',

-- Customer fields
CUSTOMER_ID STRING comment 'customer id',
CUSTOMER_NAME STRING comment ' customer name',
CUSTOMER_REGION STRING COMMENT 'Customer region',
CUSTOMER_SEGMENT STRING COMMENT 'Customer segment',

-- Supplier fields
SUPPLIER_ID STRING comment 'supplier identifier',
SUPPLIER_NAME STRING comment 'supplier name',
SUPPLIER_COUNTRY STRING COMMENT 'Supplier country',
LEAD_TIME_DAYS number comment 'supplier lead time in days.',
PERFORMANCE_SCORE FLOAT COMMENT 'Supplier performance score',

-- Shipment fields
SHIPMENT_ID STRING comment 'shipment identifier',
CARRIER STRING comment 'shipping carrier',
SHIP_DATE timestamp_ntz comment 'shipment date',
ESTIMATED_DELIVERY DATE comment 'estimated delivery date',
DELAY_DAYS number comment 'number of delay days',

-- Product fields (flattened from items array)
PRODUCT_ID STRING comment 'string identifier',
PRODUCT_NAME STRING COMMENT 'Product name',
CATEGORY string comment 'product category',
QUANTITY number comment 'order quantity',
UNIT_PRICE float comment 'unit price',


-- Financial fields
TOTAL_AMOUNT float comment 'total order amount',
PAYMENT_STATUS STRING COMMENT 'Payment status',

-- Warehouse fields
WAREHOUSE_ID string comment 'warehouse identifier',
WAREHOUSE_LOCATION STRING COMMENT 'Warehouse location',
INVENTORY_LEVEL number comment 'current inventory level',

-- Metadata
FILE_NAME string comment 'source file name',
FILE_ROW_NUMBER NUMBER comment 'row number in source file',
INGESTED_AT timestamp_ntz comment 'when records was ingested',
TRANSFORMED_AT timestamp_ntz default current_timestamp() comment 'when records was transformed'

) COMMENT = 'Silver Stage 1 - flattened and cleaned supply chain orders';


/*
setp 3
create stream on raw_orders from bronze layer
*/
create or replace stream silver_sch.raw_orders_stream 
on table FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS
append_only = true
show_initial_rows = true
comment = 'stream on raw_orders - captures new data for silver processig.';

-- Verify stream created
show streams;
SELECT * FROM silver_sch.raw_orders_stream ;
-- STEP 4 - STORED PROCEDURE: SP_BRONZE_TO_SILVER
/*Purpose : Two responsibilities:
1. Route bad records to DEAD_LETTER
2. MERGE clean records into STG_ORDERS
Why SP : Reusable, independently testable, clean separation
from Task orchestration logic
Called by: BRONZE_TO_SILVER_TASK (every 1 minute)
Test by : CALL SILVER_SCH. SP_BRONZE_TO_SILVER()
*/


create or replace procedure silver_sch. sp_bronze_to_silver()
returns string
Language sql
comment = 'SP - routes bas records to dead_letter, merges clean reocrds to stg_orders - will be called by task'
as 
begin

-- PART 0 - BUFFER STREAM INTO TEMP TABLE
-- Stream can only be consumed once per DML statement.
-- We materialise it here so both DEAD_LETTER and MERGE
--can read from the same snapshot.

create or replace TEMPORARY table silver_sch.stream_buffer
as 
SELECT * FROM silver_sch.raw_orders_stream 
WHERE METADATA$ACTION = 'INSERT';

-- PART 1 - ROUTE BAD RECORDS TO DEAD LETTER
-- Only truly unrecoverable records are rejected
-- Validation is case-insensitive using UPPER(TRIM())
--order_date: reject ONLY when BOTH conditions fail
--(AND logic) - ISO format passes TRY_TO_TIMESTAMP_NTZ
--DD-MM-YYYY matches LIKE pattern
-- order_status: UPPER(TRIM()) before IN check
--so "shipped"/"Shipped"/"SHIPPED" all pass



INSERT INTO silver_sch.dead_letter(
raw_data,
error_reason,
file_name,
file_row_number
)
SELECT 
S.RAW_DATA,
CASE 
--order level validation
WHEN S.RAW_DATA:order_id::STRING IS NULL THEN 'Missing order_id'
WHEN TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL
AND S.RAW_DATA:order_date NOT LIKE '__-__-____' THEN 'invalid or missing order_date'
when UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN('PENDING','PROCESSING','SHIPPED','IN TRANSIT','DELIVERED','CANCELLED') THEN 'invalid order status' || COALESCE(S.RAW_DATA:order_status::STRING,'NULL')
--customer level validation
WHEN S.RAW_DATA:customer.customer_id::STRING IS NULL THEN 'missing customer_id'
--supplier level
WHEN S.RAW_DATA:supplier.supplier_id::STRING IS NULL THEN 'missing supplier_id'
WHEN S.RAW_DATA:supplier.performance_score::FLOAT > 100 THEN 'invalid performance_score: '|| S.RAW_DATA:supplier.performance_score::STRING
WHEN S.RAW_DATA:supplier.performance_score::FLOAT < 100 THEN 'invalid performance_score: '|| S.RAW_DATA:supplier.performance_score::STRING
WHEN S.RAW_DATA:supplier.lead_time_days::NUMBER < 100 THEN 'invalid lead_time_days: '|| S.RAW_DATA:supplier.lead_time_days::STRING

--product level

WHEN S.RAW_DATA:items[0].product_id::STRING IS NULL THEN 'invalid product_id'
WHEN S.RAW_DATA:items[0].quantity::NUMBER IS NULL OR S.RAW_DATA:items[0].quantity::NUMBER <=0
THEN 'invalid quantity: '|| coalesce(S.RAW_DATA:items[0].quantity::STRING,'NULL')
WHEN S.RAW_DATA:items[0].unit_price::FLOAT IS NULL OR S.RAW_DATA:items[0].unit_price::FLOAT < 0
THEN 'unit_price unit_price: '|| coalesce(S.RAW_DATA:items[0].unit_price::STRING,'NULL')


-- - Financial Level
WHEN S.RAW_DATA: financials. total_amount:: FLOAT IS NULL
OR S.RAW_DATA:financials.total_amount::FLOAT < 0
THEN 'Invalid total_amount: ' || COALESCE(S.RAW_DATA: financials. total_amount :: STRING, 'NULL')

-- - Warehouse Level
WHEN S.RAW_DATA:warehouse.inventory_Level::NUMBER < 0
THEN 'Invalid inventory_Level < 0: ' || S.RAW_DATA:warehouse.inventory_Level:: STRING

ELSE 'Unknown'
END AS ERROR_REASON,
S.FILE_NAME,
S.FILE_ROW_NUMBER
FROM silver_sch.stream_buffer S
WHERE

--Order Level
S.RAW_DATA:order_id::STRING IS NULL
OR
(
TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL
AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____'
)

OR UPPER(TRIM(S.RAW_DATA:order_status::STRING) ) NOT IN (
'PENDING', 'PROCESSING', 'SHIPPED',
'IN TRANSIT', 'DELIVERED', 'CANCELLED'
)

-- Customer Level
OR S.RAW_DATA:customer.customer_id::STRING IS NULL
-- Supplier Level
OR S.RAW_DATA:supplier.supplier_id::STRING IS NULL
OR S.RAW_DATA:supplier.performance_score::FLOAT > 100
OR S.RAW_DATA:supplier.performance_score::FLOAT < 0
OR S.RAW_DATA:supplier.Lead_time_days::NUMBER < 0
-- Product Level
OR S.RAW_DATA:items[0].product_id::STRING IS NULL
OR S.RAW_DATA:items[0].quantity::NUMBER IS NULL
OR S.RAW_DATA:items[0].quantity::NUMBER <= 0
OR S.RAW_DATA:items[0].unit_price::FLOAT IS NULL
OR S.RAW_DATA:items[0].unit_price::FLOAT < 0
-- Financial Level
OR S.RAW_DATA:financials.total_amount::FLOAT IS NULL
OR S.RAW_DATA:financials.total_amount::FLOAT < 0
-- Warehouse Level
OR S.RAW_DATA:warehouse.inventory_Level::NUMBER < 0;




MERGE INTO silver_sch.stg_orders AS TGT 
USING (

SELECT
--order fields
UPPER(TRIM(S.RAW_DATA:order_id::STRING)) AS ORDER_ID,
TRY_TO_TIMESTAMP_NTZ(
CASE WHEN S.RAW_DATA:order_date::STRING LIKE '__-__-____'
THEN TO_VARCHAR(
TO_DATE(S.RAW_DATA:order_date::STRING,'DD-MM-YYYY'),'YYYY-MM-DD') || 'T00:00:00Z'
ELSE S.RAW_DATA:order_date::STRING END) AS ORDER_DATE,
UPPER(TRIM(S.RAW_DATA:order_status::STRING)) AS ORDER_status,

--customer fields
UPPER(TRIM(S.RAW_DATA:customer.customer_id::STRING)) AS CUSTOMER_ID,
COALESCE(UPPER(TRIM(S.RAW_DATA:customer.customer_name::STRING)),'UNKNOWN') AS CUSTOMER_NAME,
COALESCE(UPPER(TRIM(S.RAW_DATA:customer.region::STRING)),'UNKNOWN') AS CUSTOMER_REGION,
COALESCE(UPPER(TRIM(S.RAW_DATA:customer.segment::STRING)),'UNKNOWN') AS CUSTOMER_SEGMENT,

--spplier fields
 UPPER(TRIM(S.RAW_DATA:supplier.supplier_id::STRING)) AS SUPPLIER_ID,
 COALESCE(UPPER(TRIM(S.RAW_DATA:supplier.supplier_name::STRING)),'UNKNOWN') AS SUPPLIER_NAME,
 COALESCE(UPPER(TRIM(S.RAW_DATA:supplier.country::STRING)),'UNKNOWN') AS SUPPLIER_COUNTRY,
 COALESCE(S.RAW_DATA:supplier.lead_time_days::NUMBER,0) AS LEAD_TIME_DAYS,
 COALESCE(S.RAW_DATA:supplier.performance_score::FLOAT,0) AS PERFORMANCE_SCORE,

--shipment fields
COALESCE(UPPER(TRIM(s.raw_data:shipment.shipment_id::STRING)),'UNKNOWN') AS SHIPMENT_ID,
COALESCE(UPPER(TRIM(s.raw_data:shipment.carrier::STRING)),'UNKNOWN') AS CARRIER,
TRY_TO_TIMESTAMP_NTZ(s.raw_data:shipment.ship_date::STRING) AS SHIP_DATE,
TRY_TO_DATE(s.raw_data:shipment.estimated_delivery::STRING) AS ESTIMATED_DELIVERY,
GREATEST(COALESCE(s.raw_data:shipment.delay_days::NUMBER,0),0) AS  DELAY_DAYS,

--product fields

UPPER(TRIM(S.RAW_DATA:items[0].product_id::STRING)) AS PRODUCT_ID,
COALESCE(UPPER(TRIM(S.RAW_DATA:items[0].product_name::STRING)),'UNKNOWN') AS PRODUCT_NAME,
COALESCE(UPPER(TRIM(S.RAW_DATA:items[0].category::STRING)),'UNKNOWN') AS CATEGORY,
(S.RAW_DATA:items[0].quantity::NUMBER) AS QUANTITY,
(S.RAW_DATA:items[0].unit_price::FLOAT) AS UNIT_PRICE,

--financial fields
S.RAW_DATA:financials.total_amount::FLOAT AS TOTAL_AMOUNT,
COALESCE(UPPER(TRIM(S.RAW_DATA:financials.payment_status::STRING)),'UNKNOWN') AS PAYMENT_STATUS,

--warehouse fields
COALESCE(UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_id::STRING)),'UNKNOWN') AS WAREHOUSE_ID,
COALESCE(UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_location::STRING)),'UNKNOWN') AS WAREHOUSE_LOCATION,
COALESCE(S.RAW_DATA:warehouse.inventory_level::STRING,0) AS INVENTORY_LEVEL,
S.FILE_NAME,
S.FILE_ROW_NUMBER,
INGESTED_AT 

FROM silver_sch.stream_buffer S
WHERE 
S.RAW_DATA:order_id::STRING IS NOT NULL 
AND (
TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NOT NULL
OR S.RAW_DATA:order_date::STRING LIKE '__-__-____')
AND UPPER(TRIM(S.RAW_DATA:order_status::STRING))  IN (
'PENDING', 'PROCESSING', 'SHIPPED',
'IN TRANSIT', 'DELIVERED', 'CANCELLED'
)

AND S.RAW_DATA:customer.customer_id::STRING IS NOT NULL 
AND S.RAW_DATA:supplier.supplier_id::STRING IS NOT NULL 
AND S.RAW_DATA:supplier.performance_score::FLOAT BETWEEN 0 AND 100 
AND (
S.RAW_DATA:supplier.lead_time_days::NUMBER IS NULL
OR 
 S.RAW_DATA:supplier.lead_time_days::NUMBER  >= 0 
 )

 AND S.RAW_DATA:items[0].product_id::STRING IS NOT NULL
 AND S.RAW_DATA:items[0].quantity::NUMBER IS NOT NULL
 AND S.RAW_DATA:items[0].quantity::NUMBER > 0
 AND S.RAW_DATA:items[0].unit_price::FLOAT IS NOT NULL
 AND S.RAW_DATA:items[0].unit_price::FLOAT >= 0
 AND S.RAW_DATA:financials.total_amount::FLOAT IS NOT NULL
 AND S.RAW_DATA:financials.total_amount::FLOAT >= 0
AND (
S.RAW_DATA:warehouse.inventory_level::NUMBER IS NULL
OR
S.RAW_DATA:warehouse.inventory_level::NUMBER >= 0 
)

) SRC ON TGT.ORDER_ID = SRC.ORDER_ID
WHEN MATCHED THEN UPDATE SET 
TGT.ORDER_STATUS = SRC.ORDER_STATUS,
TGT.CARRIER = SRC.CARRIER,
TGT.SHIP_DATE = SRC.SHIP_DATE,
TGT.ESTIMATED_DELIVERY = SRC.ESTIMATED_DELIVERY,
TGT.DELAY_DAYS = SRC.DELAY_DAYS,
TGT.INVENTORY_LEVEL = SRC.INVENTORY_LEVEL,
TGT.PAYMENT_STATUS = SRC.PAYMENT_STATUS,
TGT.CUSTOMER_NAME = SRC.CUSTOMER_NAME,
TGT.CUSTOMER_REGION = SRC.CUSTOMER_REGION,
TGT.SUPPLIER_NAME = SRC.SUPPLIER_NAME,
TGT.PERFORMANCE_SCORE = SRC.PERFORMANCE_SCORE,
TGT.WAREHOUSE_ID = SRC.WAREHOUSE_ID,
TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
TGT.TRANSFORMED_AT = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
ORDER_ID,ORDER_DATE,ORDER_STATUS,
CUSTOMER_ID,CUSTOMER_NAME,CUSTOMER_REGION,CUSTOMER_SEGMENT,
SUPPLIER_ID,SUPPLIER_NAME,SUPPLIER_COUNTRY,LEAD_TIME_DAYS,PERFORMANCE_SCORE,
SHIPMENT_ID,CARRIER,SHIP_DATE,ESTIMATED_DELIVERY,DELAY_DAYS,
PRODUCT_ID,PRODUCT_NAME,CATEGORY,QUANTITY,UNIT_PRICE,
TOTAL_AMOUNT,PAYMENT_STATUS,
WAREHOUSE_ID,WAREHOUSE_LOCATION,INVENTORY_LEVEL,
FILE_NAME,FILE_ROW_NUMBER,INGESTED_AT
) VALUES(
SRC.ORDER_ID,SRC.ORDER_DATE,SRC.ORDER_STATUS,
SRC.CUSTOMER_ID,SRC.CUSTOMER_NAME,SRC.CUSTOMER_REGION,SRC.CUSTOMER_SEGMENT,
SRC.SUPPLIER_ID,SRC.SUPPLIER_NAME,SRC.SUPPLIER_COUNTRY,SRC.LEAD_TIME_DAYS,SRC.PERFORMANCE_SCORE,
SRC.SHIPMENT_ID,SRC.CARRIER,SRC.SHIP_DATE,SRC.ESTIMATED_DELIVERY,SRC.DELAY_DAYS,
SRC.PRODUCT_ID,SRC.PRODUCT_NAME,SRC.CATEGORY,SRC.QUANTITY,SRC.UNIT_PRICE,
SRC.TOTAL_AMOUNT,SRC.PAYMENT_STATUS,
SRC.WAREHOUSE_ID,SRC.WAREHOUSE_LOCATION,SRC.INVENTORY_LEVEL,
SRC.FILE_NAME,SRC.FILE_ROW_NUMBER,SRC.INGESTED_AT
);
                                                                                                                                                    DROP TABLE IF EXISTS  silver_sch.stream_buffer;
                                                                                                                                                    RETURN 'sp_bronze_to_silver Completed successfully';
                                                                                                                                                    
                                                                                                                                                    END;
                                                                                                                                                                        



--===================================================
--STEP 5
--TASK: bronze_to_silver_task
--purpose: orchestrate sp_bronze_to_silve every  minute
--schedule: every 1 minute
--when: SYSTEM$STREAM_HAS_DATA('STREAM_NAME') -NO DATA -NO RUN- NO COST
--NOTE: TASK CREATED SUSPENDED BY DEFAULT
--===================================================

create or replace task silver_sch.bronze_to_silver_task
warehouse = flowbridge_pipeline_wh
schedule = '1 minute'
comment = 'this task is intended to run sp_bronze_to_silve for every 1 minut and when stream has data.'
when SYSTEM$STREAM_HAS_DATA('silver_sch.raw_orders_stream')
AS
CALL silver_sch. sp_bronze_to_silver();


-- RESUME TASK
ALTER TASK silver_sch.bronze_to_silver_task RESUME;

