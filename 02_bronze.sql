USE DATABASE flowbridge_dev_db;

USE ROLE ACCOUNTADMIN;

-- create storage account
CREATE OR REPLACE STORAGE INTEGRATION  flowbridge_adls_integration
type = external_stage
storage_provider = azure
enabled = true
azure_tenant_id = '40c9e89a-e383-4d5a-a2cc-9464aa03bff9'
storage_allowed_locations = (
'azure://flowbridgeadls.blob.core.windows.net/supply-chain-raw-dev/',
'azure://flowbridgeadls.blob.core.windows.net/supply-chain-raw-prod/'
);


DESC INTEGRATION  flowbridge_adls_integration;

-- gran to sysadmin
grant usage on integration flowbridge_adls_integration to role sysadmin;

-- create notification integration

create or replace notification integration flowbridge_notification_inti
type = queue
enabled = true
notification_provider = azure_storage_queue
azure_storage_queue_primary_uri = 'https://flowbridgeadls.queue.core.windows.net/flowbridge-supply-chain-queue'
azure_tenant_id = '40c9e89a-e383-4d5a-a2cc-9464aa03bff9';

desc integration flowbridge_notification_inti;

grant usage on integration flowbridge_notification_inti to role sysadmin;


use role sysadmin;
use database flowbridge_dev_db;
use schema flowbridge_dev_db.bronze_sch;
use warehouse flowbridge_pipeline_wh;

--create file format for json
create or replace file format bronze_sch.json_file_format
type ='json'
strip_outer_array = true
comment = 'json file format for flowbridge project.';

-- create external stage

create or replace stage bronze_sch.adls_raw_stage
url = 'azure://flowbridgeadls.blob.core.windows.net/supply-chain-raw-dev/'
storage_integration = flowbridge_adls_integration
file_format = bronze_sch.json_file_format
comment = 'external stage -adls gen2 container.';

list @bronze_sch.adls_raw_stage;

--------------------------------------
/*
raw orders tale
transient table - no fail-safe, cost efficient , relodable
metadata columns added at ingestion time
*/ 

create or replace transient table bronze_sch.raw_orders(
raw_data variant,
ingested_at timestamp_ntz default current_timestamp(),
file_name string,
file_row_number number,
load_id string default uuid_string()
)
comment = 'bronze layer - raw orders supply chain for flowbridge';


/*-------------SNOWPIPE----------------*/
CREATE or replace PIPE bronze_sch.supply_chain_pipe
auto_ingest = true
integration = flowbridge_notification_inti
comment = 'snowpipe for flowbridge supply chain raw data injestion. '
as
copy into bronze_sch.raw_orders(
raw_data,
file_name,
file_row_number
) from (
select 
$1,
metadata$filename,
metadata$file_row_number
from @bronze_sch.adls_raw_stage
)
file_format = (format_name = 'bronze_sch.json_file_format');


/* ---------------check snowpipe status-----------*/

select system$pipe_status('bronze_sch.supply_chain_pipe');

/*------ refresh snowpipe to ingest  existing files which are loaded before snopipe created.*/
alter pipe bronze_sch.supply_chain_pipe refresh;

/*check raw data*/
select * from bronze_sch.raw_orders;
/*check ingestion history*/

SELECT * FROM TABLE(
information_schema.copy_history(
table_name => 'raw_orders',
start_time => dateadd(hour,-12,current_timestamp())
));
