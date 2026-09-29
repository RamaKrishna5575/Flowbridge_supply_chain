USE ROLE ACCOUNTADMIN;

--------------------------DATABASES------------------------
CREATE DATABASE IF NOT EXISTS flowbridge_dev_db
comment = 'flowbridge supply chain - development database';

CREATE DATABASE IF NOT EXISTS flowbridge_prod_db
comment = 'flowbridge supply chain - production database';


-----------------SCHEMA-------------------------

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.bronze_sch
comment = 'RAW injestion layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.silver_sch
comment = 'Transformation layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.gold_sch
comment = 'Aggregation layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.serving_sch
comment = 'Serving layer';

-------------PROD SCHEMA--------------------

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.bronze_sch
comment = 'RAW injestion layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.silver_sch
comment = 'Transformation layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.gold_sch
comment = 'Aggregation layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.serving_sch
comment = 'Serving layer';


------------------WAREHOUSES------------------

--Pipeline WH -snowpipe + streams + tasks + dynamic tables
create warehouse if not exists flowbridge_pipeline_wh
warehouse_size = 'x-small'
auto_suspend = 60
auto_resume = true
comment = 'pipeline workloads = injestion + transformation';

-- analytics WH --streamlit + secure views + reader account

create warehouse if not exists flowbridge_analytics_wh
warehouse_size = 'x-small'
auto_suspend = 60
auto_resume = true
comment = 'pipeline workloads = streamlit + datasharing';


--------pipeline warehouse monitor--------------
create or replace resource monitor flowbridge_pipeline_rm
with credit_quota = 20
frequency = monthly
start_timestamp = immediately
triggers 
 on 75 percent do notify
 on 90 percent do notify
 on 100 percent do suspend;

 create or replace resource monitor flowbridge_analytics_rm
with credit_quota = 20
frequency = monthly
start_timestamp = immediately
triggers 
 on 75 percent do notify
 on 90 percent do notify
 on 100 percent do suspend;

 ----assign warehouse with resoure monitor

ALTER WAREHOUSE flowbridge_pipeline_wh SET RESOURCE_MONITOR = flowbridge_pipeline_rm;
ALTER WAREHOUSE flowbridge_analytics_wh SET RESOURCE_MONITOR = flowbridge_analytics_rm;

-- GRANT PREVILAGES TO SYSADMIN
USE ROLE ACCOUNTADMIN;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;

grant usage on warehouse flowbridge_pipeline_wh to role sysadmin;
grant usage on warehouse flowbridge_analytics_wh to role sysadmin;

grant all privileges on database flowbridge_dev_db to role sysadmin;
grant all privileges on database flowbridge_prod_db to role sysadmin;

grant all privileges on all schemas in database flowbridge_dev_db to role sysadmin;
grant all privileges on all schemas in database flowbridge_prod_db to role sysadmin;