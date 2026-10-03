
/*
 DataToCrunch - Snowflake Supply Chain Project
Script: 07_governance/governance.sql
- Purpose : Governance - Email Notification Integration, Pipeline Health Alerts
Run as: ACCOUNTADMIN
- Note: Run each step individually, not all at once

WHY GOVERNANCE?
===============

Without governance:
. Pipeline breaks at 2am -> nobody knows until morning
. Partner needs data -> manual file exports + stale data
. No control over who sees what

With governance:
. Pipeline alert fires immediately + fix before business starts
. Partner queries Live data directly + no CSV exports
. Full control over data access
*/

use database flowbridge_dev_db;
use role accountadmin;
use warehouse flowbridge_pipeline_wh;
use schema gold_sch;

/*
STEP 1 - EMAIL NOTIFICATION INTEGRATION

What   : Connects Snowflake to an email server
Why    : Required before any alert can send emails
         Without this + alerts run but no email sent
Real life: Every enterprise monitoring system needs
a notification channel - email is simplest.
*/


create  notification integration if not exists email_notification_int
type = email
enabled = true
comment = 'Email notification integration for pipeline health alerts.';

select system$start_user_email_verification('RAMASNOWFLAKEUSER123');

call system$send_email(
'email_notification_int',
'ramakrishnasnowflake123@gmail.com',
'flowbridge project - data2crunch',
'there is a error in pipeline please verify.'
);


create or replace alert bronze_sch.pipeline_health_alert
warehouse = flowbridge_pipeline_wh
schedule = '1 minute'
if(exists(
--bronze snowpipe filure alert
select 1 from TABLE(information_schema.copy_history(
table_name => 'bronze_sch.RAW_ORDERS',
start_time => dateadd(hour,-1,current_timestamp())
  ))
  WHERE status = 'LOAD FAILED'
  union all
  --- silver task failure alert
  select 1 from table(information_schema.task_history(
scheduled_time_range_start =>  dateadd(hour,-1,current_timestamp())
  ))
  where state = 'FAILED'
  and database_name = 'flowbridge_dev_db'
  union all 
  -- gold dynamic table failure
  select 1 from table(information_schema.dynamic_table_refresh_history())
  where schema_name = 'GOLD_SCH'
and database_name = 'flowbridge_dev_db'
and state = 'FAILED'
and refresh_start_time = dateadd(minute,-5,current_timestamp())
  
))
then call system$send_email(
'email_notification_int',
'ramakrishnasnowflake123@gmail.com',
'snowpipe success alert.',
'snowpipe has successfully loaded files'
);
alter alert bronze_sch.pipeline_health_alert resume;


  
show alerts in database flowbridge_dev_db;

show tables in database flowbridge_dev_db;

 
select * from table(information_schema.alert_history(
scheduled_time_range_start => dateadd(hour,-1,current_timestamp())
));

show resource monitors;
