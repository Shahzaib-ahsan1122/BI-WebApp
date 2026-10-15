--creating the database;
create database if not exists ecom_pipeline;
create schema if not exists ecom_pipeline.public;
use schema ecom_pipeline.public;

use role accountadmin;
grant usage on integration s3_init_integration to role accountadmin;


--creating the storage INTEGRATION.
create or replace  storage integration s3_init_integration
type = external_stage 
storage_provider = 'S3'
enabled= TRUE
storage_allowed_locations = ('s3://web-medallion-bucket/api-events/')
storage_aws_role_Arn = 'arn:aws:iam::552496640844:role/SnowflakeS3AccessRole';


--creating file format 
create or replace file format json__file_format
TYPE = 'JSON'
strip_outer_array = true;

--creating stage pointing to the s3 bucket
CREATE OR REPLACE STAGE s3_checkout_stage
  STORAGE_INTEGRATION = s3_init_integration
  URL = 's3://web-medallion-bucket/api-events/';

list @s3_checkout_stage;


describe integration s3_init_integration;
show storage integrations;


--time fo rthe medallion architecture 
create or replace table bronze_checkout_events(
raw_payload variant,
ingested_at timestamp_ntz default current_timestamp()
);

-- for faster backfill
copy into bronze_checkout_events(raw_payload)
from @s3_checkout_stage
file_format = (type = 'JSON' strip_outer_array = true);

select* from bronze_checkout_events;
--describe table bronze_checkout_events;


--creating the snowpipe (it will oad new files from s3 storage )
create or replace pipe pipe_checkout_events
auto_ingest = true as
copy into bronze_checkout_events(raw_payload)
from @s3_checkout_stage
file_format = (type = 'JSON' strip_outer_array = true);

--show pipes; => to see the creation of the snowpipes
--alter pipe pipe_checkout_events refresh;

-- creating the silver layer of the medallion architecture
create or replace dynamic table silver_checkout_events
target_lag = '1 minute'
warehouse = compute_wh
as 
select
    raw_payload:event_id::string as event_id,
    raw_payload:user_id::int as user_id,
    raw_payload:action::string as action,
    raw_payload:item::string as item,
    raw_payload:amount::float as amount,
    raw_payload:timestamp::timestamp_ntz as event_time,
    ingested_at
from bronze_checkout_events;

select* from silver_checkout_events;


--creating gold layer for the medallion architecture 

--1. overall revenue
create or replace dynamic table gold_product_performance
target_lag ='1 minute'
warehouse = compute_wh
as
select 
    item as product_name,
    count(distinct event_id) as total_orders,
    sum(amount) as total_revenue,
    avg(amount) as avg_order_value,
    max(event_time) as last_purchase_at
from silver_checkout_events
group by item;

select * from gold_product_performance;

--2. customer lifetime value (ltv)  * top spenders:
create or replace dynamic table gold_customer_kpi
target_lag = '1 minute'
warehouse = compute_wh 
as
select
    user_id,
    count(distinct event_id) as total_orders,
    sum(amount) as total_spent,
    avg(amount) as avg_basket_size,
    min(event_time) as first_purchase_at,
    max(event_time) as last_purchase_at
from silver_checkout_events 
group by user_id;

select * from gold_customer_kpi;


-- adding some limits fo rthe warehouse :
alter warehouse compute_wh set
    warehouse_size ='xsmall'
    auto_suspend = 60;
    auto_resume = true;

show warehouses like 'compute_wh';

--testing the end-end pipeline live
select * from bronze_checkout_events 
order by ingested_at desc limit 1 ;

--==================== creating system alerts =====================

--1.invalid amount ALERT
create or replace alert alert_invalid_order_amount
    schedule ='1 minute'
    if (
    exists(
        select 1 from silver_checkout_events
        where amount <= 0
     )
    )
    then 
       CALL SYSTEM$LOG_ERROR('Data Quality ALert: Invalid order amount detected in silver_checkout_events!');


--2. Ingestion ALERT

create or replace alert alert_ingestion_stalled
    schedule = '5 minute'
    if (
        exists(
            select 1
            from bronze_checkout_events
            having max(ingested_at) < dateadd('minute', -15, current_timestamp())
        )
    )
    then
    CALL system$log_error('Ingestion Alert: No new events ingested into the bronze in the last 15 minutes!');

--3.Schema & payload completeness
create or replace alert alert_missing_mandatory_fields
    schedule = '1 minute'
    if(
      exists (
            select 1 from silver_checkout_events
            where event_id is null or user_id is null or iten is null 
      )
    )
    then 
    CALL system$og_error('Data Quality Alert: Missing mandatory fields detected in silver layer!');


    --4.Duplicate values alert
    create or replace alert alert_duplicate_events
        schedule = '1 minute'
        if (
            exists (
                select event_id, count(*)
                from silver_checkout_events
                group by event_id
                having count(*) > 1
            )
        )
        then 
        CALL system$log_error('Data Quality Alert: Duplicate event_ids detected in Silver layer!');

-- production pattern: quarantine table(Dead Letter Qeue)this will stop the pipeline if there are any alerts.
create or replace view silver_checkout_events_DlQ as
select
    raw_payload,
    ingested_at,
    case
        when raw_payload:event_id is null then 'Missing event_id'
        when raw_payload:user_id is null then 'Missing user_id'
        when raw_payload:amount:: float <= 0 then 'Invalid Amount'
        else 'Unknown error'
    end as rejection_reason
from bronze_checkout_events
where raw_payload:event_id is null
    or raw_payload:user_id is null
    or raw_payload:amount::float <= 0;


    -- TO ACTIVATE THE ALERTS BECAUSE THERE ARE IN THE SUSPENDED STATE BY DEFAULT
    alter alert alert_invalid_order_amount resume;
    alter alert alert_ingestion_stalled resume;
    alter alert alert_duplicate_events resume;
    alter alert alert_missing_mandatory_fields resume;


show alerts;
  