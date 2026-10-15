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

