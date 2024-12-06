-- Use role
use role accountadmin;

-- Creation of database
create or replace database snowpipe_demo;

-- For Create of table 
create or replace table orders_data_landingzone(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(30),
    order_date date
);

-- For creation of a Cloud Storage Integration in Snowflake
create or replace storage integration gcs_bucket_read_int
 type = external_stage
 storage_provider = gcs
 enabled = true
 storage_allowed_locations = ('gcs://snowpipe_project_demo/');

-- Retrieve the Cloud Storage Service Account for your snowflake account
desc storage integration gcs_bucket_read_int;

-- Creation of stage in snowflake
create or replace stage snowpipe_stage
  url = 'gcs://snowpipe_project_demo/'
  storage_integration = gcs_bucket_read_int;

-- Show stages
show stages;

-- Creation of PUB-SUB Topic and Subscription
-- gsutil notification create -t snowpipe_pubsub_topic -f json gs://snowpipe_project_demo/ --> -t refers to topic name, -f refers to filetype

-- Creation of notification integration
create or replace notification integration notification_from_pubsub_int
 type = queue
 notification_provider = gcp_pubsub
 enabled = true
 gcp_pubsub_subscription_name = 'projects/global-momento-430210-c8/subscriptions/snowpipe_pubsub_topic-sub';

-- Describe integration for getting service account
desc integration notification_from_pubsub_int;


-- Creation of Snow Pipe
Create or replace pipe gcs_to_snowflake_pipe
auto_ingest = true
integration = notification_from_pubsub_int
as
copy into orders_data_landingzone
from @snowpipe_stage
file_format = (type = 'CSV');

-- Show pipes
show pipes;

-- Check the status of pipe
select system $pipe_status('gcs_to_snowflake_pipe');

-- Check the history of ingestion
Select * 
from table(information_schema.copy_history(table_name=>'orders_data_landingzone', start_time=> dateadd(hours, -1, current_timestamp())));

-- Terminate a pipe
drop pipe gcs_snowpipe;