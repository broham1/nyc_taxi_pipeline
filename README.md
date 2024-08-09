# NYC Taxi ELT Pipeline
This project is an ELT pipeline that extracts files from the <a href="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" target="_blank">TLC Trip Record Data Website</a>, loads it into S3 (which is staged to Snowflake), and then runs dbt transformations on the staged data for dimensional modeling and reporting. The reporting models are then used to create a dashboard in Metabase. The goal here to learn about the tools used in modern data engineering.

## Prerequisites:
- AWS Account (offers free tiers - credit card required)
- Snowflake Account (offers free trial - no credit card required)
- Astro CLI (<a href="https://www.astronomer.io/docs/astro/cli/install-cli" target="_blank">directions for download</a>)
- Docker (<a href="https://docs.docker.com/get-docker/" target="_blank">directions for download</a>)

### AWS:
You will need to have an AWS account and create an s3 bucket. The bucket you create will need to be synced to snowflake as a stage, which can be done by following snowflake's guide <a href="https://docs.snowflake.com/en/user-guide/tables-external-s3" target="_blank">here</a>. That guide will also show you how to set up SQS notifications on said bucket so that your external snowflake table can auto-refresh everytime new data is added to the bucket, which you should do after setting up your snowflake project environment. You will also need give airflow credentials to access your S3 bucket. I downloaded access keys for my account as a csv and copy-pasted the keys into airflow_settings.yaml. 

### Snowflake:
After configuring your accounts in AWS and Snowflake, you can run these two notebookes to setup your snowflake environment and create the necessary resources for the project. **Note:** The Taxi Zone Lookup table requires you to manually load the CSV into the snowflake table of the same name. You can download it <a href="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" target="_blank">here</a>.

#### Snowflake System Setup Notebook:
```
USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE dbt_DEV_ROLE COMMENT='dbt_DEV_ROLE';
GRANT ROLE dbt_DEV_ROLE TO ROLE SYSADMIN;

CREATE OR REPLACE USER dbt_USER PASSWORD='REPLACE WITH YOUR PASSWORD'
	DEFAULT_ROLE=dbt_DEV_ROLE
	DEFAULT_WAREHOUSE=dbt_WH
	COMMENT='dbt User';
    
GRANT ROLE dbt_DEV_ROLE TO USER dbt_USER;

USE ROLE ACCOUNTADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE dbt_DEV_ROLE;

USE ROLE SYSADMIN;

CREATE OR REPLACE WAREHOUSE dbt_DEV_WH
  WITH WAREHOUSE_SIZE = 'SMALL' 
  AUTO_SUSPEND = 120
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

GRANT ALL ON WAREHOUSE dbt_DEV_WH TO ROLE dbt_DEV_ROLE;
USE ROLE dbt_DEV_ROLE;

CREATE OR REPLACE DATABASE dbt_DEV_DB;
GRANT ALL ON DATABASE dbt_DEV_DB TO ROLE dbt_DEV_ROLE;

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'INSERT YOUR AWS ROLE ARN'
  STORAGE_ALLOWED_LOCATIONS = ('INSERT YOUR ALLOWED S3 LOCATIONS');

DESC INTEGRATION s3_int;

GRANT USAGE ON INTEGRATION s3_int TO ROLE dbt_dev_role;

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = 'PARQUET'

CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    COMPRESSION = 'AUTO'
    SKIP_HEADER = 1;


GRANT USAGE ON FILE FORMAT my_parquet_format TO ROLE dbt_dev_role;
GRANT USAGE ON FILE FORMAT my_csv_format TO ROLE dbt_dev_role;
```
#### Project Setup Notebook:
```
USE ROLE dbt_dev_role;

USE WAREHOUSE dbt_dev_wh;

CREATE OR REPLACE SCHEMA DBT_DEV_DB.nyc_taxi_schema;

CREATE OR REPLACE STAGE DBT_DEV_DB.NYC_TAXI_SCHEMA.YELLOW_TRIPDATA
  STORAGE_INTEGRATION = s3_int
  URL = 'INSERT YOUR S3 PATH/nyc_taxi/raw/yellow_tripdata'
  FILE_FORMAT = my_parquet_format;

CREATE OR REPLACE STAGE DBT_DEV_DB.NYC_TAXI_SCHEMA.GREEN_TRIPDATA
  STORAGE_INTEGRATION = s3_int
  URL = 'INSERT YOUR S3 PATH/nyc_taxi/raw/green_tripdata'
  FILE_FORMAT = my_parquet_format;


CREATE OR REPLACE TABLE DBT_DEV_DB.NYC_TAXI_SCHEMA.TAXI_ZONE_LOOKUP (
    LocationID INT,
    Borough VARCHAR(15),
    Zone VARCHAR(50),
    service_zone VARCHAR(15)
)

UPDATE DBT_DEV_DB.NYC_TAXI_SCHEMA.TAXI_ZONE_LOOKUP
SET 
    BOROUGH = LTRIM(RTRIM(BOROUGH, '"'), '"'),
    ZONE = LTRIM(RTRIM(ZONE, '"'), '"'),
    SERVICE_ZONE = LTRIM(RTRIM(SERVICE_ZONE, '"'), '"')


CREATE OR REPLACE EXTERNAL TABLE DBT_DEV_DB.NYC_TAXI_SCHEMA.yellow_tripdata_ext (
    vendorID INT AS (value:VendorID::INT),
    tpep_pickup_datetime TIMESTAMP_NTZ AS to_timestamp_ntz((value:tpep_pickup_datetime::VARCHAR)),
    tpep_dropoff_datetime TIMESTAMP_NTZ AS to_timestamp_ntz((value:tpep_dropoff_datetime::VARCHAR)),
    trip_distance FLOAT AS (value:trip_distance::FLOAT),
    PULocationID INT AS (value:PULocationID::INT),
    DOLocationID INT AS (value:DOLocationID::INT),
    passenger_count INT AS (value:passenger_count::INT),
    ratecodeID INT AS (value:RatecodeID::INT),
    store_and_fwd_flag BOOLEAN AS to_boolean((value:store_and_fwd_flag::VARCHAR)),
    payment_type INT AS (value:payment_type::INT),
    fare_amount FLOAT AS (value:fare_amount::FLOAT),
    extra FLOAT AS (value:extra::FLOAT),
    mta_tax FLOAT AS (value:mta_tax::FLOAT),
    improvement_surcharge FLOAT AS (value:improvement_surcharge::FLOAT),
    tip_amount FLOAT AS (value:tip_amount::FLOAT),
    tolls_amount FLOAT AS (value:tolls_amount::FLOAT),
    total_amount FLOAT AS (value:total_amount::FLOAT),
    congestion_surcharge FLOAT AS (value:congestion_surcharge::FLOAT),
    airport_fee FLOAT AS (value:airport_fee::FLOAT)
)
LOCATION = @YELLOW_TRIPDATA
FILE_FORMAT = my_parquet_format
AUTO_REFRESH = TRUE;

CREATE OR REPLACE EXTERNAL TABLE DBT_DEV_DB.NYC_TAXI_SCHEMA.green_tripdata_ext (
    vendorID INT AS (value:VendorID::INT),
    lpep_pickup_datetime TIMESTAMP_NTZ AS to_timestamp_ntz((value:lpep_pickup_datetime::VARCHAR)),
    lpep_dropoff_datetime TIMESTAMP_NTZ AS to_timestamp_ntz((value:lpep_dropoff_datetime::VARCHAR)),
    trip_distance FLOAT AS (value:trip_distance::FLOAT),
    PULocationID INT AS (value:PULocationID::INT),
    DOLocationID INT AS (value:DOLocationID::INT),
    passenger_count INT AS (value:passenger_count::INT),
    ratecodeID INT AS (value:RatecodeID::INT),
    store_and_fwd_flag BOOLEAN AS to_boolean((value:store_and_fwd_flag::VARCHAR)),
    payment_type INT AS (value:payment_type::INT),
    fare_amount FLOAT AS (value:fare_amount::FLOAT),
    extra FLOAT AS (value:extra::FLOAT),
    mta_tax FLOAT AS (value:mta_tax::FLOAT),
    improvement_surcharge FLOAT AS (value:improvement_surcharge::FLOAT),
    tip_amount FLOAT AS (value:tip_amount::FLOAT),
    tolls_amount FLOAT AS (value:tolls_amount::FLOAT),
    total_amount FLOAT AS (value:total_amount::FLOAT),
    trip_type INT AS (value:trip_type::INT)
)
LOCATION = @GREEN_TRIPDATA
FILE_FORMAT = my_parquet_format
AUTO_REFRESH = TRUE;
```

## Running the Pipeline:
- Clone the repo into your project directory - type in terminal:
```
git clone git@github.com:broham1/nyc_taxi_pipeline.git
```
- Create your own .env and airflow_settings.yaml files:
```
touch .env && touch airflow_settings.yaml
```
- Populate your .env and airflow_settings.yaml files with your information:
```
# .env file
PROTOCOL_BUFFERS_PYTHON_IMPLMENTATION=python
SNOWFLAKE_ACCOUNT=YOUR_SNOWFLAKE_ACCOUNT
SNOWFLAKE_PASSWORD=YOUR_SNOWFLAKE_PASSWORD
S3_BUCKET_NAME=YOUR_S3_BUCKET_NAME
```
**Note:** SNOWFLAKE_ACCOUNT requires this format: Organization-Account
```
# airflow_settings.yaml
airflow:
  connections:
    - conn_id: aws_conn
      conn_type: aws
      conn_host:
      conn_schema:
      conn_login: YOUR_AWS_ACCESS_KEY_ID
      conn_password: YOUR_AWS_SECRET_ACCESS_KEY
      conn_port:
      conn_extra:
  pools:
    - pool_name:
      pool_slot:
      pool_description:
  variables:
    - variable_name:
      variable_value:
```
- Starting containers - type in terminal:
```
astro dev start
```
- Stopping containers - type in terminal:
 - type in terminal:
```
astro dev stop
```
- Removing the volumes and containers - type in terminal:
```
astro dev kill
```
- Go to http:localhost/8080 (username & password both are admin)
- Trigger nyc_taxi_ELT manually (optionally could be done in terminal)
- Create your own dashboard in metabase at http:localhost/3000

## Metabase Dashboard:
Here is the Metabase dashboard I made with the reporting tables.
![dashboard](taxi_dashboard.png)

## Issues:
Image and container size were an issue for my old laptop. This would result in an astro error saying webserver container failed healthcheck, denying access to airflow UI or metabase until it was healthy again. This would also result in the credentials in airflow_settings.yaml failing to be passed appropriately, so you would have to manually enter them in the airflow UI under connections. This may not be an issue for you if you have better specs than me (2 cores/8gb ram) as you could give docker access to more resources if needed. Another issue, possibly related, is it took the metabse quite a while to load up sometimes.

## Conclusion:
Doing this project allowed me to learn about snowflake, dbt, and dimensional modeling. I wanted to explore data quality tests with dbt, but I realized that since this dataset had a lot of errors in it, the pipeline would not run, so I dropped it. Another thing I wanted was for this pipeline to be run on a schedule, but since the TLC data doesn't have a strict update schedule, I decided to just make it a manual pipeline. My code is also pretty rough, so I could improve on that front as well. In the future, I'd like to follow best practices in regards to credentials and roles/secrets manager over hard-coded credentials in .env file.
