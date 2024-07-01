from pendulum import datetime, duration
from airflow.decorators import dag, task, task_group
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


# Declare Static Variables
TEMPLATE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_{year}-{month}.parquet"
TYPES = ["yellow_tripdata", "green_tripdata", "fhv_tripdata"]
YEARS = ["2024"]
MONTHS = ["01", "02", "03"]
SRC_DIR = "/usr/local/airflow/include/data"


# Set Default Arguements
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}
# Instantiate DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    default_args=default_args,
    catchup=False,
    tags=["nyc_taxi_elt"]
    # include path to look for external files
    # template_searchpath="./include"
)
def nyc_taxi_ELT():
    start = DummyOperator(task_id="Begin")


    # Defining Atomic Task for Downloading a Single File -> Called in Extract Task Group
    @task(task_id="download_file")
    def download_file(year: str, month: str, type: str) -> None:
        import requests

        url = TEMPLATE_URL.format(type=type, year=year, month=month)
        destination = f"{SRC_DIR}/{type}_{year}-{month}.parquet"

        try:
            response = requests.get(url)
            response.raise_for_status()
            print(f"Request succeeded for {type}_{year}-{month}! Status Code: ", response.status_code)
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {type}_{year}-{month}! Status Code: {e}")
        with open(destination, mode="wb") as file:
                file.write(response.content)
        return
    
    # Task group for Extracting Desired Files to Local
    @task_group(group_id="Extract_Group")
    def extract_data_to_local():

        for year in YEARS:
            for month in MONTHS:
                for type in TYPES:
                    download_file(year, month, type)
        return
    
    @task(task_id="Upload_File")
    def upload_file(bucket_name: str, year: str, month: str, type: str) -> None:
        import os 

        local_file_path = f"{SRC_DIR}/{type}_{year}-{month}.parquet"
        s3_key = f"RAW/{type}/{year}/{month}"
        if os.path.exists(local_file_path):
            print(f"File {local_file_path} exists. Proceeding with upload.")
            #with open(local_file_path, 'rb') as f:
            #    file_content = f.read()
            try:
                upload_task = LocalFilesystemToS3Operator(
                    task_id="upload_to_s3",
                    filename=local_file_path,
                    dest_key=s3_key,
                    dest_bucket=bucket_name,
                    aws_conn_id="aws_default",
                    replace=True
                )
                upload_task.execute(context={}) 
                print(f"Uploaded {type} for {year}-{month} to S3")
            except Exception as e:
                print(f"Failed to upload {type} for {year}-{month} to S3: {e}")
        else: print(f"File {local_file_path} doesn't exist!")

        return
    
    @task_group(group_id="Upload_Group")
    def local_to_s3():
        import os

        bucket_name = os.environ.get("S3_BUCKET_NAME")
        for year in YEARS:
            for month in MONTHS:
                for type in TYPES:
                    upload_file(bucket_name, year, month, type)

    # Load Raw Data to Warehouse: Snowflake / AWS Redshift

    # Execute dbt Transformations

    # Finish Dag Run
    end = DummyOperator(task_id="End")

    # Set Task Dependancties
    start >> extract_data_to_local() >> local_to_s3() >> end

    
# Run Dag
nyc_taxi_ELT()