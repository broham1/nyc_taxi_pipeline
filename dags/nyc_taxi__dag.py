from pendulum import datetime, duration
import os
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from include.dbt.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG, EXECUTION_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

# Declare Static Variables
TEMPLATE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_{year}-{month}.parquet"
TYPES = ["yellow_tripdata", "green_tripdata"]
YEARS = ["2024"]
MONTHS = ["01", "02", "03", "04", "05"]
SRC_DIR = os.environ["HOME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
PREFIX = "nyc_taxi"

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
    # template_searchpath="./include"
)
def nyc_taxi_ELT():
    start = DummyOperator(task_id="Begin")

    # Create a directory to reference and to store downloaded files
    @task
    def create_directory():
        os.makedirs(f"{SRC_DIR}/data", exist_ok=True)

    # Defining atomic task for downloading a single file -> called in Extract task Ggroup
    @task(task_id="extract_file")
    def download_file(year: str, month: str, type: str) -> None:
        import requests

        url = TEMPLATE_URL.format(type=type, year=year, month=month)
        destination = f"{SRC_DIR}/data/{type}_{year}-{month}.parquet"

        try:
            response = requests.get(url)
            response.raise_for_status()
            print(f"Request succeeded for {type}_{year}-{month}! Status Code: ", response.status_code)
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {type}_{year}-{month}! Status Code: {e}")
        with open(destination, mode="wb") as file:
                file.write(response.content)
        
    
    # Task group for Extracting desired files to local
    @task_group(group_id="Extract_To_Local_Group")
    def extract_data_to_local() -> None:
        for year in YEARS:
            for month in MONTHS:
                for type in TYPES:
                    download_file(year, month, type)
    
    
    # Atomic task for uploading a single file -> called in upload task group
    @task(task_id="upload_file_to_s3")
    def upload_file(year: str, month: str, type: str) -> None:
        import os 

        local_file_path = f"{SRC_DIR}/data/{type}_{year}-{month}.parquet"
        s3_key = f"{PREFIX}/raw/{type}/{year}/{month}"
        if os.path.exists(local_file_path):
            print(f"File {local_file_path} exists. Proceeding with upload.")
            try:
                upload_task = LocalFilesystemToS3Operator(
                    task_id="upload_to_s3",
                    filename=local_file_path,
                    dest_key=s3_key,
                    dest_bucket=S3_BUCKET_NAME,
                    aws_conn_id="aws_conn",
                    replace=True,
                )
                upload_task.execute(context={}) 
                print(f"Uploaded {type} for {year}-{month} to S3")
            except Exception as e:
                print(f"Failed to upload {type} for {year}-{month} to S3: {e}")
        else: print(f"File {local_file_path} doesn't exist!")
    
    
    # Task group for uploading all files to s3
    @task_group(group_id="S3_Upload_Group")
    def local_to_s3() -> None:
        for year in YEARS:
            for month in MONTHS:
                for type in TYPES:
                    upload_file(year, month, type)

    # Execute dbt dimensional modeling
    dbt_modeling = DbtTaskGroup(group_id="dbt_modeling", 
                project_config=DBT_PROJECT_CONFIG, 
                profile_config=DBT_CONFIG,
                execution_config=EXECUTION_CONFIG,
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_LS,
                    select=["path:models/staging", "path:models/intermediate", "path:models/marts"]
                ))
    
    # Execute dbt models for reporting
    dbt_reporting = DbtTaskGroup(group_id="dbt_reporting", 
                project_config=DBT_PROJECT_CONFIG, 
                profile_config=DBT_CONFIG,
                execution_config=EXECUTION_CONFIG,
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_LS,
                    select=["path:models/report"]
                ))

    # Finish Dag Run
    end = DummyOperator(task_id="End")

    # Set Task Dependancties
    start >> create_directory() >> extract_data_to_local() >> local_to_s3()  >> dbt_modeling >> dbt_reporting >> end

# Run Dag
nyc_taxi_ELT()