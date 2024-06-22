from pendulum import datetime, duration
from airflow.decorators import dag, task, task_group
from airflow.operators.dummy import DummyOperator

TEMPLATE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_{year}-{month}.parquet"
TYPES = ["yellow_tripdata", "green_tripdata", "fhv_tripdata"]
YEARS = ["2024"]
MONTHS = ["01", "02", "03"]
SRC_DIR = "/usr/local/airflow/include/data"

# Set default arguements
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
    catchup=False
    # include path to look for external files
    # template_searchpath="./include"
)
def nyc_taxi_ELT():
    start = DummyOperator(task_id="Begin")

    @task(task_id="Extract")
    def extract_data_to_local():
        import requests
        import logging
        import time
        for year in YEARS:
            for month in MONTHS:
                for type in TYPES:
                    url = TEMPLATE_URL.format(type=type, year=year, month=month)
                    destination = f"{SRC_DIR}/{type}_{year}-{month}.parquet"
                    try:
                        time.sleep(5)
                        response = requests.get(url)
                        response.raise_for_status()
                        logging.info("Request succeeded! Status Code: ", response.status_code)
                        with open(destination, mode="wb") as file:
                            file.write(response.content)
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Request failed! Status Code: {e}")
                        raise
        return
    
    # Load Raw Data to S3 bucket



    # Load Raw Data to AWS Redshift

    # Execute dbt Trasformations

    # Finish dag run
    end = DummyOperator(task_id="End")

    # Set task dependancties
    start >> extract_data_to_local() >> end
    
# Run dag
nyc_taxi_ELT()