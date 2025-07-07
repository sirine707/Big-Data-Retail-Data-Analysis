from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0
}

with DAG(
    dag_id="extract_schema_dag",
    default_args=default_args,
    schedule_interval=None,  # déclenchement manuel
    catchup=False,
    description="Extract schema from CSV file and save as JSON",
    tags=["schema", "ingestion"]
) as dag:

    CSV_FILE = "/opt/airflow/data/retail_data.csv"

    extract_schema = BashOperator(
        task_id="extract_schema",
        bash_command=f"python /opt/airflow/scripts/extract_schema.py {CSV_FILE}"
    )
