from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'spark_process',
    default_args=default_args,
    description='Traitement Spark',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Paramètres adaptés à ton projet
kafka_bootstrap_servers = 'kafka:9092'
kafka_topic = 'csv-data-topic'
output_dir = '/opt/airflow/data/output_es'
es_host = 'http://elasticsearch:9200'
es_index = 'rettail_data'

spark_processing_es = BashOperator(
    task_id='spark_processing_es',
    bash_command=(
        'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 '
        '/opt/airflow/scripts/process_with_spark.py '
        f'{kafka_bootstrap_servers} {kafka_topic} {output_dir} {es_host} {es_index}'
    ),
    dag=dag,
)