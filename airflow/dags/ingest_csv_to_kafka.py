from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer
import json

CSV_PATH = "/opt/airflow/data/retail_data.csv"  # 📂 Monte ce volume dans docker-compose
TOPIC_NAME = "csv-data-topic"
BOOTSTRAP_SERVERS = "kafka:9092"

def send_csv_to_kafka():
    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        # else:
        #     print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    try:
        df = pd.read_csv(CSV_PATH)
        producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

        for _, row in df.iterrows():
            message = json.dumps(row.to_dict())
            while True:
                try:
                    producer.produce(TOPIC_NAME, value=message, callback=delivery_report)
                    break
                except BufferError:
                    # Buffer plein, on attend que le producteur traite les messages
                    producer.poll(0.1)

            producer.poll(0)  # Permet de traiter les callbacks

        producer.flush()
    except Exception as e:
        print("❌ ERROR:", e)
        raise
with DAG(
    dag_id="send_csv_to_kafka",
    start_date=datetime(2025, 6, 13),
    schedule_interval=None,
    catchup=False,
) as dag:

    send_task = PythonOperator(
        task_id="send_csv_rows_to_kafka",
        python_callable=send_csv_to_kafka
    )
