from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ensure src/ is on path
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from ml_pipeline.sqs_publisher import load_and_send_to_sqs

default_args = {"owner": "airflow", "retries": 1}

def sqs_wrapper():
    load_and_send_to_sqs(queue_name="ml_pipeline_queue")

with DAG(
    dag_id="msg_sqs_pipeline",
    default_args=default_args,
    description="Pipeline: Load test data and send messages to SQS",
    schedule=None, 
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    send_messages_task = PythonOperator(
        task_id="publish_to_sqs",
        python_callable=sqs_wrapper 
    )