from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys, os, json

# Ensure src/ is on path
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from ml_pipeline.data import generate_data, load_data
from ml_pipeline.model import train_model, evaluate_model, promote_model

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="Pipeline: Generate Data -> Train Model -> Evaluate Model -> Promote Model",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def generate_data_wrapper():
        return generate_data()
        
    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data_wrapper
        )

    def train_model_wrapper(data_path: str, model_path: str):
        df = load_data(data_path)
        return train_model(df, model_path)
        
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model_wrapper,
        op_kwargs={
            "data_path": "data/breast_cancer.csv",
            "model_path": "models/breast_cancer_model.pkl",
        })
        
    def evaluate_model_wrapper(data_path: str, model_path: str):
        df = load_data(data_path)
        return evaluate_model(df, model_path)
        
    evaluate_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model_wrapper,
        op_kwargs={
                    "data_path": "data/breast_cancer.csv",
                    "model_path": "models/breast_cancer_model.pkl",
    })
    
    def promote_model_wrapper(s3: str = "s3://async-ai-inf/models/", base_path: str = "models", threshold: float = 0.94):

        metadata_path = f"{base_path}/metadata.json"
    
        with open(metadata_path, "r") as f:
            data = json.load(f)
    
        candidates = [m for m in data if m["accuracy"] >= threshold]
        
        if not candidates:
            raise ValueError("No models meet the accuracy threshold")
    
        
        latest_model = max(candidates, key=lambda m: m["model_version"])
        model_version = latest_model["model_version"]
    
        promote_model(model_version=model_version, s3=s3, base_path=base_path)
        
    promote_task = PythonOperator(
        task_id="promote_model",
        python_callable=promote_model_wrapper,
        op_kwargs={
            "s3": "s3://async-ai-inf/models/",
            "base_path": "models"
        }
    )

    generate_task >> train_task >> evaluate_task >> promote_task