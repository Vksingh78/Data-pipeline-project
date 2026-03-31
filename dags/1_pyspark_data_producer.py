from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
import os

# 1. Define the Asset/Dataset URI (Research paper calls this 'Asset-Aware Scheduling')
RAW_DATA_ASSET = Dataset("file://tmp/raw_data.csv")

def generate_mock_data():
    # Simulated PySpark/Python data generation
    data = "id,name,transaction_amount\n1,Amit,500\n2,Priya,1200\n3,Rahul,900\n4,Neha,300"
    
    # Save to a common location accessible by Docker containers
    os.makedirs("/tmp", exist_ok=True)
    with open("/tmp/raw_data.csv", "w") as f:
        f.write(data)
    print("Raw data generated successfully at /tmp/raw_data.csv")

with DAG(
    dag_id="1_pyspark_data_producer",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["college_project", "producer"]
) as dag:

    # 2. When this task finishes, it explicitly updates the RAW_DATA_ASSET
    produce_data_task = PythonOperator(
        task_id="extract_and_save_data",
        python_callable=generate_mock_data,
        outlets=[RAW_DATA_ASSET] # <--- This is the magic word for Asset-Aware scheduling!
    )