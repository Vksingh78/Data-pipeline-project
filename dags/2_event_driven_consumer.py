from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
import pandas as pd

RAW_DATA_ASSET = Dataset("file://tmp/raw_data.csv")

def quality_and_transform():
    print("Reading 100MB data...")
    # Pandas will read the data easily
    df = pd.read_csv("/tmp/raw_data.csv")
    
    # 1. SHIFT-LEFT DATA QUALITY CHECK
    print("Running Data Quality Check...")
    null_count = df['transaction_amount'].isnull().sum()
    
    if null_count > 0:
        raise ValueError(f"Data Quality Check Failed! Found {null_count} missing transaction amounts. Stopping pipeline.")
    
    print("Data Quality Passed! Proceeding with transformation...")
    
    # 2. TRANSFORMATION (Adding 18% Tax)
    df['tax_amount'] = df['transaction_amount'] * 0.18
    df.to_csv("/tmp/processed_data_output.csv", index=False)
    print("Data Transformed and saved!")

with DAG(
    dag_id="2_event_driven_consumer",
    start_date=datetime(2023, 1, 1),
    schedule=[RAW_DATA_ASSET],
    catchup=False,
    tags=["college_project", "consumer", "pandas"]
) as dag:

    process_data_task = PythonOperator(
        task_id="quality_check_and_transform",
        python_callable=quality_and_transform,
    )