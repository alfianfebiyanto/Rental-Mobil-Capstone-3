from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# import fungsi dari package src
from src.insert_data import create_tables, insert_data

default_args = {
    "owner": "alfian",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_dag",
    description="DAG testing produk ini",
    schedule_interval="@daily",      # ok, atau ganti 'schedule' di Airflow baru
    start_date=datetime(2025, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    task_insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    end = EmptyOperator(task_id="end")

    # pipeline
    start >> task_create_tables >> task_insert_data >> end
