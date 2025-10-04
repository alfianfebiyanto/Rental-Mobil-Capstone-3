from airflow import DAG
from airflow.operators.empty import EmptyOperator  # DummyOperator (deprecated)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from insert_data import create_table, load_all, insert_batch

# Default arguments
default_args = {
    "owner": "alfian",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definisi DAG
with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="DAG sederhana buat testing",
    schedule_interval="@daily",   # jalan tiap hari
    start_date=datetime(2025, 10, 1),
    catchup=False,  # biar gak nge-run backlog
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    task_insert_batch = PythonOperator(
        task_id="insert_batch",
        python_callable=insert_batch
    )

    task_load_all = PythonOperator(
        task_id="load_all",
        python_callable=load_all
    )

    end = EmptyOperator(task_id="end")

    # Definisi urutan task (pipeline)
    start >> task_create_table >> task_insert_batch >> task_load_all >> end
