from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Fungsi sederhana buat dites
def print_hello():
    print("🚀 Hello World from Airflow DAG!")

# Default arguments
default_args = {
    "owner": "alfian",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
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

    task1 = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello
    )

    task1
