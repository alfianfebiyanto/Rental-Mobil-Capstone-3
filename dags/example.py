from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- fungsi python sederhana ---
def hello_world():
    print("👋 Halo Fian! Airflow DAG kamu jalan sukses 🚀")

# --- default arguments ---
default_args = {
    "owner": "alfian",
    "retries": 1,
}

# --- definisi DAG ---
with DAG(
    dag_id="example_simple_dag",
    description="DAG sederhana untuk testing Airflow",
    schedule_interval="@daily",           # jalan tiap hari
    start_date=datetime(2025, 10, 1),
    catchup=False,                        # biar gak nge-run backlog
    default_args=default_args,
    tags=["test", "simple"],
) as dag:

    # --- task start ---
    start = EmptyOperator(task_id="start")

    # --- task python ---
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )

    # --- task end ---
    end = EmptyOperator(task_id="end")

    # --- urutan eksekusi ---
    start >> say_hello >> end
