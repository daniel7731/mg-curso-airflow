from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["basic"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command='echo "Hola desde Airflow 🛫"',
    )

    end = BashOperator(
        task_id="end",
        bash_command='echo "DAG finalizado "',
    )

    start >> end