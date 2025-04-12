from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def example_task_basic(current_dag_run_date):
    print(current_dag_run_date)

with DAG(
    'example_dag_basic',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    tags=['backfill-trigger-cli'],
    catchup=False,
) as dag:
    
    PythonOperator(
        task_id="extract_task",
        python_callable=example_task_basic,
        op_kwargs={
        "current_dag_run_date": "{{ds}}"
        }
    )