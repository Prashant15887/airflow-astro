from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

def print_a():
    print('hi from task a')

def print_b():
    print('hi from task b')

def print_c():
    print('hi from task c')

def print_d():
    print('hi from task d')

def print_e():
    print('hi from task e')

dag = DAG(
    'my_dag', 
    start_date=datetime(2025, 1, 1), 
    description='A simple tutorial DAG', 
    tags=['data_science'], 
    schedule='@daily', 
    catchup=False
    )
    
task_a = PythonOperator(
    task_id='task_a', 
    python_callable=print_a,
    dag=dag,
    retries=3
    )

task_b = PythonOperator(
    task_id='task_b', 
    python_callable=print_b,
    dag=dag,
    retries=3
    )

task_c = PythonOperator(
    task_id='task_c', 
    python_callable=print_c,
    dag=dag,
    retries=3
    )

task_d = PythonOperator(
    task_id='task_d', 
    python_callable=print_d,
    dag=dag,
    retries=3
    )

task_e = PythonOperator(
    task_id='task_e', 
    python_callable=print_e,
    dag=dag,
    retries=3
    )

# chain(task_a, [task_b, task_c], [task_d, task_e])
task_a >> [task_b, task_c]
task_b >> task_d
task_c >> task_e