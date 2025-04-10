from airflow.decorators import dag, task
from datetime import datetime

@dag(
    'xcom_dag_1',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
)
def xcom_dag_1():

    @task
    def prashant_task(ti=None):
        return 'iphone'
    
    @task
    def keerti_task(mobile_phone):
        print(mobile_phone)

    keerti_task(prashant_task())

xcom_dag_1()