from airflow.decorators import dag, task
from datetime import datetime

@dag(
    'xcom_dag_2',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
)
def xcom_dag_2():

    @task
    def peter_task(ti=None):
        ti.xcom_push(key='mobile_phone', value='iphone')

    @task
    def lorie_task(ti=None):
        ti.xcom_push(key='mobile_phone', value='galaxy')
    
    @task
    def bryan_task(ti=None):
        phones = ti.xcom_pull(task_ids=['peter_task', 'lorie_task'], key='mobile_phone')
        print(phones)

    peter_task() >> lorie_task() >> bryan_task()

xcom_dag_2()