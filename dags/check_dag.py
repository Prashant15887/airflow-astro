from airflow.decorators import dag, task
from datetime import datetime

@dag(
    'check_dag', 
    start_date=datetime(2025, 1, 1), 
    description='DAG to check data', 
    tags=['data_engineering'], 
    schedule='@daily', 
    catchup=False,
    #default_args=default_args
    )
def check_dag():

    @task.bash
    def create_file():
        return 'echo "Hi there!" > /tmp/dummy'

    @task.bash
    def check_file():
        return 'test -f /tmp/dummy'

    @task
    def read_file():
        print(open('/tmp/dummy', 'rb').read())

    create_file() >> check_file() >> read_file()

check_dag()