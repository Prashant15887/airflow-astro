from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def cli():

    @task
    def my_task(val):
        # raise AirflowException()
        print(val)
        return 42

    my_task(80)
cli()