# test_module.py


from airflow.decorators import dag, task

import pendulum

from my_packages.packages_a.module_a import TestClass


@dag(schedule = None, start_date = pendulum.datetime(2025, 3, 1), catchup = False)

def test_module():

 

   @task

   def test_task():

       print(TestClass.my_time())

 

   test_task()


dag = test_module()