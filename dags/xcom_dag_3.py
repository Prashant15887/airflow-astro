from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import pendulum

@dag(
   'xcom_dag_3',
   schedule = None,
   start_date = pendulum.datetime(2025,1,1),
   catchup = False
)
def xcom_dag_3():

    @task
    def _transform(ti):
        import requests
        resp = requests.get('https://swapi.dev/api/people/1').json()
        print(resp)
        my_character = {}
        my_character["height"] = int(resp["height"]) - 20
        my_character["mass"] = int(resp["mass"]) - 50
        my_character["hair_color"] = "black" if resp["hair_color"] == "blond" else "blond"
        my_character["eye_color"] = "hazel" if resp["eye_color"] == "blue" else "blue"
        my_character["gender"] = "female" if resp["gender"] == "male" else "female"
        ti.xcom_push("character_info", my_character)

    @task
    def _transform2(ti):
        import requests
        resp = requests.get(f'https://swapi.dev/api/people/2').json()
        print(resp)
        my_character = {}
        my_character["height"] = int(resp["height"]) - 50
        my_character["mass"] = int(resp["mass"]) - 20
        my_character["hair_color"] = "burgundy" if resp["hair_color"] == "blond" else "brown"
        my_character["eye_color"] = "green" if resp["eye_color"] == "blue" else "black"
        my_character["gender"] = "male" if resp["gender"] == "male" else "female"
        ti.xcom_push("character_info", my_character)

    @task
    def _load(values):
        print(values)

    [_transform(), _transform2()] >> _load(["{{ ti.xcom_pull(task_ids=['_transform','_transform2'], key='character_info') }}"])

xcom_dag_3()
