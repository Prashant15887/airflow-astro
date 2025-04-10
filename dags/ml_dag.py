from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

def _ml_task(ml_parameter):
    print(ml_parameter)

with DAG(
    'ml_dag',
    start_date = datetime(2025, 1, 1),
    schedule_interval = None,
    catchup = False
) as dag:
    
    ml_tasks = []
    for ml_parameter in Variable.get("ml_model_parameters", deserialize_json=True)["param"]:
        ml_tasks.append(PythonOperator(
            task_id = f"ml_task_{ml_parameter}",
            python_callable = _ml_task,
            op_kwargs = {
                "ml_parameter": ml_parameter
            }
        ))

    report_1 = BashOperator(
        task_id = "report_1",
        bash_command='echo "report_{{ var.value.ml_report_name }}"'
    )

    report_2 = BashOperator(
        task_id = "report_2",
        bash_command='echo "{{ var.json.ml_model_parameters.param }}"'
    )

    ml_tasks >> report_1 >> report_2