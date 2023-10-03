from datetime import datetime
from random import randint

from airflow import DAG
from airflow.operators.python import PythonOperator


def _training_model(model):
    print(model)
    return randint(1, 10)


dag_id_var = "my_dag_var"
with DAG(dag_id="copy_my_dag_var", start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    training_model_tasks = PythonOperator(
        task_id=f"training_model_A", python_callable=_training_model, op_kwargs={"model": "A"}
    )
    training_model_tasks
