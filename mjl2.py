from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hey I am MJL'

dag = DAG('mjl2', description='Hello from MJL2',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
