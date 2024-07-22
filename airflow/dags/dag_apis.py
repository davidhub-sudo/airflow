from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime
from airflow.models import Variable

def _get_api_config():
  app_id = Variable.get("app_id")
  app_key = Variable.get("app_key")
  print(f"app_id: {app_id}")
  print(f"app_key: {app_key}")   


with DAG('dag_apis', 
  start_date=datetime(2023, 1, 1), 
  schedule='@daily', 
  catchup=False):

  downloading_data = BashOperator(
    task_id='downloading_data',
    bash_command='sleep 3',
    do_xcom_push=False
  )

  print_apis = PythonOperator(
    task_id='print_apis',
    python_callable=_get_api_config
  )

downloading_data >> print_apis 