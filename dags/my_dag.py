from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'retries': 3,
}

def print_a():
    print('hi from task a')

def print_b():
    print('hi from task b')

with DAG(
    dag_id='my_dag',
    description="A simple Tutorial DAG",
    tags=['data_science'],
    start_date=datetime(2023,5,1),
    default_args=default_args,
    schedule="@daily",
    catchup=False
    ) as dag:
    
    task_a=PythonOperator(
        task_id='task_a',
        python_callable=print_a
    )
    task_b=PythonOperator(
        task_id='task_b',
        python_callable=print_a
    )

task_a>>task_b