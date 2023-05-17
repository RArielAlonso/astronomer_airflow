from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

def lamda_print():
    lambda: print(open('/tmp/dummy', 'rb').read())

with DAG(dag_id='check_dag',
         description='DAG to check data',
         tags=['data_engineering team'],
         start_date=datetime(2023,1,1),
         catchup=False,
         schedule_interval='@daily'
) as dag:
    create_file=BashOperator(
        task_id="create_file",
        bash_command='echo "Hi there!" > /tmp/dummy'
    )
    check_file=BashOperator(
        task_id='check_file',
        bash_command='test -f /tmp/dummy'
    )
    print_file=PythonOperator(
        task_id='print_file',
        python_callable=lamda_print
    )

create_file>>check_file>>print_file