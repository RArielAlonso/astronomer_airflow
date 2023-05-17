from airflow.decorators import dag, task
from datetime import datetime
from airflow.utils.helpers import chain


@dag(start_date=datetime(2023, 1 , 1),
     description='A simple tutorial DAG',
     tags=['data_science'],
     schedule='@daily',
     catchup=False)
def dag_with_taskflow_api_dependencies():

    @task
    def print_a():
        print('hi from task a')
    
    @task
    def print_b():
        print('hi from task b')

    @task
    def print_c():
        print('hi from task c')

    @task
    def print_d():
        print('hi from task d')

    @task
    def print_e():
        print('hi from task e')


    print_a() >> print_b() >> print_c() >> print_d() >> print_e()

dag_with_taskflow_api_dependencies()