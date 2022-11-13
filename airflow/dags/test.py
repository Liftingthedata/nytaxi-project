from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import time

dag_name = 'dagName'

default_args = {
    'owner': 'Airflow',
    'start_date':datetime(2022,2,16),
    'schedule_interval':'@once',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'catchup':False
}

with DAG(dag_name,default_args=default_args) as dag:

    t1 = DummyOperator(task_id="start")

    t2 = BashOperator(task_id='hello_world',
                        bash_command='echo "Hi!!"')
    
    t3 = DummyOperator(task_id="end")
    
    t1 >> t2 >> t3
