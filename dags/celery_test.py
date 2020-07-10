from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_dag_one',
            schedule_interval='@daily',
            default_args=default_args)

t1 = BashOperator(
    task_id='print_date1',
    bash_command='sleep 2m',
    dag=dag)

t2 = BashOperator(
    task_id='print_date2',
    bash_command='sleep 2m',
    dag=dag)

t3 = BashOperator(
    task_id='print_date3',
    bash_command='sleep 2m',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)