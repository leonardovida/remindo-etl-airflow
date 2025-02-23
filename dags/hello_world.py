from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# TODO: fix manual change when retrieval breaks
# meaning that dates need to be automatically changed or fixed already
# Either: 1) delete what was retrieved and restart
# select only the missing recipes and moments and continue
# TODO: display total time at the end of the retrieval


def print_hello():
    print("Hello world ran.")
    return "Hello world!"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2017, 3, 20),
    "email": ["l.j.vida@uu.nl"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "hello_world",
    default_args=default_args,
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

dummy_operator = DummyOperator(task_id="dummy_task", retries=3, dag=dag)

hello_world = PythonOperator(task_id="hello_task", python_callable=print_hello, dag=dag)

dummy_operator >> hello_world
