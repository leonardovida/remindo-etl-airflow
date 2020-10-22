import configparser
from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from src.remindo_driver import main

config = configparser.ConfigParser()
config.read_file(open(os.path.join(Path(__file__).parents[1], "config/prod.cfg")))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 1),
    "email": ["l.j.vida@uu.nl"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,  # To setup only in production
}


dag_name = "transform_load_pipeline"

dag = DAG(
    dag_name,
    default_args=default_args,
    description="Transform and load data from landing zone to processed zone.\
        Populate data from Processed zone to remindo Warehouse.",
    # schedule_interval=None,
    schedule_interval=timedelta(minutes=5),
    max_active_runs=1,
)

startOperator = DummyOperator(task_id="BeginExecution", dag=dag)

jobOperator = PythonOperator(task_id="TransformLoadJob", python_callable=main, dag=dag)

endOperator = DummyOperator(task_id="StopExecution", dag=dag)

startOperator >> jobOperator >> endOperator
