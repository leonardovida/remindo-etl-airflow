import os
import logging.config
import logging
import os.path
from datetime import datetime, timedelta
from pathlib import Path
import configparser

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from src.api.fetchdata import main

# Setting up logger, Logger properties are defined in logging.ini file
# logging.config.fileConfig(os.path.join(Path(__file__).parents[1], "config/logging.ini"))
# logger = logging.getLogger(__name__)

# Reading configurations
config = configparser.ConfigParser()
config.read_file(open(os.path.join(Path(__file__).parents[1], "config/prod.cfg")))

# TODO: use provide_context=True to provide to the fetching of data
# the updated value of the time!


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(7),
    "email": ["l.j.vida@uu.nl"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,  # To setup only in production
}

dag = DAG(
    "extract_pipeline",
    default_args=default_args,
    description="Extract data from Remindo to landing zone",
    # schedule_interval=None,
    dagrun_timeout=timedelta(minutes=1),
    schedule_interval=timedelta(minutes=5),
    max_active_runs=1,
)

startOperator = DummyOperator(task_id="beginExecution", dag=dag)

jobOperator = PythonOperator(
    task_id="retrieveData",
    provide_context=False,
    python_callable=main,
    dag=dag,
)

endOperator = DummyOperator(task_id="stopExecution", dag=dag)

startOperator >> jobOperator >> endOperator

# if __name__ == "__main__":
#     dag.cli()
