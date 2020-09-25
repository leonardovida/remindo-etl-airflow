from datetime import datetime, timedelta
import os

import configparser
from pathlib import Path

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import sys
sys.path.insert(1, '/Users/leonardovida/airflow/src/')
from src import remindo_driver

config = configparser.ConfigParser()
config.read_file(open(os.path.join(Path(__file__).parents[1], "config/prod.cfg")))

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date' : datetime(2020, 9, 16),
    "email": ["l.j.vida@uu.nl"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'catchup': False #To setup only in production
}


dag_name = 'transform_load_pipeline'

dag = DAG(dag_name,
          default_args=default_args,
          description='Transform and load data from landing zone to processed zone. Populate data from Processed zone to remindo Warehouse.',
          #schedule_interval=None,
          schedule_interval='*/10 * * * *',
          max_active_runs = 1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# jobOperator = BashOperator(
#     task_id="TransformLoadJob",
#     bash_command='{{"/Users/leonardovida/airflow/src/TLJob.sh"}}',
#     dag=dag
# )

jobOperator = PythonOperator(
    task_id="TransformLoadJob",
    python_callable=remindo_driver.main(),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> jobOperator >> end_operator
