import ast
import glob
import itertools
import os.path
import shutil
from collections import Counter
import configparser
from csv import DictWriter, writer
import json
import logging
import logging.config
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator

from remindo_api import client
from remindo_api import collectdata
from six.moves import input
import pandas as pd

# TODO: fix manual change when retrieval breaks
# meaning that dates need to be automatically changed or fixed already
# Either: 1) delete what was retrieved and restart
# select only the missing recipes and moments and continue
# TODO: display total time at the end of the retrieval

def print_hello():
    logging.warning("Hello world ran.")
    return "Hello world!"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 10),
    "email": ["l.j.vida@uu.nl"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    default_args=default_args,
    catchup=False,
)

hello_world = PythonOperator(
    task_id="hello_task",
    python_callable=print_hello,
    dag=dag
)

hello_world