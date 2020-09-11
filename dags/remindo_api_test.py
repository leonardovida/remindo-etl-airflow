from datetime import datetime, timedelta
import os
import logging.config
import logging
import os.path
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import configparser

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
#from airflow.operators.remindo_plugin import DataQualityOperator
#from airflow.operators.remindo_plugin import LoadAnalyticsOperator
#from helpers import AnalyticsQueries

from remindo_api import client
from remindo_api import collectdata
from six.moves import input
import pandas as pd

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(os.path.join(Path(__file__).parents[1], 'config/logging.ini'))
logger = logging.getLogger(__name__)

# Reading configurations
config = configparser.ConfigParser()
config.read_file(open(os.path.join(Path(__file__).parents[1], "config/prod.cfg")))

# TODO: use provide_context=True to provide to the fetching of data
# the updated value of the time!

def _open_from_temp(directory, name):
    file = os.path.join(directory, f"{name}.txt")
    with open(file, "r") as f:
        result = list()
        for line in f:
            result.append(int(line.strip()))
        return result


def _open_from_temp_dict(directory, name):
    file = os.path.join(directory, f"{name}.json")
    with open(file, "r") as f:
        new = json.load(f)
        return new


def _is_file(directory, name):
    if os.path.isfile(os.path.join(directory, name)):
        return True
    else:
        return False


def fetchdata():
    """Main function for fetching Remindo data."""
    logging.debug("Creating Remindo client.")
    rclient = client.RemindoClient(
        config["REMINDOKEYS"]["UUID"],
        config["REMINDOKEYS"]["SECRET"],
        config["REMINDOKEYS"]["URL_BASE"],
    )

    # Set the folder where the data is going to go land initially
    working_directory = config["DATA_DIR_PATH"]["PATH"]

    logging.debug("Fetching data from {0}.".format(config["DATE"]["SINCE"]))
    logging.debug(f"Execution started at {datetime.now()}")

    if _is_file(working_directory, "items.csv"):
        try:
            logging.debug("Found items.csv")
            r = _open_from_temp(working_directory, "recipe_id_list")
            m = _open_from_temp(working_directory, "moment_id_list")
            rm = _open_from_temp_dict(working_directory, "recipe_moment_id_dict")

            # val = input("Do you want to continue fetching the reliability? (Yes/No) ")
            # answer = _input_continue(val)

            # if answer:
            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
                recipe_id_list=r,
                moment_id_list=m,
                recipe_moment_id_dict=rm,
            )
            rcollector.fetch_reliability()
            logging.debug("Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)

    elif _is_file(working_directory, "stats.csv"):
        try:
            logging.debug("Found stats.csv.")
            r = _open_from_temp(working_directory, "recipe_id_list")
            m = _open_from_temp(working_directory, "moment_id_list")
            rm = _open_from_temp_dict(working_directory, "recipe_moment_id_dict")

            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
                recipe_id_list=r,
                moment_id_list=m,
                recipe_moment_id_dict=rm,
            )
            rcollector.fetch_item_data()
            logging.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug("Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)

    elif (
        _is_file(working_directory, "recipe_id_list.txt")
        and _is_file(working_directory, "moment_id_list.txt")
        and _is_file(working_directory, "recipe_moment_id_dict.json")
    ):
        try:
            logging.debug("Found all id lists.")
            r = _open_from_temp(working_directory, "recipe_id_list")
            m = _open_from_temp(working_directory, "moment_id_list")
            rm = _open_from_temp_dict(working_directory, "recipe_moment_id_dict")

            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
                recipe_id_list=r,
                moment_id_list=m,
                recipe_moment_id_dict=rm,
            )
            rcollector.fetch_stats_data()
            logging.debug("Finished retrieving stats")
            rcollector.fetch_item_data()
            logging.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug("Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)

    elif _is_file(working_directory, "recipe_id_list.txt"):
        try:
            logging.debug("Found recipe id list.")
            r = _open_from_temp(working_directory, "recipe_id_list")
            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
                recipe_id_list=r,
            )

            rcollector.fetch_moments()
            logging.debug("Finished retrieving moments")
            rcollector.fetch_stats_data()
            logging.debug("Finished retrieving stats")
            rcollector.fetch_item_data()
            logging.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug("Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)
    else:
        try:
            logging.debug("No id lists found, starting from studies.")
            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
            )
            rcollector.fetch_studies_recipes()
            logging.debug("Finished retrieving studies")
            rcollector.fetch_recipes()
            logging.debug("Finished retrieving recipes")
            rcollector.fetch_moments()
            logging.debug("Finished retrieving moments")
            rcollector.fetch_stats_data()
            logging.debug("Finished retrieving stats")
            rcollector.fetch_item_data()
            logging.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug("Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)
    logging.debug("Execution ended.")


default_args = {
    'owner': 'remindo',
    'depends_on_past': True,
    'start_date' : datetime(2020, 9, 11),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup': True
}

dag = DAG(dag_id='remindo_api_test',
          default_args=default_args,
          description='Load and Transform data from landing zone to processed zone. Populate data from Processed zone to remindo Warehouse.',
          schedule_interval=timedelta(minutes=10),
          max_active_runs = 1
)

# start_operator = DummyOperator(
#     task_id='Begin_execution',
#     dag=dag
# )

retrieve_from_api = PythonOperator(
    task_id='retrieve_data',
    #provide_context=True,
    python_callable=fetchdata,
    dag=dag
)

#emrsshHook= SSHHook(ssh_conn_id='emr_ssh_connection')

# jobOperator = SSHOperator(
#     task_id="RemindoETLJob",
#     command='cd /Users/leonardovida/dev/remindo-etl-pipeline/src;python remindo_driver.py;',
#     ssh_hook=emrsshHook,
#     dag=dag)

# end_operator = DummyOperator(
#     task_id='Stop_execution',
#     dag=dag
# )

retrieve_from_api
