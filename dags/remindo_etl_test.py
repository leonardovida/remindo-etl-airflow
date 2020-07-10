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
# Either: 1) delete what was retrieved and restart
# select only the missing recipes and moments and continue
# TODO: display total time at the end of the retrieval

# Set up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[1]}/config/logging.ini")
logger = logging.getLogger(__name__)

# Read configurations
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[1]}/config/prod.cfg"))

# Get variables from config file
DATA_DIR = config["DATA_DIR_PATH"]["PATH"]

# Get variables from Airflow
SINCE = Variable.get("since")
FROM = Variable.get("from")
UNTIL = Variable.get("until")

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


def _input_continue(input):
    if input != "Yes":
        return False
    else:
        return True

def extract_data():
    logging.debug("Creating Remindo client.")
    rclient = client.RemindoClient(
        config["REMINDOKEYS"]["UUID"],
        config["REMINDOKEYS"]["SECRET"],
        config["REMINDOKEYS"]["URL_BASE"],
    )

    working_directory = config["DATA_DIR_PATH"]["PATH"]

    logging.debug("Fetching data from {0}.".format(config["DATE"]["SINCE"]))
    logging.debug(f"Execution started at {datetime.now()}")

    if _is_file(working_directory, "items.csv"):
        try:
            logging.debug(f"Found items.csv")
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
            logging.debug(f"Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)

    elif _is_file(working_directory, "stats.csv"):
        try:
            logging.debug(f"Found stats.csv.")
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
            logging.debug(f"Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug(f"Finished retrieving reliability")
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
            logging.debug(f"Found all id lists.")
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
            logging.debug(f"Finished retrieving stats")
            rcollector.fetch_item_data()
            logging.debug(f"Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug(f"Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)

    elif _is_file(working_directory, "recipe_id_list.txt"):
        try:
            logging.debug(f"Found recipe id list.")
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
            logging.debug(f"Finished retrieving moments")
            rcollector.fetch_stats_data()
            logging.debug(f"Finished retrieving stats")
            rcollector.fetch_item_data()
            logging.debug(f"Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug(f"Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)
    else:
        try:
            logging.debug(f"No id lists found, starting from studies.")
            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
            )
            rcollector.fetch_studies_recipes()
            logging.debug(f"Finished retrieving studies")
            rcollector.fetch_recipes()
            logging.debug(f"Finished retrieving recipes")
            rcollector.fetch_moments()
            logging.debug(f"Finished retrieving moments")
            rcollector.fetch_stats_data()
            logging.debug(f"Finished retrieving stats")
            rcollector.fetch_item_data()
            logging.debug(f"Finished retrieving items")
            rcollector.fetch_reliability()
            logging.debug(f"Finished retrieving reliability")
        except KeyError as e:
            logging.exception("MAIN ", e)
        try:
            logging.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logging.exception("A exception occured: ", e)
    logging.debug("Execution ended.")

### To delete or modify

def extract_tweet_data(tweepy_obj, query):
    """ Extract relevant and serializable data from a tweepy Tweet object
        params:
            tweepy_obj: Tweepy Tweet Object
            query: str
        returns dict
    """
    return {
        "user_id": tweepy_obj.user.id,
        "user_name": tweepy_obj.user.name,
        "user_screenname": tweepy_obj.user.screen_name,
        "user_url": tweepy_obj.user.url,
        "user_description": tweepy_obj.user.description,
        "user_followers": tweepy_obj.user.followers_count,
        "user_friends": tweepy_obj.user.friends_count,
        "created": tweepy_obj.created_at.isoformat(),
        "text": tweepy_obj.text,
        "hashtags": [ht.get("text") for ht in tweepy_obj.entities.get("hashtags")],
        "mentions": [
            (um.get("id"), um.get("screen_name"))
            for um in tweepy_obj.entities.get("user_mentions")
        ],
        "urls": [url.get("expanded_url") for url in tweepy_obj.entities.get("urls")],
        "tweet_id": tweepy_obj.id,
        "is_quote_status": tweepy_obj.is_quote_status,
        "favorite_count": tweepy_obj.favorite_count,
        "retweet_count": tweepy_obj.retweet_count,
        "reply_status_id": tweepy_obj.in_reply_to_status_id,
        "lang": tweepy_obj.lang,
        "source": tweepy_obj.source,
        "location": tweepy_obj.coordinates,
        "query": query,
    }

def search_twitter(**kwargs):
    """ Search for a query in public tweets"""
    query = kwargs.get("params").get("query")

    auth = OAuthHandler(Variable.get("consumer_key"), Variable.get("consumer_secret"))
    auth.set_access_token(
        Variable.get("access_token"), Variable.get("access_token_secret")
    )
    api = API(auth)

    all_tweets = []
    page_num = 0
    since_date = datetime.strptime(kwargs.get("ds"), "%Y-%m-%d").date() - timedelta(
        days=1
    )
    query += " since:{} until:{}".format(
        since_date.strftime("%Y-%m-%d"), kwargs.get("ds")
    )
    print(f"searching twitter with: {query}")
    for page in Cursor(
        api.search, q=query, monitor_rate_limit=True, wait_on_rate_limit=True
    ).pages():
        all_tweets.extend([extract_tweet_data(t, query) for t in page])
        page_num += 1
        if page_num > MAX_TWEEPY_PAGE:
            break

    # if it's an empty list, stop here
    if not len(all_tweets):
        return

    filename = "{}/{}_{}.csv".format(
        DATA_DIR, query, datetime.now().strftime("%m%d%Y%H%M%S")
    )

    # check that the directory exists
    if not Path(filename).resolve().parent.exists():

        os.mkdir(Path(filename).resolve().parent)

    with open(filename, "w") as raw_file:
        raw_wrtr = DictWriter(raw_file, fieldnames=all_tweets[0].keys())
        raw_wrtr.writeheader()
        raw_wrtr.writerows(all_tweets)


def csv_to_sql(directory=DATA_DIR, **kwargs):
    """ csv to sql pipeline using pandas
        params:
            directory: str (file path to csv files)
    """
    dbconn = MySqlHook(mysl_conn_id="mysql_default")
    cursor = dbconn.get_cursor()

    for fname in glob.glob("{}/*.csv".format(directory)):
        if "_read" not in fname:
            try:
                df = pd.read_csv(fname)
                df.to_sql("tweets", dbconn, if_exists="append", index=False)
                shutil.move(fname, fname.replace(".csv", "_read.csv"))
            except pd.io.common.EmptyDataError:
                # probably an io error with another task / open file
                continue

def remindo_driver_etl():
    return(None)

# --------------------------------------
# Dag
# -------------------------------------

default_args = {
    "owner": "admin",
    # If past scheduled task failed, it fails
    "depends_on_past": True,
    "start_date": datetime(2020, 6, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    "retries": 20,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "remindo_etl_test",
    default_args=default_args,
    #TODO: Catchup to be activated if interval of retrieval is automatic
    catchup=False,
    schedule_interval="@monthly"
)

# --------------------------------------
# Tasks
# -------------------------------------
start_operator = DummyOperator(
    task_id='begin_execution',
    dag=dag,
)

extract_operator = PythonOperator(
    task_id="extract_from_remindo_api",
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
    # note we pass this as a params obj
    # Here we have to pass the params extracted from config
    params={"query": "#pycon"},
)

move_to_landing_folder_operator = PythonOperator(
    task_id="data_to_landing_area",
    # extra DAG context
    provide_context=True,
    # call the function
    python_callable=csv_to_sql,
    dag=dag,
)

etl_job_operator = PythonOperator(
    task_id="Remindo_ETL_job",
    # extra DAG context
    provide_context=True,
    # call the function
    python_callable=remindo_driver_etl,
    dag=dag,
)

email_operator = EmailOperator(
    task_id="email_confirmation",
    to="l.j.vida@uu.nl",
    subject="Extraction lastest Remindo data",
    html_content="The extraction was successful.",
    dag=dag,
)

finish_operator = DummyOperator(
    task_id='finish_execution',
    dag=dag,
)

start_operator >> extract_operator >> move_to_landing_folder_operator
move_to_landing_folder_operator >> etl_job_operator >> email_operator >> finish_operator