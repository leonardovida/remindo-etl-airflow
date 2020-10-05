# src/remindo_api/collectdata.py
"""Class implementation to fetch Remindo data."""
import configparser
from datetime import datetime
import json
import logging
import logging.config
import os
from pathlib import Path

# import time
from remindo_api import client
from remindo_api import collectdata

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(os.path.join(Path(__file__).parents[1], "config/logging.ini"))
logger = logging.getLogger(__name__)

# Reading configurations
config = configparser.ConfigParser()
config.read_file(open(os.path.join(Path(__file__).parents[1], "config/prod.cfg")))

# TODO: fix manual change when retrieval breaks
# Either: 1) delete what was retrieved and restart
# select only the missing recipes and moments and continue
# TODO: display total time at the end of the retrieval


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


def main():
    """Main function for fetching Remindo data."""
    logger.debug("Creating Remindo client.")
    rclient = client.RemindoClient(
        config["REMINDOKEYS"]["UUID"],
        config["REMINDOKEYS"]["SECRET"],
        config["REMINDOKEYS"]["URL_BASE"],
    )

    # Set the folder where the data is going to go land initially
    working_directory = config["DATA_DIR_PATH"]["PATH"]

    logger.info("Fetching data from {0}.".format(config["DATE"]["SINCE"]))
    logger.debug(f"Execution started at {datetime.now()}")

    if _is_file(working_directory, "items.csv"):
        try:
            logger.debug("Found items.csv")
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
            logger.debug("Finished retrieving reliability")
        except KeyError as e:
            logger.exception("MAIN ", e)
        try:
            logger.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logger.exception("A exception occured: ", e)

    elif _is_file(working_directory, "stats.csv"):
        try:
            logger.debug("Found stats.csv.")
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
            logger.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logger.debug("Finished retrieving reliability")
        except KeyError as e:
            logger.exception("MAIN ", e)
        try:
            logger.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logger.exception("A exception occured: ", e)

    elif (
        _is_file(working_directory, "recipe_id_list.txt")
        and _is_file(working_directory, "moment_id_list.txt")
        and _is_file(working_directory, "recipe_moment_id_dict.json")
    ):
        try:
            logger.debug("Found all id lists.")
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
            logger.debug("Finished retrieving stats")
            rcollector.fetch_item_data()
            logger.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logger.debug("Finished retrieving reliability")
        except KeyError as e:
            logger.exception("MAIN ", e)
        try:
            logger.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logger.exception("A exception occured: ", e)

    elif _is_file(working_directory, "recipe_id_list.txt"):
        try:
            logger.debug("Found recipe id list.")
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
            logger.debug("Finished retrieving moments")
            rcollector.fetch_stats_data()
            logger.debug("Finished retrieving stats")
            rcollector.fetch_item_data()
            logger.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logger.debug("Finished retrieving reliability")
        except KeyError as e:
            logger.exception("MAIN ", e)
        try:
            logger.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logger.exception("A exception occured: ", e)
    else:
        try:
            logger.debug("No id lists found, starting from studies.")
            rcollector = collectdata.RemindoCollect(
                rclient=rclient,
                data_directory=working_directory,
                since_date=config["DATE"]["SINCE"],
                until_date=config["DATE"]["UNTIL"],
                from_date=config["DATE"]["FROM"],
            )
            rcollector.fetch_studies_recipes()
            logger.debug("Finished retrieving studies")
            rcollector.fetch_recipes()
            logger.debug("Finished retrieving recipes")
            rcollector.fetch_moments()
            logger.debug("Finished retrieving moments")
            rcollector.fetch_stats_data()
            logger.debug("Finished retrieving stats")
            rcollector.fetch_item_data()
            logger.debug("Finished retrieving items")
            rcollector.fetch_reliability()
            logger.debug("Finished retrieving reliability")
        except KeyError as e:
            logger.exception("MAIN ", e)
        try:
            logger.debug("Deleting data lists.")
            rcollector.reset_data_lists()
        except Exception as e:
            logger.exception("A exception occured: ", e)
    logger.debug("Execution ended.")


if __name__ == "__main__":
    main()
    logger.debug("Finished data retrival. Exiting.")
