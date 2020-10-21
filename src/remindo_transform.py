import configparser
import logging
from logging.config import fileConfig
from os.path import join, dirname, abspath
from pathlib import Path
import shutil

from loguru import logger

# from pyspark.sql.types import StringType
# from pyspark.sql import functions as fn
# import remindo_udf

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# TODO: All the dates need to be validated
# - if they are not valid then 0 or something that can be accepted
# - by the SQL server
# Remember that responses ARE STORED IN `TEXT`
# TODO: All the false and true need to be written nicely
# TODO: If spark, individual folders for files have to be created


class RemindoTransform:
    """Class to transform datasets without Spark"""

    def __init__(self, load_path, save_path):
        self._load_path = load_path
        self._save_path = save_path

    def transform_clusters_dataset(self):
        logger.debug("Inside cluster dataset")
        logger.debug(f"Attempting to move data to {self._save_path + '/clusters/'}")
        shutil.move(
            self._load_path + "/clusters.csv", self._save_path + "/clusters.csv"
        )

    def transform_studies_dataset(self):
        logger.debug("Inside studies dataset")
        logger.debug(f"Attempting to move data to {self._save_path + '/studies/'}")
        shutil.move(self._load_path + "/studies.csv", self._save_path + "/studies.csv")

    def transform_recipes_dataset(self):
        logger.debug("Inside recipes dataset")
        logger.debug(f"Attempting to move data to {self._save_path + '/recipes/'}")
        shutil.move(self._load_path + "/recipes.csv", self._save_path + "/recipes.csv")

    def transform_moments_dataset(self):
        logger.debug("Inside moments dataset")
        logger.debug(f"Attempting to move data to {self._save_path + '/moments/'}")
        shutil.move(self._load_path + "/moments.csv", self._save_path + "/moments.csv")

    def transform_moment_results_dataset(self):
        logger.debug("Inside moment_results dataset")
        logger.debug(
            f"Attempting to move data to {self._save_path + '/moment_results/'}"
        )
        shutil.move(
            self._load_path + "/moment_results.csv",
            self._save_path + "/moment_results.csv",
        )

    def transform_stats_dataset(self):
        logger.debug("Inside stats dataset")
        logger.debug(f"Attempting to move data to {self._save_path + '/stats/'}")
        shutil.move(self._load_path + "/stats.csv", self._save_path + "/stats.csv")

    def transform_items_dataset(self):
        logger.debug("Inside items dataset")
        logger.debug(f"Attempting to move data to {self._save_path + '/items/'}")
        shutil.move(self._load_path + "/items.csv", self._save_path + "/items.csv")

    def transform_reliabilities(self):
        logger.debug("Inside reliabilities dataset")
        logger.debug(
            f"Attempting to move data to {self._save_path + '/reliabilities/'}"
        )
        shutil.move(
            self._load_path + "/reliabilities.csv",
            self._save_path + "/reliabilities.csv",
        )


class RemindoSparkTransform:
    """Class to transform dataset using Spark"""

    def __init__(self, spark):
        self._spark = spark
        self._load_path = config.get("FOLDER", "WORKING_ZONE")
        self._save_path = config.get("FOLDER", "PROCESSED_ZONE")

    def transform_moments_dataset(self):
        logger.debug("Inside transform moments dataset module")
        moments_df = self._spark.read.csv(
            self._load_path + "/moments.csv",
            header=True,
            mode="PERMISSIVE",
            inferSchema=True,
        )

        logger.debug(f"Attempting to write data to {self._save_path + '/moments/'}")
        deduped_moments_df.repartition(10).write.csv(
            path=self._save_path + "/moments/",
            sep="|",
            mode="overwrite",
            compression="gzip",
            header=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss.SSS",
            quote='"',
            escape='"',
        )

    def transform_moment_result_dataset(self):
        logger.debug("Inside transform moment_results dataset module")
        moment_result_df = self._spark.read.csv(
            self._load_path + "/moment_result.csv",
            header=True,
            mode="PERMISSIVE",
            inferSchema=True,
        )

        logger.debug(
            f"Attempting to write data to {self._save_path + '/moment_results/'}"
        )
        deduped_moment_result_df.repartition(10).write.csv(
            path=self._save_path + "/moment_results/",
            sep="|",
            mode="overwrite",
            compression="gzip",
            header=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss.SSS",
            quote='"',
            escape='"',
        )

    def transform_item_dataset(self):
        logger.debug("Inside transform items dataset module")
        items_df = self._spark.read.csv(
            self._load_path + "/item.csv",
            header=True,
            mode="PERMISSIVE",
            inferSchema=True,
        )

        logger.debug(f"Attempting to write data to {self._save_path + '/items/'}")
        deduped_items_df.repartition(10).write.csv(
            path=self._save_path + "/items/",
            sep="|",
            mode="overwrite",
            compression="gzip",
            header=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss.SSS",
            quote='"',
            escape='"',
        )
