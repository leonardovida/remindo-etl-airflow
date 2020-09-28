# from pyspark.sql.types import StringType
# from pyspark.sql import functions as fn
# from pyspark.sql import SparkSession
import logging.config
import logging
import configparser
from pathlib import Path
import time
import os
#import cx_Oracle
import datetime
import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, MetaData
from sqlalchemy import *
from sqlalchemy.sql import *

from copy_module import RemindoCopyModule
from warehouse import remindo_dwh_base
from warehouse import remindo_dwh_classes
from warehouse import remindo_warehouse_driver

#from warehouse.remindo_warehouse_driver import RemindoWarehouseDriver

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger()


def main():
#     rt = RemindoTransform()

#     # # Modules in the project
#     modules = {
#         "cluster.csv": rt.transform_cluster_dataset,
#         # "item.csv": rt.transform_item_dataset,
#         # "moment_result.csv": rt.transform_moment_result_dataset,
#         # "moments.csv": rt.transform_moments_dataset,
#         # "recipes.csv": rt.transform_recipes_dataset,
#         # "stats.csv": rt.transform_stats_dataset,
#         # "studies.csv": rt.transform_studies_dataset
#     }

#     logging.debug("\n\nCopying data from landing zone to ...")
#     rcm = RemindoCopyModule()
#     rcm.move_data(
#         source_folder= config.get('FOLDER', 'LANDING_ZONE'),
#         target_folder= config.get('FOLDER', 'WORKING_ZONE')
#         )

#     files_in_working_zone = rcm.get_files(config.get('FOLDER', 'WORKING_ZONE'))

#     # # Cleanup processed zone if files available in working zone
#     if len([set(files_in_working_zone)]) > 0:
#         logging.info("Cleaning up processed zone.")
#         rcm.clean_folder(config.get('FOLDER', 'PROCESSED_ZONE'))

#     for file in files_in_working_zone:
#         if file in modules.keys():
#             modules[file]()
    
    # wrd = remindo_warehouse_driver.RemindoWarehouseDriver()
    # wrd.test_conn()
    # wrd.delete_staging_tables()
    # wrd.setup_staging_tables()
    # wrd.load_staging_tables()
    # wrd.setup_warehouse_tables()
    # wrd.upsert()

if __name__ == "__main__":
    main()
    