# from pyspark.sql import SparkSession
from pathlib import Path
import time
import configparser
import sys

from loguru import logger

from src.remindo_transform import RemindoTransform
from src.copy_module import RemindoCopyModule
from src.warehouse.remindo_warehouse_driver import RemindoWarehouseDriver

# TODO: finish the configuration
# TODO: Create or eliminate the usage of Spark

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# Add logger using loguru
logger.add(
    sys.stderr,
    format="{time} {level} {exeception} {file} {message} {elapsed}",
    level="INFO",
)
logger.add("run_log_{time}.log", rotation="6 month", backtrace=True, diagnose=True)

# def create_sparksession():
#     return SparkSession.builder.master('yarn').appName("remindo") \
#         .enableHiveSupport().getOrCreate()


def main():
    LANDING_ZONE = config.get("FOLDER", "LANDING_ZONE")
    WORKING_ZONE = config.get("FOLDER", "WORKING_ZONE")
    PROCESSED_ZONE = config.get("FOLDER", "PROCESSED_ZONE")

    # logging.debug("\n\nSetting up Spark Session...")
    # spark = create_sparksession()
    rt = RemindoTransform(load_path=WORKING_ZONE, save_path=PROCESSED_ZONE)

    # Modules in the project
    modules = {
        "clusters.csv": rt.transform_clusters_dataset,
        "studies.csv": rt.transform_studies_dataset,
        "recipes.csv": rt.transform_recipes_dataset,
        "moments.csv": rt.transform_moments_dataset,
        "moment_results.csv": rt.transform_moment_results_dataset,
        "reliabilities.csv": rt.transform_reliabilities,
        "stats.csv": rt.transform_stats_dataset,
        "items.csv": rt.transform_items_dataset,
    }

    logger.info("Copying data from landing zone to working zone")
    rcm = RemindoCopyModule(
        landing_zone=LANDING_ZONE,
        working_zone=WORKING_ZONE,
        processed_zone=PROCESSED_ZONE,
    )

    rcm.move_data(source_folder=LANDING_ZONE, target_folder=WORKING_ZONE)

    files_in_working_zone = rcm.get_files(WORKING_ZONE)

    # Cleanup processed zone if files available in working zone
    if len([set(files_in_working_zone)]) > 0:
        logger.info("Cleaning up processed zone.")
        rcm.clean_folder(PROCESSED_ZONE)

    # If file in the zone, apply transform
    for file in files_in_working_zone:
        if file in modules.keys():
            modules[file]()

    logger.info("Waiting before setting up Warehouse")
    time.sleep(2)

    # Starting warehouse functionality
    rwarehouse = RemindoWarehouseDriver(
        landing_zone=LANDING_ZONE,
        working_zone=WORKING_ZONE,
        processed_zone=PROCESSED_ZONE,
    )
    rwarehouse.delete_staging_tables()
    rwarehouse.setup_staging_tables()
    rwarehouse.load_staging_tables()
    rwarehouse.setup_warehouse_tables()
    rwarehouse.perform_upsert()


if __name__ == "__main__":
    main()
