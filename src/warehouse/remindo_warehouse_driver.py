from collections import OrderedDict
import configparser
import logging
from pathlib import Path
import re
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# import cx_Oracle
# from contextlib import contextmanager

# from src.warehouse.remindo_dwh_base import Base
from src.copy_module import RemindoCopyModule
from src.warehouse.base import Base
from src.warehouse.models.cluster import Cluster
from src.warehouse.models.study import Study
from src.warehouse.models.recipe import Recipe
from src.warehouse.models.moment import Moment
from src.warehouse.models.moment_result import MomentResult
from src.warehouse.models.reliability import Reliability
from src.warehouse.models.stat import Stat
from src.warehouse.models.item import Item

# from src.warehouse.remindo_dwh_classes import (
#     Cluster,
#     Reliability,
#     Study,
#     Recipe,
#     Moment,
#     MomentResult,
#     Stat,
#     Item,
# )
from src.warehouse.remindo_upsert import upsert

from loguru import logger

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))


class RemindoWarehouseDriver:
    """This is the class driver of the warehouse."""

    def __init__(self, landing_zone, working_zone, processed_zone):
        # Database credentials
        self.host = config.get("DATABASE", "HOST")
        self.db_name = config.get("DATABASE", "DB_NAME")
        self.db_user = config.get("DATABASE", "DB_USER")
        self.db_password = config.get("DATABASE", "DB_PASSWORD")
        self.db_port = config.get("DATABASE", "DB_PORT")
        self.password = config.get("DATABASE", "DB_PASSWORD")

        # Files' names
        self.files = re.sub("[^\w]", " ", config.get("FILES", "NAMES")).split()

        # Location of files
        self.landing_zone = landing_zone
        self.working_zone = working_zone
        self.processed_zone = processed_zone

        self.rcm = RemindoCopyModule(
            landing_zone=self.landing_zone,
            working_zone=self.working_zone,
            processed_zone=self.processed_zone,
        )

        # TODO: finish the engine configuration using the data from UU
        # self.engine = create_engine(
        #     'oracle+cx_oracle://Remindo:Hog14GHln@its-w-s521.soliscom.uu.nl:1521/DWHO',
        #     max_identifier_length=30,
        #     echo=True)
        # Remindo/Hog14GHln@//its-w-s521.soliscom.uu.nl:1521/DWHO

        self.engine = create_engine(
            "postgresql+psycopg2://airflow:airflow@localhost/airflow", echo=True
        )

        Base.metadata.bind = self.engine

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Create session and cursor to talk to the db
        self.conn = self.engine.connect()

        # List of files to retrieve
        self.modules = OrderedDict(
            [
                ("clusters.csv", Cluster),
                ("studies.csv", Study),
                ("recipes.csv", Recipe),
                ("moments.csv", Moment),
                ("moment_results.csv", MomentResult),
                ("stats.csv", Stat),
                ("items.csv", Item),
                ("reliabilities.csv", Reliability),
            ]
        )

        # self.con = cx_Oracle.connect("pythonhol/welcome@127.0.0.1/".format(
        # *config['CLUSTER'].values()))
        # self.dsn_tns = cx_Oracle.makedsn('localhost', 1521, 'XE')
        # self.con2 = cx_Oracle.connect('hr', 'hrpwd', self.dsn_tns)
        # self._cur = self._con.cursor()

    # @contextmanager
    # def session_scope(self):
    #     """Provide a transactional scope around a series of operations."""
    #     try:
    #         yield self.session
    #         self.session.commit()
    #     except Exception:
    #         self.session.rollback()
    #         raise
    #     finally:
    #         self.session.close()

    def test_conn(self):
        """Test the connection to the database."""
        print(self.conn.info)

    def delete_staging_tables(self):
        """Delete the previous tables in the staging database."""
        # ATTENTION!
        # Drops all existing tables!!!
        logger.debug("Drop staging tables.")
        Base.metadata.drop_all(self.engine, checkfirst=True)

    def setup_staging_tables(self):
        """Setup the staging database schema."""
        logger.debug("Create staging schema.")
        Base.metadata.create_all(self.engine, checkfirst=True)

    def load_staging_tables(self):
        """Load the data from csv files to the database."""
        logger.debug("Loading staging tables")

        files_in_processed_zone = self.rcm.get_files(self.processed_zone)

        for file in self.modules.keys():
            if file in files_in_processed_zone:
                logger.debug(f"Loading table {file}.")
                data = pd.read_csv(
                    os.path.join(self.processed_zone, file), delimiter=","
                )
                data_list = data.to_dict(orient="records")
                for row in data_list:
                    row = self.modules[file](**row)
                    try:
                        self.session.add(row)
                        self.session.commit()
                        self.session.close()
                    except SQLAlchemyError as e:
                        self.session.rollback()
                        error = str(e.__dict__["orig"])
                        logger.debug(f"Rolled back. Error: {error}")
                        print(e)
                logger.debug("Finished loading.")
            else:
                logger.debug(f"File {file} not found.")

    def setup_warehouse_tables(self):
        logger.debug("Setup schema for warehouse.")

        # Change schema from staging to warehouse
        self.conn = self.engine.connect().execution_options(
            schema_translate_map={"staging_schema": "warehouse"}
        )

        # Delete tables
        logger.debug("Dropping tables for warehouse.")
        Base.metadata.drop_all(self.conn, checkfirst=True)
        # Create same tables as in staging_schema
        logger.debug("Creating tables for warehouse.")
        Base.metadata.create_all(self.conn, checkfirst=True)

    def perform_upsert(self):
        logger.debug("Performing upsert operations.")
        # Upsert operation of each of the tables
        for m, c in self.modules.items():
            try:
                upsert(self.engine, self.session, self.conn, c)
            except SQLAlchemyError as e:
                self.session.rollback()
                error = str(e.__dict__["orig"])
                logger.debug(f"Rolled back. Error: {error}")
                print(e)
            except NameError:
                logger.debug(f"File {m} not found.")
