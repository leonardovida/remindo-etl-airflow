from collections import OrderedDict
import configparser
from contextlib import contextmanager
import csv
import collections
import logging
from pathlib import Path
import re
import sys

from copy_module import RemindoCopyModule
from . import remindo_dwh_base
from .remindo_dwh_classes import Cluster, Study, Recipe, Moment, MomentResult, Stat, Item
from . import remindo_upsert

#import cx_Oracle
import pandas as pd
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

class RemindoWarehouseDriver:
    """This is the class driver of the warehouse."""

    def __init__(self):
        # Database credentials
        self.host = config.get('DATABASE', 'HOST')
        self.db_name = config.get('DATABASE', 'DB_NAME')
        self.db_user = config.get('DATABASE', 'DB_USER')
        self.db_password = config.get('DATABASE', 'DB_PASSWORD')
        self.db_port = config.get('DATABASE', 'DB_PORT')
        self.password = config.get('DATABASE', 'DB_PASSWORD')

        # Files' names
        self.files = re.sub("[^\w]", " ",  config.get('FILES', 'NAMES')).split()

        # Location of files
        self.processed_zone = config.get('FOLDER', 'PROCESSED_ZONE')
        self.working_zone = config.get('FOLDER', 'WORKING_ZONE')

        self.rcm = RemindoCopyModule()

        #TODO: finish the engine configuration using the data from UU
        # self.engine = create_engine(
        #     'oracle+cx_oracle://Remindo:Hog14GHln@its-w-s521.soliscom.uu.nl:1521/DWHO',
        #     max_identifier_length=30,
        #     echo=True)
        #Remindo/Hog14GHln@//its-w-s521.soliscom.uu.nl:1521/DWHO

        self.enginge = create_engine(
            'postgresql+psycopg2://airflow:airflow@localhost/airflow',
            echo=True
        )
        
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Create session and cursor to talk to the db
        self.conn = self.engine.connect()
        
        # List of files to retrieve
        self.modules = OrderedDict([
            ("clusters.csv", Cluster),
            ("studies.csv", Study),
            ("recipes.csv", Recipe),
            ("moments.csv", Moment),
            ("moment_results.csv", MomentResult),
            ("stats.csv", Stat),
            ("items.csv", Item)
        ])

        # self.con = cx_Oracle.connect("pythonhol/welcome@127.0.0.1/".format(*config['CLUSTER'].values()))
        # self.dsn_tns = cx_Oracle.makedsn('localhost', 1521, 'XE')
        # self.con2 = cx_Oracle.connect('hr', 'hrpwd', self.dsn_tns)
        # self._cur = self._con.cursor()

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        try:
            yield self.session
            self.session.commit()
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()


    def test_conn(self):
        """Test the connection to the database."""
        print(self.conn.info)

    def delete_staging_tables(self):
        """Delete the previous tables in the staging database."""
        # ATTENTION! 
        # Drops all existing tables!!!
        logging.debug("Drop staging tables.")
        remindo_dwh_base.Base.metadata.drop_all(self.engine)

    def setup_staging_tables(self):
        """Setup the staging database schema."""
        logging.debug("Create staging schema.")
        remindo_dwh_base.Base.metadata.create_all(self.engine, checkfirst=True)

    def load_staging_tables(self):
        """Load the data from csv files to the database."""
        logging.debug("Populating staging tables")

        #files_in_processed_zone = self.rcm.get_files(self.processed_zone)
        files_in_working_zone = self.rcm.get_files(self.working_zone)

        for file in self.modules.keys():
            if file in files_in_working_zone:
                logging.debug(f"Loading table {file}.")
                data = pd.read_csv(str(self.working_zone+"/"+file), delimiter=",")
                data_list = data.to_dict(orient='records')
                for row in data_list:
                    row = self.modules[file](**row)
                    try:
                        self.session.add(row)
                        self.session.commit()
                        self.session.close()
                        # logging.debug("Success.")
                    except SQLAlchemyError as e:
                        self.session.rollback()
                        error = str(e.__dict__['orig'])
                        logging.debug(f"Rolled back. Error: {error}")
                        print(e)
                logging.debug("Finished loading.")
            else:
                logging.debug(f"File {file} not in working zone.")

    def setup_warehouse_tables(self):
        logging.debug("Creating schema for warehouse.")

        # Change schema from staging to warehouse
        self.conn = self.engine.connect().execution_options(
            schema_translate_map= {"staging_schema": "warehouse"}
        )

        # Create same tables as in staging_schema
        remindo_dwh_base.Base.metadata.create_all(self.conn, checkfirst=True)

    def perform_upsert(self):
        logging.debug("Performing upsert operations.")
        # Upsert operation of each of the tables
        for m, c in self.modules.items():
            try:
                remindo_upsert.upsert(self.engine, self.session, self.conn, c)
            except SQLAlchemyError as e:
                self.session.rollback()
                error = str(e.__dict__['orig'])
                logging.debug(f"Rolled back. Error: {error}")
                print(e)
            except NameError:
                logging.debug(f"File {m} not found.")