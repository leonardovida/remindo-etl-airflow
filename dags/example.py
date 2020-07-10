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

def main():
    logging.config.fileConfig(f"{Path(__file__).parents[1]}/config/logging.ini")
    logger = logging.getLogger(__name__)


if __name__ == "__main__":
    main()
    logging.debug("Finished data retrival. Exiting.")
