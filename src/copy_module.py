import shutil
import logging
from logging.config import fileConfig
import configparser
from pathlib import Path
from os import listdir, remove
from os.path import isfile, join, dirname, abspath

from loguru import logger

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# TODO: Clean source?


class RemindoCopyModule:
    """Util class to copy and move data"""

    def __init__(self, landing_zone, working_zone, processed_zone):
        self._files = []
        self._landing_zone = landing_zone
        self._working_zone = working_zone
        self._processed_zone = processed_zone

    def move_data(self, source_folder=None, target_folder=None):
        if source_folder is None:
            source_folder = self._landing_zone
        if target_folder is None:
            target_folder = self._working_zone

        # Retrieve name of folders for logger
        source_folder_name = Path(source_folder).name
        target_folder_name = Path(target_folder).name

        logger.debug(
            f"Moving data: \
            Source folder: {source_folder} \
            Target folder: {target_folder}"
        )

        # First, clean the target folder
        self.clean_folder(target_folder)

        # Second, move files to target folder
        for csv in self.get_files(source_folder):
            if csv in config.get("FILES", "NAME").split(","):
                logger.debug(
                    f"Copying file {csv} from {source_folder_name} \
                        to {target_folder_name}"
                )
                shutil.move(join(source_folder, csv), target_folder)

    def get_files(self, folder):
        folder_name = Path(folder).name
        logger.debug(f"Inspecting folder: {folder_name} for files present")
        return [csv for csv in listdir(folder) if isfile(join(folder, csv))]

    def clean_folder(self, folder):
        folder_name = Path(folder).name
        logger.debug(f"Cleaning folder: {folder_name}")
        for csv in self.get_files(folder):
            remove(join(folder, csv))
