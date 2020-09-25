import shutil
import logging
import configparser
from pathlib import Path
from os import listdir, remove
from os.path import isfile, join

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# TODO: Clean source?

class RemindoCopyModule:

    def __init__(self):
        self._files = []
        self._landing_zone = config.get('FOLDER','LANDING_ZONE')
        self._working_zone = config.get('FOLDER','WORKING_ZONE')
        self._processed_zone = config.get('FOLDER','PROCESSED_ZONE')

    def move_data(self, source_folder = None, target_folder= None):
        if source_folder is None:
            source_folder = self._landing_zone
        if target_folder is None:
            target_folder = self._working_zone

        logger.debug(f"Inside move_data: Source folder set is : {source_folder}\n Target set is: {target_folder}")

        # Clean the target folder
        self.clean_folder(target_folder)

        # Move files to target folder
        for csv in self.get_files(source_folder):
            if csv in config.get('FILES','NAME').split(","):
                logger.debug(f"Copying file {csv} from {source_folder} to {target_folder}")
                shutil.move(join(source_folder,csv), target_folder)

    def get_files(self, folder_name):
        logger.debug(f"Inspecting folder: {folder_name} for files present")
        return [csv for csv in listdir(folder_name) if isfile(join(folder_name, csv))]

    def clean_folder(self, folder_name):
        logger.debug(f"Cleaning folder: {folder_name}")
        for csv in self.get_files(folder_name):
            remove(join(folder_name,csv))
