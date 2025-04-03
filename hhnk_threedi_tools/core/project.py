import json
import time
from pathlib import Path

from hhnk_threedi_tools.core.folders import Folders


# A class for project
class Project:
    def __init__(self, folder: str):
        self.project_folder = folder  # set project folder
        folders = Folders(folder, create=True)  # create folders instance for new project folder

        self.json_path = str(folders.project_json.path)
        if folders.project_json.exists():
            self.load_from_json(self.json_path)  # load variables from json if exists (fixed filename)
        else:
            self.initialise_new_project()  # initialise new project
            self.save_to_json(self.json_path)

    def initialise_new_project(self):
        """Create all Project variables for a new project"""
        self.project_status = 0
        self.project_name = str(Path(self.project_folder).name)

    def update_project_status(self, status):
        self.project_status = status
        self.save_to_json(self.json_path)

    def retrieve_project_status(self):
        return self.project_status

    def save_to_json(self, filepath):
        self.project_date = str(time.strftime("%Y-%m-%d %H:%M:%S"))  # update project date
        with open(filepath, "w") as f:
            json.dump(self.__dict__, f)

    def load_from_json(self, filepath):
        with open(filepath, "r") as f:
            data = json.load(f)
            self.__dict__.update(data)
