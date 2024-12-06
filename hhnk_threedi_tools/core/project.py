from folders import Folders
import json
from pathlib import Path
import time

# A class for project
class Project:
    def __init__(self, folder: str):
        self.folder = folder # set project folder

        if Path(folder).exists():
            self.folders = Folders(folder, create=False) # set folders instance for existing project folder
        else:
            Path(folder).mkdir(parents=True, exist_ok=True)
            self.folders = Folders(folder, create=True) # create folders instance for new project folder

        if self.folders.project_json.exists():
            self.load_from_json() # load variables from json if exists (fixed filename)
        else: 
            self.initialise_new_project() # initialise new project
            self.save_to_json()

    def initialise_new_project(self):
        """ Create all Project variables for a new project """
        self.project_status = 0
        self.project_name = str(Path(self.folder).name)
        self.project_folder = self.folder

    def update_project_status(self, status):
        self.project_status = status
        self.save_to_json()

    def retrieve_project_status(self):
        return self.project_status

    def save_to_json(self):
        self.project_date = str(time.strftime("%Y-%m-%d %H:%M:%S")) # update project date
        with open(self.folders.project_json.path, 'w') as f:
            json.dump(self.__dict__, f)

    def load_from_json(self):
        with open(self.folders.project_json.path, 'r') as f:
            data = json.load(f)
            self.__dict__.update(data)



