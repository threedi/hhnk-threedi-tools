from folders import Folders
import json
from pathlib import Path
import time

# A class for project
class Project:
    def __init__(self, folder: str):
        self.folder = folder # set project folder

        if Path(folder).exists():
            folders = Folders(folder, create=False) # set folders instance for existing project folder
        else:
            Path(folder).mkdir(parents=True, exist_ok=True)
            folders = Folders(folder, create=True) # create folders instance for new project folder

        if folders.project_json.exists():
            self.load_from_json(folders.project_json.path) # load variables from json if exists (fixed filename)
        else: 
            self.initialise_new_project() # initialise new project
            self.save_to_json(folders.project_json.path)

    def initialise_new_project(self):
        """ Create all Project variables for a new project """
        self.project_status = 0
        self.project_name = str(Path(self.folder).name)
        self.project_folder = self.folder

    def update_project_status(self, status):
        self.project_status = status

    def save_to_json(self, filename):
        self.project_date = str(time.strftime("%Y-%m-%d %H:%M:%S")) # update project date
        with open(filename, 'w') as f:
            json.dump(self.__dict__, f)

    def load_from_json(self, filename):
        with open(filename, 'r') as f:
            data = json.load(f)
            self.__dict__.update(data)



