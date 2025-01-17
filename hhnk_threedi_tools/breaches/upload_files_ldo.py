"""
Upload 3Di results to LDO
Created: 2024-11-10
Author: Juan Acosta for Hoogheemraadschap Hollands Noorderkwartier

Description:
Collecting, Zipping, and Uploading 3Di Results into LDO for Flood Calculation

The script processes each scenario individually, zipping and uploading the result.nc files for flood
calculations into LDO. To do this, you need to have the metadata for each scenario stored separately.
In our case, we have 495 Excel files, each corresponding to a different scenario.

To upload a scenario, the script first uploads the corresponding Excel file, which must be named
after the scenario you want to upload. It then searches in the output_folder for the uploaded metadata
and copies the scenarios information to be uploaded from the output_folder  into the ldo_structuur folder.
Following the next structure

├── ldo_structuur
    ├── ROR-PRI-WATERLANDSE_ZEEDIJK_1.5_2-T100000
        ├── dem_clip.tif
        ├── results_3di.nc

Next, the script creates a zip file and uploads it into the LDO. In the subsequent loop, it deletes
the folder, in this case ROR-PRI-WATERLANDSE_ZEEDIJK_1.5_2-T100000 and all the file contents on it
to prevent overloading the computer with information. The final structure of the scenario folder to
be uploaded should follows the next structure

├── ldo_structuur
    ├── ROR-PRI-WATERLANDSE_ZEEDIJK_1.5_2-T100000
        ├── dem_clip.tif
        ├── results_3di.nc
        ├── ROR-PRI-WATERLANDSE_ZEEDIJK_1.5_2-T100000.zip

Once a scenario is uploaded, the script stores the upload ID in an Excel file. This excel file must
have two columns: ID_SCENARIO and SIZE_KB.

Remarks:
This code upload in a zip file: DEM and a file result.nc and a excel file.

You must set up the excel metadata file name the same as the scenario zip file your are planning to
uplod.

Currently it is uploading more than 2 GB. In case that you re going to upload more than 2 GB,
try to check after it upload the information to ensure that the data is correctly uploaded.

Dem is aggregated to 5x5 results created at 0.5x0.5m

It is important to set a sleep time so the API wil not be overloaded
"""

# %%
import json
import os
import shutil
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from breaches import Breaches

LDO_API_URL = "https://www.overstromingsinformatie.nl/auth/v1"

# Generate api key on de LDO_API_URL website.
LDO_API_KEY = ""

# %%
# FOR ADMINISTRATION PERMISSION USE THE FOLLOWING. Otherwise you will get a permission feedback
# at the moment you will try to upload the excel file.
# You will need to copy your own data in the webiste *https://www.overstromingsinformatie.nl/auth/) #TODO how does this work?
# 1. Open the website
# 2. Provide credential to log in
# 3. On the website in the box auth/v1/personalapikeys/ copy the information that is below (line 44 to 49)
# 4. Copy the API key in line 30

# TODO you dont need this here?
# This one I leave it to copy and paste the information in the website


parameters = {
    "scope": "admin",
    "name": "Juan_Test_12",
    "expiry_date": "2025-01-18T10:22:48.008Z",
    "revoked": False,
}


# Set Paths from the data to be uploaded
# TODO provide example.
# TODO make class of the ldo_structuur_folder? unclear how its structured now

# Excel files per scenario.
metadata_folder = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\metadata_per_scenario"
# Folder location from where the scenarios are going to be copied
output_folder = (
    r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output"
)

# Folder location to copy the scenarios
ldo_structuur_folder = (
    r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur"
)


# Excel file where the ID and size of the upload is going to be stored
id_scenarios = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\scenarios_ids.xlsx"
# Why user input?
# Those ID are needed to be store for multiple reasons:
# 1 . In case we need to delete the scenario ID it is going to be easy to simply store them and then delete them with the
# script delete_scenarios
# 2. With that id I can create easily the website that takes you directly to the scenario you want to check. Examplel
# https://www.overstromingsinformatie.nl/scenarios/106063
# for that path the patern is very easy: https://www.overstromingsinformatie.nl/scenarios/  + ID
# 3. I that folder I am storing also the size of the zipfile to be uploaded. That help me to check those scenarios
# that has more than 2 GB size. Beacuse according to LDO, the API is not going to be able to upload those scenarios
# with a bigger size than 2GB. thanksfully they all worked even if they where bigger than 2 GB
# 4. It make easy to locate them in the website.

# Open the excel file as pandas dataframe
pd_scenarios = pd.read_excel(id_scenarios)


class LDO_API:
    def __init__(self, url, key):
        self.url = url
        self.key = key

        self.test_api()

    @property
    def health(self):
        return self.url[:-3] + "/health/"  # Health is not under v1.

    @property
    def tenants(self):
        return self.url + "/tenants/"

    @property
    def token(self):
        return self.url + "/token/"

    def get(self, url):
        """Get request with authorisation"""
        return requests.get(url=url, auth=("__key__", self.key))

    def test_api(self):
        """Test api connection"""
        response_health = requests.get(url=self.health)
        assert response_health.status_code == 200


# TODO  There are 3 diferent links from 2 website to work with. One the webstie use to retrieve the APIKEY is (authorization website):
# https://www.overstromingsinformatie.nl/auth and the other two that are more related to the core of the API
# https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create and f"https://www.overstromingsinformatie.nl/api/v1/excel-imports/{id_excel}/files/{zip_name}/upload"
# The first one to upload the excel file and the other one to upload the data
self = ldo_api = LDO_API(url=LDO_API_URL, key=LDO_API_KEY)

# %%
# Check Tenants
response_tenants = requests.get(url=ldo_api.tenants, auth=("__key__", self.key))
print(response_tenants.json())

# %%
# Get Token
# TODO why do we need this?
# The token is requiered to get the refresh_token which is going to be use in in this website:  "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
# That is different from LDO_API_URL
r_token = requests.post(
    url=ldo_api.token, json={"tenant": 4}, auth=("__key__", LDO_API_KEY)
)
print(r_token.json())
refresh = r_token.json()["refresh"]

# Get the TokenRefresh
# TODO why do we need this??
# If we do not use the refresh token, the api formo the website "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
# will not work. with out that refersh_token API does not work.
access = r_token.json()
refresh_url = f"{ldo_api.token}refresh/"
data_refresh = {"refresh": r_token.json()["refresh"]}
response_refresh = requests.post(
    url=refresh_url, json=data_refresh, auth=("__key__", LDO_API_KEY)
)
response_refresh = response_refresh.json()
refresh_token = response_refresh["access"]
print(response_refresh)

# %%


# function to select folder from which the info is goin to be copied
# TODO is this necessary? shouldnt the scenario_path be provided already? This is the first time
# the output_folder is used, bit unclear. Would expect this to be in the excel you ge tit from.


# They way I structure the code is that I use the scenario_name from the excel files, to locate the
# scenario in the output_folder. From that folder I copy DEM and results.nc in the ldo_structuur folder.
def select_folder(scenario_name_path):
    scenario_paths = [
        j for i in Path(output_folder).glob("*/") for j in list(i.glob("*/"))
    ]
    for scenario_path in scenario_paths:
        if scenario_path.name == scenario_name:
            return scenario_path


scenario_names = os.listdir(metadata_folder)

# Sleep time to not burn out the API
sleeptime = 420
# %%
# Create a list to delete scenarios already uploaded.
delete_file = []

# check if scenario is done
scenario_done = pd_scenarios.loc[
    pd_scenarios["ID_SCENARIO"] > 0, "Naam van het scenario"
].to_list()
# %%
# TODO wvg
# heb wat moeite met deze loop. Kan het niet testen omdat ik geen data heb
# maar er wordt nogal wat heen enweer geschoven met bestanden in een
# Separate the local moving and zipping of files from the API interaction. You dont need
# to post the excel before everything is set up locally right?
# Yes I need to do that, because once I load the excel file I use the excel_id of the upload
# to upload the zipfile. That is why I do this id_excel = response_json["id"]. Check comment line 236

# I am copy one excel file per scenario. I can not upload just the whole metadata and then the scenarios
# Each scenario must have its onw excel metedata file.

# They restructure of the code is not going to work becuse it is using only 1 link.
# TODO wvg
# Loop over al the scenarios
for excel_file_name in scenario_names:
    # If the scenario is done the continue
    if excel_file_name[:-5] in scenario_done:
        continue
    else:
        # Set metadata file location
        metadata_location = os.path.join(metadata_folder, excel_file_name)

        # Get the name from the scenario.
        scenario_name = Path(metadata_location).stem
        print(f"uploading scenario {scenario_name}")

        # UPLOAD EXCEL FILE OF THE SCENARIO
        # With this link the excel file is not going to be uploaded. Check comment in line 119
        excel_import_url = f"{ldo_api.url}/excel-imports?mode=create"

        headers_excel = {
            "accept": "application/json",
            "authorization": f"Bearer {refresh_token}",
            # 'content-type':'multiplart/form-data',
        }

        with open(metadata_location, "rb") as excel_files:
            excel_files = {
                "file": (
                    f"{excel_file_name}",
                    excel_files,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }
            excel_response = requests.post(
                url=excel_import_url, headers=headers_excel, files=excel_files
            )

        # GET RESPONSE.
        print(f"The excel file for the scenario {scenario_name} has been uploaded")

        # GET THE ID OF THE IMPORT --> This one (the id) is needed to upload the zip file.
        response_json = json.loads(excel_response.content.decode("utf-8"))
        id_excel = response_json["id"]

        # store scenario id in the metadata
        scenario_id = response_json["scenario_ids"][0]
        pd_scenarios.loc[
            pd_scenarios["Naam van het scenario"] == scenario_name, "ID_SCENARIO"
        ] = scenario_id

        print(f"Uploading scenario {scenario_name} with uploading id:{scenario_id}")

        # Create folder in ldo_structuur location folder
        scenario_folder_structuur = os.path.join(ldo_structuur_folder, scenario_name)
        if not os.path.exists(scenario_folder_structuur):
            os.makedirs(scenario_folder_structuur)

        # SELECT FOLDER AND FILES TO BE COPIED IN THE LDO SCTRUCTUUR folder
        scenario_folder = select_folder(scenario_name)
        breach = Breaches(scenario_folder)
        raster_compress_path = os.path.join(breach.wss.path, "dem_clip.tif")
        netcdf_path = os.path.join(breach.netcdf.path, "results_3di.nc")

        # #COPY FILES in LDO FOLDER STRUCUTRE
        shutil.copy2(netcdf_path, scenario_folder_structuur)
        shutil.copy2(raster_compress_path, scenario_folder_structuur)

        # Create name of the zip file
        zip_name = scenario_name + ".zip"
        zip_name = zip_name.replace(" ", "_")

        # Set the zip file path
        zipfile_location = os.path.join(ldo_structuur_folder, scenario_name, zip_name)
        zipfile_location_name = Path(zipfile_location).stem

        # Zip the folder to be uploaded
        with zipfile.ZipFile(zipfile_location, "w") as zipf:
            # Walk through the folder and add files to the zip file
            for root, dirs, files in os.walk(scenario_folder_structuur):
                for file in files:
                    if file != f"{zip_name}":  # Avoid adding the zip file itself
                        # root = r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur'
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, scenario_folder_structuur)
                        zipf.write(
                            file_path,
                            arcname=os.path.join(f"{zipfile_location_name}", arcname),
                        )

        print(
            f"Folder '{scenario_folder_structuur}' has been zipped successfully into '{scenario_folder_structuur}'."
        )
        # Set sleep time while the folder is zipped.
        time.sleep(50)  # TODO Why?

        # Get zipfile size.
        zp = zipfile.ZipFile(f"{zipfile_location}")
        size = sum([zinfo.file_size for zinfo in zp.filelist])
        zip_kb = float(size) / 1000  # kB
        print(f"zip file created with size {zip_kb} kb")

        # copy the size of the scenario in the metdata dataframe
        pd_scenarios.loc[
            pd_scenarios["Naam van het scenario"] == scenario_name, "SIZE_KB"
        ] = zip_kb

        # UPOLOAD ZIP FILES TO LDO
        # TODO Create functions
        # With this link the zip file is not going to be uploaded. Check comment in line 119
        file_import_url = (
            f"{ldo_api.url}/excel-imports/{id_excel}/files/{zip_name}/upload"
        )
        headers_excel = {
            "accept": "application/json",
            "authorization": f"Bearer {refresh_token}",
        }

        # Create link to upload zip file
        response = requests.put(url=file_import_url, headers=headers_excel)
        upload_url = response.json()["url"]
        print(file_import_url)

        # use link to upload data using link
        with open(f"{zipfile_location}", "rb") as data:
            r = requests.put(upload_url, data=data)
        print(r.status_code)
        print(r.reason)
        print("uploading")
        time.sleep(sleeptime)

        # REMOVE/DELETE ZIP AND FOLDER FROM THE SCENARIO THAT IS ALREADY UPLOADED.
        delete_file.append(scenario_folder_structuur)

        if len(delete_file) > 1:
            previous_folder = delete_file.pop(0)
            shutil.rmtree(previous_folder)

        print(f"the scenario {scenario_name} has been uploaded")

        # Save the excel file.
        pd_scenarios.to_excel(id_scenarios, index=False, engine="openpyxl")

    # %%
