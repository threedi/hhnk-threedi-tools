"""
Upload 3Di results to LDO
Created: 2024-11-10
Author: Juan Acosta for Hoogheemraadschap Hollands Noorderkwartier

Description:
Collect, zip and upload 3Di results into LDO for result of flood calculation


Remarks:
Maximum upload is 2 Gb
Dem is aggregated to 5x5 results created at 0.5x0.5m
Etc.
"""

# %%
import requests
from pathlib import Path
import json
import os
from breaches import Breaches
import shutil
import zipfile
import time as timesleep
import pandas as pd

# %%
# Test API connection
# Copy the hearders from the swagger website.
health = "https://www.overstromingsinformatie.nl/auth/health/"
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "X-CSRFToken": "u64z4jugD7ojNi1DPtuSRdAoSi6nG5fjJB1Q7HclZgCKubqHhqUqPgyjQXb0MvPD",
}
response_health = requests.get(url=health, headers=headers)
print(response_health.json())

# %%
well_know = "https://www.overstromingsinformatie.nl/auth/.well-known/jwks.json"
well_know_response = requests.get(url=well_know, headers=headers)
print(well_know_response.json())
# %%
# FOR ADMINISTRATION PERMISION USE THE FOLLOWING. Otherwise you will get a permission feedback
# at the moment you will try to upload the excel file.
# You will need to copy your own data in the webiste *https://www.overstromingsinformatie.nl/auth/)
# to retrieve de API KEY

parameters = {
    "scope": "admin",
    "name": "Juan_Test_12",
    "expiry_date": "2025-01-18T10:22:48.008Z",
    "revoked": False,
}

# Copy here the API key generated on the website
api_key_10_07_24 = "You must place here the API KEY"
# %%
# Check Tenants
tenants = "https://www.overstromingsinformatie.nl/auth/v1/tenants/"
response_tenants = requests.get(
    url=tenants, headers=headers, auth=("__key__", api_key_10_07_24)
)
print(response_tenants.json())

# %%
# Get Token
token_url = "https://www.overstromingsinformatie.nl/auth/v1/token/"
response_5 = requests.post(
    url=token_url, json={"tenant": 4}, auth=("__key__", api_key_10_07_24)
)
print(response_5.json())
refresh = response_5.json()["refresh"]

# Get the TokenRefresh
access = response_5.json()
refresh_url = "https://www.overstromingsinformatie.nl/auth/v1/token/refresh/"
data_refresh = {"refresh": response_5.json()["refresh"]}
response_refresh = requests.post(
    url=refresh_url, json=data_refresh, auth=("__key__", api_key_10_07_24)
)
response_refresh = response_refresh.json()
refresh_token = response_refresh["access"]
print(response_refresh)

# %%

#Set Paths from the data to be uploaded

# Excel files per scenario.
metadata_folder = 
# Folder location from where the scenarios are going to be copy
output_folder = 

# Folder location to copy the Scenarios
ldo_structuur_folder =

# Excel file where the ID and size of of the upload is going to be store
id_scenarios =

# Open the excel file as pandas dataframe
pd_scenarios = pd.read_excel(id_scenarios)
# pd_scenarios_id = pd.read_excel(id_scenarios_v2)


# function to select folder from which the info is goin to be copy
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
        excel_import_url = refresh_url = (
            "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
        )

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
        print(f"the excel file for the scenario {scenario_name} has been uploaded")

        # GET THE ID OF THE IMPORT --> This one (the id) is needed to upload the zip file.
        response_json = json.loads(excel_response.content.decode("utf-8"))
        id_excel = response_json["id"]

        # store scenario id in the metadata
        scenario_id = response_json["scenario_ids"][0]
        pd_scenarios.loc[
            pd_scenarios["Naam van het scenario"] == scenario_name, "ID_SCENARIO"
        ] = scenario_id

        print(f"uploading scenario {scenario_name} with uploading id:{scenario_id}")

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
        zip_initial_name = scenario_name + ".zip"

        # DELETE SPACES IN THE ZIP_NAME if it contains spaces
        if zip_initial_name.__contains__(" "):
            zip_name = zip_initial_name.replace(" ", "_")
        else:
            zip_name = zip_initial_name

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
        timesleep.sleep(50)

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
        file_import_url = f"https://www.overstromingsinformatie.nl/api/v1/excel-imports/{id_excel}/files/{zip_name}/upload"
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
        timesleep.sleep(sleeptime)

        # REMOVE/DELETE ZIP AND FOLDER FROM THE SCENARIO THAT IS ALREADY UPLOADED.
        delete_file.append(scenario_folder_structuur)

        if len(delete_file) > 1:
            previous_folder = delete_file.pop(0)
            shutil.rmtree(previous_folder)

        print(f"the scenario {scenario_name} has been uploaded")

        # Save the excel file.
        with pd.ExcelWriter(id_scenarios, engine="openpyxl") as writer:
            pd_scenarios.to_excel(writer, index=False)

    # %%
