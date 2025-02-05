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
from hhnk_research_tools import Folder
import pandas as pd
import requests
from breaches import Breaches

LDO_API_URL = "https://www.overstromingsinformatie.nl/auth/"

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

#%%
class LDO_API_AUTH:
    def __init__(self, url_auth, api_key):
        self.url_auth = url_auth
        self.api_key = api_key
        self.headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "X-CSRFToken": "lIiP686oF2VRs9iXgtLDxKRdqBUBzHSPS19M3MZVERhlTVhZOzNXeCciUERzVuMA",
         }
        # self.test_api()
    
    @property #to get health url 
    def health(self):
        return self.url_auth[:-3] + "/health/"  # Health is not under v1.

    @property #to get tenant
    def tenants(self):
        tenat_url = self.url_auth + "v1/tenants/4"
        tenant_hhnk = (requests.get(url = tenat_url, headers=self.headers, auth=("__key__", self.api_key))).json()
        return tenant_hhnk

    # The token is requiered to get the refresh_token which is going to be use in in this website:  "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
    @property 
    def token(self):
        token_url= self.url_auth + "v1/token/"
        token = (requests.post(url=token_url, json={"tenant": 4}, auth=("__key__", self.api_key))).json()["refresh"]
        return token
    
    def get_access_refresh(self, token):
        url_refresh =self.url_auth + "v1/token/refresh/"
        data_refresh = {"refresh": token}
        access = (requests.post(url=url_refresh, json = data_refresh,  auth=("__key__", self.api_key))).json()["access"]
        return access
    
    def test_api(self):
        """Test api connection"""
        response_health = requests.get(url=self.health)
        assert response_health.status_code == 200
#%%

FOLDER_STRUCTURE = """
    Main Folder object
        ├── dem.tif
        ├── results_3di.nc
    """        
#%%
class SelectFolder(Folder):
    __doc__ = f"""
        --------------------------------------------------------------------------
        An object to ease the accessibility, creation and checks of folders and
        files that need to be uploaded to LDO. 
         {FOLDER_STRUCTURE}
        """
    
    def __init__(self, base,  create=True):
        super().__init__(base, create=create)
        self.path = base.path



    # @classmethod
    # def is_valid(self):
    #     """Check if folder stucture is available in input folder."""
    #     return (Path(self.path).joinpath().exists())
      
    @classmethod
    def copy_files(self, folder_structure_path, scenario_name):
        def select_folder(output_folder, scenario_name):
            scenario_paths = [
                j for i in Path(output_folder).glob("*/") for j in list(i.glob("*/"))
            ]
            for scenario_path in scenario_paths:
                if scenario_path.name == scenario_name:
                    return scenario_path
            
        scenario_folder = select_folder(output_folder, scenario_name)
        breach = Breaches(scenario_folder)
        raster_compress_path = os.path.join(breach.wss.path, "dem_clip.tif")
        netcdf_path = os.path.join(breach.netcdf.path, "results_3di.nc")

        shutil.copy2(netcdf_path, folder_structure_path)
        shutil.copy2(raster_compress_path, folder_structure_path)

        return(print(f'Scenario {scenario_name} has been copy in the folder structure'))
    
    @classmethod
    def zip_files(self, base, scenario_name):

        # Create name of the zip file
        zip_name = scenario_name + ".zip"
        zip_name = zip_name.replace(" ", "_")

        # Set the zip file path
        folder_structure_path= Path(os.path.join(base, scenario_name))
        zipfile_location = Path(os.path.join(base, scenario_name, zip_name))
        

        # Zip the folder to be uploaded
        with zipfile.ZipFile(zipfile_location, "w") as zipf:
            # Walk through the folder and add files to the zip file
            for root, dirs, files in os.walk(folder_structure_path):
                for file in files:
                    if file != f"{zip_name}": 
                        # root = r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur'
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, folder_structure_path)
                        zipf.write(
                            file_path,
                            arcname=os.path.join(f"{scenario_name}", arcname),
                )
        # Get zipfile size.
        zp = zipfile.ZipFile(f"{zipfile_location}")
        size = sum([zinfo.file_size for zinfo in zp.filelist])
        zip_kb = float(size) / 1000  # kB
        print(f"zip file created with size {zip_kb} kb") 
        return(zipfile_location)    
#%%   

scenario_done = pd_scenarios.loc[pd_scenarios["ID_SCENARIO"] > 0, "Naam van het scenario"].to_list()
#%%
class LDO_API_UPLOAD:

    def __init__(self, metadata_folder_path, refresh_token, scenario_name):
        self.metadata_folder_path =  metadata_folder_path
        self.metadata_file = Path(os.path.join(metadata_folder, scenario_name +'.xlsx'))
        self.refresh_token = refresh_token
        self.headers_excel =  {
            "accept": "application/json",
            "authorization": f"Bearer {self.refresh_token }",
            # 'content-type':'multiplart/form-data',
        }
        self.url_uploadfile = "https://www.overstromingsinformatie.nl/api/v1/excel-imports"
    @classmethod
    def upload_excel(self, metadata_file, url_uploadfile, headers_excel):
        """"""
        # UPLOAD EXCEL FILE OF THE SCENARIO and retrieve id of the excel upload. In case there is an
        #error y will print the reason. 
     
        excel_import_url = url_uploadfile+"?mode=create"
        excel_name = metadata_file.name
        with open(metadata_file, "rb") as excel_files:
            excel_files = {
                "file": (
                    f"{excel_name}",
                    excel_files,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }
            excel_response = requests.post(
                url=excel_import_url, headers=headers_excel, files=excel_files
            )
        response_json = json.loads(excel_response.content.decode("utf-8"))
        if response_json.__contains__('message'):
            msg = response_json['detail'][0]['msg']
            print(f'The excel file has a error, reason {msg}')

        else:
            status = response_json['status']
            id_excel = response_json["id"]
            print(f'The excel file is {status},  and has been uploaded with id_excel number: {id_excel}')
        return(response_json)
    
    @classmethod
    def upload_zip_files(self, url_uploadfile, id_excel, zipfile_location, headers_excel):
        # With this link the zip file is not going to be uploaded. 
        zip_name = zipfile_location.name
        file_import_url = url_uploadfile+ f"/{id_excel}/files/{zip_name}/upload"

        # Create link to upload zip file
        response = requests.put(url=file_import_url, headers= headers_excel)
        upload_url = response.json()["url"]

         # upload data using link
        with open(f"{zipfile_location}", "rb") as data:
            r = requests.put(upload_url, data=data)
        print(r.status_code)
        print(r.reason)
        print("uploading")
        print(file_import_url)
        return (print(f"the scenario {zip_name} has been uploaded"))
#%%
        # GET RESPONSE.
        # print(excel_response.status_code)

# TODO  There are 3 diferent links from 2 website to work with. One the webstie use to retrieve the APIKEY is (authorization website):
# https://www.overstromingsinformatie.nl/auth and the other two that are more related to the core of the API
# https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create and f"https://www.overstromingsinformatie.nl/api/v1/excel-imports/{id_excel}/files/{zip_name}/upload"
# The first one to upload the excel file and the other one to upload the data

# %%


# %%
# Get Token
# TODO why do we need this?
# The token is requiered to get the refresh_token which is going to be use in in this website:  "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
# That is different from LDO_API_URL



# Get the TokenRefresh
# TODO why do we need this??
# If we do not use the refresh token, the api formo the website "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
# will not work. with out that refersh_token API does not work.


# %%


# function to select folder from which the info is goin to be copied
# TODO is this necessary? shouldnt the scenario_path be provided already? This is the first time
# the output_folder is used, bit unclear. Would expect this to be in the excel you ge tit from.


# They way I structure the code is that I use the scenario_name from the excel files, to locate the
# scenario in the output_folder. From that folder I copy DEM and results.nc in the ldo_structuur folder.


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
        
        # store scenario id in the metadata
        scenario_id = response_json["scenario_ids"][0]
        pd_scenarios.loc[
            pd_scenarios["Naam van het scenario"] == scenario_name, "ID_SCENARIO"
        ] = scenario_id

        
        # Set sleep time while the folder is zipped.
        time.sleep(50)  # TODO Why?

        # copy the size of the scenario in the metdata dataframe
        pd_scenarios.loc[
            pd_scenarios["Naam van het scenario"] == scenario_name, "SIZE_KB"
        ] = zip_kb

        

        # REMOVE/DELETE ZIP AND FOLDER FROM THE SCENARIO THAT IS ALREADY UPLOADED.
        delete_file.append(scenario_folder_structuur)

        if len(delete_file) > 1:
            previous_folder = delete_file.pop(0)
            shutil.rmtree(previous_folder)

        print(f"the scenario {scenario_name} has been uploaded")

        # Save the excel file.
        pd_scenarios.to_excel(id_scenarios, index=False, engine="openpyxl")

    # %%
