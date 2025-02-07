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


with open("api_ldo_key.txt", "r") as file:
     api_key = file.read().strip()

LDO_API_URL = "https://www.overstromingsinformatie.nl/auth/"

# Generate api key on de LDO_API_URL website.
LDO_API_KEY = api_key

# %%
# FOR ADMINISTRATION PERMISSION USE THE FOLLOWING. Otherwise you will get a permission feedback
# at the moment you will try to upload the excel file.
# You will need to copy your own data in the webiste *https://www.overstromingsinformatie.nl/auth/) 
# 1. Open the website
# 2. Provide credential to log in
# 3. On the website in the box auth/v1/personalapikeys/ copy your own information as below (line 78 to 83)
# 4. Store the API KEY

# This one I leave it to copy and paste the information in the website

parameters = {
    "scope": "admin",
    "name": "Juan_Test_12",
    "expiry_date": "2025-01-18T10:22:48.008Z",
    "revoked": False,
}

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
        self.token = None
        # self.test_api()
    
    @property #to get health url 
    def health(self):
        return self.url_auth[:-3] + "/health/"  # Health is not under v1.

    @property #to get tenant
    def tenants(self):
        tenat_url = self.url_auth + "v1/tenants/4"
        tenant_hhnk = (requests.get(url = tenat_url, headers=self.headers, auth=("__key__", self.api_key))).json()
        return tenant_hhnk

    # Get Token
    # The token is requiered to get the refresh_token which is going to be use in in this website:  "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
    # That is different from LDO_API_URL
     
    def get_token(self):
        token_url= self.url_auth + "v1/token/"
        self.token = (requests.post(url=token_url, json={"tenant": 4}, auth=("__key__", self.api_key))).json()["refresh"]
        return self.token

    # Get the TokenRefresh
    # If we do not use the refresh token, the api formo the website "https://www.overstromingsinformatie.nl/api/v1/excel-imports?mode=create"
    # will not work. with out that refersh_token API does not work.   
    def get_access_refresh(self):
        url_refresh =self.url_auth + "v1/token/refresh/"
        data_refresh = {"refresh": self.token}
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
    
    def __init__(self, base, create=True):
        
        self.path = base.path
        self.scenario_name = base.name
        self.zip_kb = None
        super().__init__(base, create=create)
        self.zipfile_location = None
      
    def copy_files(self):

        # Folder location from where the scenarios are going to be copied
        output_folder = (r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output")
        def select_folder(output_folder, scenario_name):
            scenario_paths = [
                j for i in Path(output_folder).glob("*/") for j in list(i.glob("*/"))
            ]
            for scenario_path in scenario_paths:
                if scenario_path.name == scenario_name:
                    return scenario_path
            
        scenario_folder = select_folder(output_folder, self.scenario_name)
        breach = Breaches(scenario_folder)
        raster_compress_path = os.path.join(breach.wss.path, "dem_clip.tif")
        netcdf_path = os.path.join(breach.netcdf.path, "results_3di.nc")

        shutil.copy2(netcdf_path, self.path)
        shutil.copy2(raster_compress_path, self.path)

        return(print(f'Scenario {self.scenario_name} has been copy in the folder structure'))
    

    #Create the zip file to uploaded. 
    def zip_files(self):

        # Create name of the zip file
        zip_name = self.scenario_name + ".zip"
        zip_name = zip_name.replace(" ", "_")

        # Set the zip file path
        folder_structure_path= Path(os.path.join(self.path))
        self.zipfile_location = Path(os.path.join(self.path, zip_name))
        

        # Zip the folder to be uploaded
        with zipfile.ZipFile(self.zipfile_location, "w") as zipf:
            # Walk through the folder and add files to the zip file
            for root, dirs, files in os.walk(folder_structure_path):
                for file in files:
                    if file != f"{zip_name}": 
                        # root = r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur'
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, folder_structure_path)
                        zipf.write(
                            file_path,
                            arcname=os.path.join(f"{self.scenario_name}", arcname),
                )
        # Get zipfile size.
        zp = zipfile.ZipFile(f"{self.zipfile_location}")
        size = sum([zinfo.file_size for zinfo in zp.filelist])
        self.zip_kb  = float(size) / 1000  # kB
        print(f"zip file created with size {self.zip_kb} kb") 
        return(self.zipfile_location)    
#%%   

class LDO_API_UPLOAD:

    def __init__(self, metadata_folder_path, refresh_token, scenario_name):
        self.metadata_folder_path =  metadata_folder_path
        self.metadata_file = Path(os.path.join(metadata_folder_path, scenario_name +'.xlsx'))
        self.refresh_token = refresh_token
        self.headers_excel =  {
            "accept": "application/json",
            "authorization": f"Bearer {self.refresh_token }",
            # 'content-type':'multiplart/form-data',
        }
        self.url_uploadfile = "https://www.overstromingsinformatie.nl/api/v1/excel-imports"
        self.scenario_id = None
        self.id_excel = None
        

    # UPLOAD EXCEL FILE OF THE SCENARIO and retrieve id of the excel upload. In case there is an
    #error y will print the reason. 
    def upload_excel(self):
    
        excel_import_url = self.url_uploadfile+"?mode=create"
        excel_name = self.metadata_file.name
        with open(self.metadata_file, "rb") as excel_files:
            excel_files = {
                "file": (
                    f"{excel_name}",
                    excel_files,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }
            excel_response = requests.post(
                url=excel_import_url, headers=self.headers_excel, files=excel_files
            )
        response_json = json.loads(excel_response.content.decode("utf-8"))
        if response_json.__contains__('message'):
            msg = response_json['detail'][0]['msg']
            print(f'The excel file has a error, reason {msg}')

        else:
            status = response_json['status']
            self.id_excel = response_json["id"]
            self.scenario_id = response_json["scenario_ids"][0]
            print(f'The excel file is {status},  and has been uploaded with id_excel number: {self.id_excel}')
        return(response_json)
    
    #Upload the zip file using the excel ID.
    def upload_zip_files(self, zipfile_location):
        # With this link the zip file is not going to be uploaded. 
        zip_name = zipfile_location.name
        file_import_url = self.url_uploadfile+ f"/{self.id_excel}/files/{zip_name}/upload"

        # Create link to upload zip file
        response = requests.put(url=file_import_url, headers= self.headers_excel)
        upload_url = response.json()["url"]

         # upload data using link
        with open(f"{zipfile_location}", "rb") as data:
            r = requests.put(upload_url, data=data)
        print(r.status_code)
        print(r.reason)
        print("uploading")
        print(file_import_url)
        return (print(f"the scenario {zip_name} has been uploaded"))

# %%
if __name__ == "__main__":
    # Set Paths from the data to be uploaded

    # Excel files per scenario.
    metadata_folder = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\metadata_per_scenario"
    
    # Excel file where the ID and size of the upload is going to be stored
    id_scenarios = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\scenarios_ids.xlsx"    

    # Folder location where the scenarios are going to be copied
    ldo_structuur_path = (r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur")
    
    #List the scenario name to be uploaded
    scenario_names = os.listdir(metadata_folder)

    # data frame from the scenarios that are gonig to be uploaded.
    pd_scenarios = pd.read_excel(id_scenarios)

    #Select scenarios ids that area already uploaded to be skiped
    scenario_done = pd_scenarios.loc[pd_scenarios["ID_SCENARIO"] > 0, "Naam van het scenario"].to_list()
    
    # Sleep time to not burn out the API
    sleeptime = 420

    # Create a list to delete scenarios already uploaded.
    delete_file = []

    # Loop over al the scenarios
    for excel_file_name in scenario_names:

        #Set Scenario Name
        scenario_name = excel_file_name[:-5] 
#%%
        # If the scenario is done the continue
        if scenario_name in scenario_done:
            continue
        else:
            # Set folder with scenario name to be uploaded to LDO
            path = Folder(os.path.join(ldo_structuur_path, scenario_name))

            # Create Folder as Oboject
            ldo_structuur = SelectFolder(path)

            # Copy file NetCDF and DEM inside the previous folder.
            ldo_structuur.copy_files()

            # Zip the file and retrieve the path of its location
            ldo_structuur.zip_files()

            # Set API key 
            ldo_api = LDO_API_AUTH(url_auth=LDO_API_URL, api_key=LDO_API_KEY)

            # Retrieve the refress token to be able to upload the info. 
            refresh = ldo_api.get_access_refresh()

            # Set UPLOAD as an object 
            ldo_upload = LDO_API_UPLOAD(metadata_folder, refresh, scenario_name)

            # get metadata file of the scenario that is been uploaded
            metadata_file = ldo_upload.metadata_file
            
            # Upload excel file from the scenario, and retrieve json infomration
            excel_response = ldo_upload.upload_excel()

            # Get size of the zip folder. 
            zip_size  = ldo_structuur.zip_kb
            # store scenario id in the metadata
            scenario_id = ldo_upload.scenario_id
            print(scenario_id)

            # Upload zip file using the 
            ldo_upload.upload_zip_files(ldo_structuur.zipfile_location)
            time.sleep(sleeptime) 
            
            # Save the id of upload from the scenario 
            pd_scenarios.loc[pd_scenarios["Naam van het scenario"] == scenario_name, "ID_SCENARIO"] = scenario_id
            
            # Save  the size of the scenario in the metdata dataframe
            pd_scenarios.loc[pd_scenarios["Naam van het scenario"] == scenario_name, "SIZE_KB"] = zip_size

            # REMOVE/DELETE ZIP AND FOLDER FROM THE SCENARIO THAT IS ALREADY UPLOADED.
            delete_file.append(ldo_structuur.path)

            if len(delete_file) > 1:
               previous_folder = delete_file.pop(0)
               shutil.rmtree(previous_folder)

            print(f"the scenario {scenario_name} has been uploaded")

            # Save the excel file.
            pd_scenarios.to_excel(id_scenarios, index=False, engine="openpyxl")
    # %%
