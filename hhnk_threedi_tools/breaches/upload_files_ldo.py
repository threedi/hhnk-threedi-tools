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

import hhnk_research_tools as hrt
import pandas as pd
import requests
from breaches import Breaches

LDO_API_URL = "https://www.overstromingsinformatie.nl/api/v1/"

# Generate api key on de LDO_API_URL website. And place it in api_ldo_key.txt
LDO_API_KEY = Path("api_ldo_key.txt").read_text()

logger = hrt.logging.get_logger(__name__)

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


# %%
class LDO_API:
    def __init__(self, api_key=LDO_API_KEY, tenant=4, url=LDO_API_URL):
        self.url = url
        self.api_key = api_key
        self.tenant = tenant  # organisation, 4=hhnk.

        # Authorisation goes through the auth endpoint
        self.url_auth = self.url.replace("/api/", "/auth/")

        self._refresh_token = None  # set on calling self.access_token
        self._access_token = None  # Property

    @property
    def access_token(self):
        """Get refresh token so we can interact with the api."""
        if self._refresh_token is None:
            r = requests.post(
                url=self.url_auth + "token/",
                json={"tenant": self.tenant},
                auth=("__key__", self.api_key),
                timeout=5,
            ).json()
            self._refresh_token = r["refresh"]
            self._access_token = r["access"]
        return self._access_token

    def get_tenants(self):
        """Tenant / organisation which has an id and name.

        Prints the tenants. The id can be used to get the token.
        """
        tenant_url = self.url + "v1/tenants/"
        tenants = requests.get(url=tenant_url, auth=("__key__", self.api_key), timeout=5).json()
        for tenant in tenants:
            logger.info(tenant)
        return tenants

    def test_api(self):
        """Test api connection without requiring authorisation"""
        health_url = self.url[:-4] + "/health/"  # Health is not under v1.
        response_health = requests.get(url=health_url, timeout=5)
        assert response_health.status_code == 200


# %%


class UploadFolder(hrt.Folder):
    def __init__(self, base, scenario_results_path, create=True):
        """
        Folder to store files that need to be uploaded to LDO.

        Folder
            ├── dem.tif
            ├── results_3di.nc
            ├── Folder.zip (zipped dem.tif + results_3di.nc)

        Parameters
        ----------
        base : Union[str,Path]
            output directory where upload files will be placed
        scenario_results_path
            directory where scenario results are stored.
        """
        super().__init__(base, create=create)
        self.scenario_results_path = scenario_results_path
        self.zip_size = None
        self.zip_path = None

    def _find_scenario_folder(self):
        """Get Path to scenario results"""
        scenario_paths = [j for i in self.scenario_results_path.glob("*/") for j in list(i.glob("*/"))]
        for scenario_path in scenario_paths:
            if scenario_path.name == self.name:
                return scenario_path
        raise FileNotFoundError(f"{self.name} not found in {self.scenario_results_path}")

    def copy_files(self):
        """Copy NetCDF and DEM to the upload folder"""
        scenario_folder = self._find_scenario_folder()
        breach = Breaches(scenario_folder)
        raster_compress_path = breach.wss.path.joinpath("dem_clip.tif")
        netcdf_path = breach.netcdf.path.joinpath("results_3di.nc")

        shutil.copy2(raster_compress_path, self.path)
        shutil.copy2(netcdf_path, self.path)

        logger.info(f"Scenario {self.name} has been copied in the folder structure")

    def zip_files(self):
        """Zip files so they can be uploaded"""
        zip_name = self.name.replace(" ", "_") + ".zip"

        # Set the zip file path
        self.zip_path = self.path.joinpath(zip_name)

        # Zip the folder to be uploaded
        with zipfile.ZipFile(self.zip_path, "w") as zipf:
            for file in self.path.glob("*"):
                if file.name != zip_name:
                    zipf.write(file, arcname=file.name)

        self.zip_size = round(self.zip_path.stat().st_size / 1024 / 1024, 2)  # MB
        logger.info(f"Zip {zip_name} created with size {self.zip_size} MB")
        return self.zip_path


# %%


class LDO_API_UPLOAD:
    def __init__(self, metadata_folder_path, refresh_token, scenario_name):
        self.metadata_folder_path = metadata_folder_path
        self.metadata_file = Path(os.path.join(metadata_folder_path, scenario_name + ".xlsx"))
        self.refresh_token = refresh_token
        self.headers_excel = {
            "accept": "application/json",
            "authorization": f"Bearer {self.refresh_token}",
            # 'content-type':'multiplart/form-data',
        }
        self.url_uploadfile = "/excel-imports"
        self.scenario_id = None
        self.id_excel = None

    # UPLOAD EXCEL FILE OF THE SCENARIO and retrieve id of the excel upload. In case there is an
    # error y will print the reason.
    def upload_excel(self):
        excel_import_url = self.url_uploadfile + "?mode=create"
        excel_name = self.metadata_file.name
        with open(self.metadata_file, "rb") as excel_files:
            excel_files = {
                "file": (
                    f"{excel_name}",
                    excel_files,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }
            excel_response = requests.post(url=excel_import_url, headers=self.headers_excel, files=excel_files)
        response_json = json.loads(excel_response.content.decode("utf-8"))
        if response_json.__contains__("message"):
            msg = response_json["detail"][0]["msg"]
            print(f"The excel file has a error, reason {msg}")

        else:
            status = response_json["status"]
            self.id_excel = response_json["id"]
            self.scenario_id = response_json["scenario_ids"][0]
            print(f"The excel file is {status},  and has been uploaded with id_excel number: {self.id_excel}")
        return response_json

    def upload_zip_files(self, zipfile_location):
        """Upload the zip file using the excel ID."""
        zip_name = zipfile_location.name
        file_import_url = self.url_uploadfile + f"/{self.id_excel}/files/{zip_name}/upload"

        # Create link to upload zip file
        response = requests.put(url=file_import_url, headers=self.headers_excel)
        upload_url = response.json()["url"]

        # upload data using link
        with open(f"{zipfile_location}", "rb") as data:
            r = requests.put(upload_url, data=data)
        print(r.status_code)
        print(r.reason)
        print("uploading")
        print(file_import_url)
        return print(f"the scenario {zip_name} has been uploaded")


# %%
if __name__ == "__main__":
    # Set Paths from the data to be uploaded

    # Excel files per scenario.
    metadata_folder = (
        r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\metadata_per_scenario"
    )

    # Excel file where the ID and size of the upload is going to be stored
    id_scenarios = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\scenarios_ids.xlsx"
    # Folder location where the scenarios are going to be copied
    ldo_structuur_path = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur"
    scenario_results_path = Path(r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output")

    # List the scenario name to be uploaded
    scenario_names = os.listdir(metadata_folder)

    # data frame from the scenarios that are gonig to be uploaded.
    pd_scenarios = pd.read_excel(id_scenarios)

    # Select scenarios ids that area already uploaded to be skiped
    scenario_done = pd_scenarios.loc[pd_scenarios["ID_SCENARIO"] > 0, "Naam van het scenario"].to_list()

    # Sleep time to not burn out the API
    sleeptime = 420

    # Create a list to delete scenarios already uploaded.
    delete_file = []

    # Loop over al the scenarios
    for excel_file_name in scenario_names:
        # Set Scenario Name
        scenario_name = excel_file_name[:-5]
        # %%
        # If the scenario is done the continue
        if scenario_name in scenario_done:
            continue
        else:
            # Set folder with scenario name to be uploaded to LDO
            path = hrt.Folder(os.path.join(ldo_structuur_path, scenario_name))

            # Create folder with data to upload.
            ldo_structuur = UploadFolder(path, scenario_results_path=scenario_results_path)
            ldo_structuur.copy_files()
            ldo_structuur.zip_files()

            # Set API key
            ldo_api = LDO_API(api_key=LDO_API_KEY)

            # Set UPLOAD as an object
            ldo_upload = LDO_API_UPLOAD(metadata_folder, ldo_api.access_token, scenario_name)

            # get metadata file of the scenario that is been uploaded
            metadata_file = ldo_upload.metadata_file

            # Upload excel file from the scenario, and retrieve json infomration
            excel_response = ldo_upload.upload_excel()

            # store scenario id in the metadata
            scenario_id = ldo_upload.scenario_id
            print(scenario_id)

            # Upload zip file using the
            ldo_upload.upload_zip_files(ldo_structuur.zipfile_location)
            time.sleep(sleeptime)

            # Save the id of upload from the scenario
            pd_scenarios.loc[pd_scenarios["Naam van het scenario"] == scenario_name, "ID_SCENARIO"] = scenario_id

            # Save  the size of the scenario in the metdata dataframe
            pd_scenarios.loc[pd_scenarios["Naam van het scenario"] == scenario_name, "SIZE_KB"] = (
                ldo_structuur.zip_size
            )

            # REMOVE/DELETE ZIP AND FOLDER FROM THE SCENARIO THAT IS ALREADY UPLOADED.
            delete_file.append(ldo_structuur.path)

            if len(delete_file) > 1:
                previous_folder = delete_file.pop(0)
                shutil.rmtree(previous_folder)

            print(f"the scenario {scenario_name} has been uploaded")

            # Save the excel file.
            pd_scenarios.to_excel(id_scenarios, index=False, engine="openpyxl")
    # %%
