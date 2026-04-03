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
import warnings
import zipfile
from pathlib import Path

import hhnk_research_tools as hrt
import pandas as pd
import requests

from hhnk_threedi_tools.breaches.breaches import Breaches

LDO_API_URL = "https://ldo.overstromingsinformatie.nl/api/v1/"

# Generate api key on de LDO_API_URL website. And place it in api_ldo_key.txt

raw = Path("api_ldo_key.txt").read_text("utf8")
LDO_API_KEY = json.loads(raw)

logger = hrt.logging.get_logger(__name__)

# %%
# FOR ADMINISTRATION PERMISSION USE THE FOLLOWING. Otherwise you will get a permission feedback
# at the moment you will try to upload the excel file.
# You will need to copy your own data in the webiste *https://ldo.overstromingsinformatie.nl/api/)
# 1. Open the website
# 2. Provide credential to log in
# 3. On the website in the box auth/v1/personalapikeys/ copy your own information as below (line 91 to 96)
# 4. In the same website in the box auth/v1/token/ use the tenant 4 to get the access token and refresh token.
# 5. Place the information in the api_ldo_key.txt file as below:

# Note: the following information is not real, it is just an example of how the information should be
# placed in the api_ldo_key.txt file (structure).

# You need to copy your own information from the website.
# {
#     "key": "olkHtC3EOu1zaDg",
#     "refresh": "eDOuz1fxdMyYN2eyLjqUWd9c_FuDyWebaIg",
#     "access": "eyJ5uxCRCpFq6hA"
# }

# The following information I leave it to copy and paste the information in the website

parameters = {
    "scope": "admin",
    "name": "Juan_Test_12",  # Change the name to your own name
    "expiry_date": "2025-01-18T10:22:48.008Z",  # Change the expiry date to a future date, otherwise you will not be able to use the API key
    "revoked": False,  # lowercase, otherwise you will not be able to use the API key
}


# %%
class LDO_API:
    def __init__(self, api_key=LDO_API_KEY, tenant=4, url=LDO_API_URL):
        self.url = url
        self.api_key = LDO_API_KEY["key"]
        self.tenant = tenant  # organisation, 4=hhnk.

        # Authorisation goes through the auth endpoint
        self.url_auth = self.url.replace("/api/", "/auth/")

        self._refresh_token = LDO_API_KEY["refresh"]  # set on calling self.access_token
        self._access_token = LDO_API_KEY["access"]  # Property

        # TODO FROM LDO_API_UPLOAD
        self.headers_excel = {
            "accept": "application/json",
            "authorization": f"Bearer {self._access_token}",
        }

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
        else:
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

    def upload_excel(self, metadata_xlsx):
        """Upload excel file of the scenario and retrieve id of the excel upload.
        In case there is an error it will print the reason.
        """
        url_excel_import = self.url + "excel-imports?mode=create"
        with open(metadata_xlsx, "rb") as excel_files:
            excel_files = {
                "file": (
                    metadata_xlsx.name,
                    excel_files,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }
            excel_response = requests.post(url=url_excel_import, headers=self.headers_excel, files=excel_files)
        response_json = json.loads(excel_response.content.decode("utf-8"))
        if response_json.__contains__("message"):
            msg = response_json["detail"][0]["msg"]
            logger.error("The excel file has an error")
            raise ValueError(msg)

        else:
            status = response_json["status"]
            excel_id = response_json["id"]
            scenario_id = response_json["scenario_ids"][0]
            logger.info(
                f"The excel file is {status}, and has been uploaded with excel_id: {excel_id}, scenario_id: {scenario_id}"
            )
            return excel_id, scenario_id

    def upload_zip_file(self, zip_path, excel_id):
        """Upload the zip file using the excel ID."""
        file_import_url = self.url + f"excel-imports/{excel_id}/files/{zip_path.name}/upload"

        logger.info(f"Uploading zip to {file_import_url}")
        # Create link to upload zip file
        response = requests.put(url=file_import_url, headers=self.headers_excel)
        upload_url = response.json()["url"]

        # Upload data using link
        with open(zip_path, "rb") as data:
            r = requests.put(upload_url, data=data)
        logger.info(f"status code: {r.status_code}")
        logger.info(f"reason: {r.reason}")
        logger.info(f"Finished uploading {zip_path.name}")

    def get_external_processings(self, scenario_id):
        """Retrieve external processing information for a scenario."""
        url = self.url + f"scenarios/{scenario_id}/external-processings"
        response = requests.get(url=url, headers=self.headers_excel, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Retrieved external processings for scenario_id={scenario_id}")
        return data

    def save_external_processings_json(self, scenario_id, output_folder):
        """Save external processing response as JSON."""
        output_folder = Path(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)

        data = self.get_external_processings(scenario_id)
        json_path = output_folder / f"external_processings_{scenario_id}.json"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved external processing JSON to {json_path}")
        return json_path, data


# %%


class LdoUploadFolder(hrt.Folder):
    def __init__(self, base, scenario_results_path, create=True):
        """
        Folder to store files that need to be uploaded to LDO.
        This is a temporary dir that will be removed after a successful
        upload

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

        # scenario_paths = [j for i in self.scenario_results_path.glob("*/") for j in list(i.glob("*/*/"))]
        # for scenario_path in scenario_paths:
        #     if scenario_path.name == self.name:
        #         return scenario_path

        scenario_path = self.scenario_results_path / self.name
        if scenario_path.is_dir():
            return scenario_path

        warnings.warn(f"Scenario folder '{self.name}' not found in '{self.scenario_results_path}'. Skipping.")
        return None

    def copy_files(self):
        """Copy NetCDF and DEM to the upload folder"""
        scenario_folder = self._find_scenario_folder()
        if scenario_folder is None:
            return

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

        self.zip_size = round(self.zip_path.stat().st_size / 1024, 0)  # KB
        logger.info(f"Zip {zip_name} created with size {self.zip_size / 1024} MB")
        return self.zip_path


# %%
if __name__ == "__main__":
    # Set Paths from the data to be uploaded

    # Excel files per scenario.
    base_path = Path(r"Y:\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S")
    metadata_folder = base_path.joinpath(r"ldo_structuur\metadata_per_scenario")

    # Excel file where the ID and size of the upload is going to be stored
    id_scenarios = base_path.joinpath(r"ldo_structuur\scenarios_ids.xlsx")

    # Folder location where the scenarios are going to be copied
    ldo_structuur_path = base_path.joinpath("ldo_structuur")

    # Folder where scenario results are stored.
    scenario_results_path = base_path.joinpath("sbln")

    # data frame from the scenarios that are gonig to be uploaded.
    pd_scenarios = pd.read_excel(id_scenarios)

    # Select scenarios ids that area already uploaded to be skiped
    scenarios_done = pd_scenarios.loc[pd_scenarios["ID_SCENARIO"] > 0, "Naam van het scenario"].to_list()

    # Sleep time to not burn out the API
    sleeptime = 300  # FIXME 7 minutes seems alot?

    # Set API key
    ldo_api = LDO_API(api_key=LDO_API_KEY)

    # Loop over al the scenarios
    scenarios = list(metadata_folder.glob("*.xlsx"))
    # %%
    for metadata_xlsx in scenarios:
        # Set Scenario Name
        scenario_name = metadata_xlsx.stem
        print(f"Processing scenario {scenario_name}")
        # If the scenario is done the continue
        if scenario_name in scenarios_done:
            continue
        else:
            # Set folder with scenario name to be uploaded to LDO
            scenario_path = ldo_structuur_path.joinpath(scenario_name)

            # Create folder with data to upload.
            ldo_structuur = LdoUploadFolder(scenario_path, scenario_results_path=scenario_results_path)

            copied = ldo_structuur.copy_files()
            # if not copied:
            #     continue

            zip_path = ldo_structuur.zip_files()

            # Upload excel file from the scenario, and retrieve json infomration
            excel_id, scenario_id = ldo_api.upload_excel(metadata_xlsx=metadata_xlsx)

            # Upload zip file
            ldo_api.upload_zip_file(zip_path=zip_path, excel_id=excel_id)

            # Save the id of upload from the scenario
            pd_scenarios.loc[pd_scenarios["Naam van het scenario"] == scenario_name, "ID_SCENARIO"] = scenario_id

            # Save the size of the scenario in the metdata dataframe
            pd_scenarios.loc[pd_scenarios["Naam van het scenario"] == scenario_name, "SIZE_KB"] = (
                ldo_structuur.zip_size
            )

            # Clear outputs
            shutil.rmtree(ldo_structuur.path)

            # Save the excel file.
            pd_scenarios.to_excel(id_scenarios, index=False, engine="openpyxl")
            logger.info(f"Finished processing {scenario_name}")
            time.sleep(sleeptime)

# %%
excel_path = r"Y:\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\check_list_control_table.xlsx"
check_excel = pd.read_excel(excel_path, sheet_name="Blad2")
scenario_id = check_excel["Scenario ID"].values
ldo_api = LDO_API(api_key=LDO_API_KEY)
sleeptime = 10
for scenario in scenario_id:
    try:
        scenario
        data_schade = ldo_api.get_external_processings(scenario)
        items = data_schade.get("items", [])
        if not items:
            print(f"No external processing found for scenario {scenario}")
            continue
        else:
            Totaal_getroffenen = data_schade["items"][0]["meta_data"]["Totaal getroffenen"]
            Totaalschade = data_schade["items"][0]["meta_data"]["Totaalschade"]
            Totaal_slachtoffers = data_schade["items"][0]["meta_data"]["Totaal slachtoffers"]
            processing_type = data_schade["items"][0]["type"]

            check_excel.loc[check_excel["Scenario ID"] == scenario, "Totaal getroffenen"] = Totaal_getroffenen
            check_excel.loc[check_excel["Scenario ID"] == scenario, "Totaalschade"] = Totaalschade
            check_excel.loc[check_excel["Scenario ID"] == scenario, "Totaal slachtoffers"] = Totaal_slachtoffers
            check_excel.loc[check_excel["Scenario ID"] == scenario, "Type"] = processing_type
            print(f"Processed scenario {scenario}")

    except Exception as e:
        logger.error(f"Error processing scenario {scenario}: {e}")
    time.sleep(sleeptime)

# Save once at the end
with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as writer:
    check_excel.to_excel(writer, index=False, sheet_name="Blad2")


# %%
