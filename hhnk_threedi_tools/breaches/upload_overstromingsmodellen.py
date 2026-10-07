# swagger: https://docs.ranawaterintelligence.com/a_releasenotes_rana_hcc_api.html
# This script uses as base the script that is here:
# https://api.3di.live/v3/docs/examples/python_cookbook/

# %%
import hashlib
import json
import os
import time
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import hhnk_research_tools as hrt
import urllib3
from threedi_api_client.api import ThreediApi
from threedi_api_client.files import upload_file
from threedi_api_client.openapi import ApiException
from threedi_api_client.versions import V3Api

from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.schematisation import upload


class SchematisationUploader:
    def __init__(self, sqlite_path: Path, commit_message: str, organisation_name):
        self.sqlite_path = Path(sqlite_path)
        self.commit_message = commit_message

        self.threedi_api = self._get_api_client()

        organisations = self.threedi_api.organisations_list(name__istartswith=organisation_name)

        self.org_uuid = organisations.results[0].unique_id

        # Metadata
        metadata = self.get_schematisation_metadata(self.sqlite_path)

        self.schematisation_id = metadata["id"]
        self.schematisation_name = metadata["name"]
        self.parent_revision = metadata["wip_parent_revision"]

        # Upload timeout
        self.upload_timeout = urllib3.Timeout(
            connect=60,
            read=600,
        )
        self.revision = None

    def _get_api_client(self) -> V3Api:

        api_keys_path = (
            rf"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default"
            rf"\python\plugins\hhnk_threedi_plugin\api_key.txt"
        )

        api_key = hrt.read_api_file(api_keys_path)

        config = {
            # "THREEDI_API_HOST": "https://hcc-api.ranawaterintelligence.com",
            "THREEDI_API_HOST": "https://api.3di.live",
            "THREEDI_API_PERSONAL_API_TOKEN": api_key["threedi"],
        }

        api_client: V3Api = ThreediApi(
            config=config,
            version="v3-beta",
        )

        try:
            user = api_client.auth_profile_list()

        except ApiException as exc:
            raise RuntimeError("Could not log in to Rana HCC API. Check your API key.") from exc

        else:
            print(f"Successfully logged in as {user.username}!")

        return api_client

    def get_schematisation_metadata(
        self,
        sqlite_path: Path,
    ) -> dict:
        """Read local schematisation metadata from admin/schematisation.json."""

        sqlite_path = Path(sqlite_path)

        if not sqlite_path.exists():
            print(f"Schematisation database not found:\n{sqlite_path}")

        schematisation_dir = sqlite_path.parent
        work_in_progress_dir = schematisation_dir.parent
        model_dir = work_in_progress_dir.parent
        model_json = model_dir / "admin" / "schematisation.json"

        if not model_json.exists():
            print("check location of the json file. Not found")

        with open(model_json, "r", encoding="utf-8") as file:
            metadata = json.load(file)

        return metadata


def upload_rasters(sqlite_path):

    local_schematisation_dir = sqlite_path.parent

    raster_folder = local_schematisation_dir / "rasters"
    rasters = os.listdir(raster_folder)

    raster_names = {}

    raster_dir = sqlite_path.parent / "rasters"

    for raster in rasters:
        if "dem" in raster.lower():
            raster_names["dem_file"] = raster_dir / raster

        elif "frictie" in raster.lower():
            raster_names["frict_coef_file"] = raster_dir / raster

        elif "ini" in raster.lower():
            raster_names["initial_waterlevel_file"] = raster_dir / raster

    return raster_names


# %%
sqlite_path = Path(
    r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
)

COMMIT_MESSAGE = "Fix Initial waterlevel and boundary condition for breach: Drechterland"
# organisation_name = "Hoogheemraadschap Hollands Noorderkwartier"
organisation_name = "BWN HHNK"


uploader = SchematisationUploader(
    sqlite_path=sqlite_path, commit_message=COMMIT_MESSAGE, organisation_name=organisation_name
)
# %%

api_keys_path = (
    rf"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default"
    rf"\python\plugins\hhnk_threedi_plugin\api_key.txt"
)
threedi = upload.ThreediApiLocal()

api_key = hrt.read_api_file(api_keys_path)
upload.threedi.set_api_key(api_key["threedi"])

organisation_uuid = uploader.org_uuid

raster_names = upload_rasters(sqlite_path)

upload.upload_and_process(
    schematisation_name=sqlite_path.stem,
    organisation_uuid=organisation_uuid,
    sqlite_path=sqlite_path,
    raster_paths=raster_names,
    commit_message=COMMIT_MESSAGE,
)
# %%
