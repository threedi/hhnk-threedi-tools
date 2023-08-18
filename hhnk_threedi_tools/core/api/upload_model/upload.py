#%%
# Doel van dit script:
# - Schematisatie maken als die nog niet bestaat
# - Nieuwe revisie aanmaken
# - Data uploaden
# - 3Di model en simulation template genereren
import hashlib
import time
from typing import Union, Dict, List
from pathlib import Path
from zipfile import ZipFile

from threedi_api_client.api import ThreediApi
from threedi_api_client.openapi import Schematisation
from threedi_api_client.files import upload_file
from threedi_api_client.openapi import ApiException



import urllib3
UPLOAD_TIMEOUT = urllib3.Timeout(connect=60, read=600)
THREEDI_API_HOST = "https://api.3di.live"



# %%


class ThreediApiLocal:
    def __init__(self):
        self.CONFIG = {
            "THREEDI_API_HOST": THREEDI_API_HOST,
            "THREEDI_API_PERSONAL_API_TOKEN": "",
        }

        self.api = None

    def set_api_key(self, api_key):
        self.CONFIG["THREEDI_API_PERSONAL_API_TOKEN"] = api_key

        self._api = ThreediApi(config=self.CONFIG, version="v3-beta")

    @property
    def api(self):
        if self._api is None:
            raise Exception("Call .set_api_key first.")
        return self._api

    @api.setter
    def api(self, dummy):
        self._api = dummy


threedi = ThreediApiLocal()


def md5(fname):
    """stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file"""
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_revision_info(revision__schematisation__name:str):
    threedimodel = threedi.api.threedimodels_list("model_test_v2__0d1d_test")
    if threedimodel.results == []:
        return "no previous model(s) available"
    
    else:
        schema_id = threedimodel.to_dict()['results'][0]['schematisation_id']
        latest_revision = threedi.api.schematisations_latest_revision(schema_id)
        rev_model = threedimodel.to_dict()['results'][0]['name']
        return "previous model revision: " + rev_model + " " + latest_revision.commit_message 



def get_schematisation(
    schematisation_name: str,
    organisation_uuid="48dac75bef8a42ebbb52e8f89bbdb9f2",
    tags: List = None,
) -> Schematisation:
    tags = [] if not tags else tags
    resp = threedi.api.schematisations_list(
        name=schematisation_name, owner__unique_id=organisation_uuid
    )
    if resp.count == 1:
        print(
            f"Schematisation '{schematisation_name}' already exists, skipping creation."
        )
        return resp.results[0]
    elif resp.count > 1:
        raise ValueError(f"Found > 1 schematisations named'{schematisation_name}!")

    else:
        print(
            f"Schematisation '{schematisation_name}' doesn't exists."
        )
        return resp.results[0]


def get_or_create_schematisation():
    return print("change function")


def get_and_create_schematisation(
    schematisation_name: str,
    organisation_uuid="48dac75bef8a42ebbb52e8f89bbdb9f2",
    tags: List = None,
) -> Schematisation:
    tags = [] if not tags else tags
    resp = threedi.api.schematisations_list(
        name=schematisation_name, owner__unique_id=organisation_uuid
    )
    if resp.count == 1:
        print(
            f"Schematisation '{schematisation_name}' already exists, skipping creation."
        )
        return resp.results[0]
    elif resp.count > 1:
        raise ValueError(f"Found > 1 schematisations named'{schematisation_name}!")

    # if not found -> create
    # cont = input(f"Create new schematisation? [y/n] - {schematisation_name}")
    # if cont == "y":
    schematisation = threedi.api.schematisations_create(
        data={
            "owner": organisation_uuid,
            "name": schematisation_name,
            "tags": tags,
        }
    )
    return schematisation
    # else:
    #     return None


def upload_sqlite(schematisation, revision, sqlite_path: Union[str, Path]):
    sqlite_path = Path(sqlite_path)
    sqlite_zip_path = sqlite_path.with_suffix(".zip")
    print(f"sqlite_zip_path = {sqlite_zip_path}")
    ZipFile(sqlite_zip_path, mode="w").write(
        str(sqlite_path), arcname=str(sqlite_path.name)
    )
    upload = threedi.api.schematisations_revisions_sqlite_upload(
        id=revision.id,
        schematisation_pk=schematisation.id,
        data={"filename": str(sqlite_zip_path.name)},
    )
    if upload.put_url is None:
        print(f"Sqlite '{sqlite_path.name}' already existed, skipping upload.")
    else:
        print(f"Uploading '{str(sqlite_path.name)}'...")
        upload_file(
            url=upload.put_url, file_path=sqlite_zip_path, timeout=UPLOAD_TIMEOUT
        )


def upload_raster(
    rev_id: int, schema_id: int, raster_type: str, raster_path: Union[str, Path]
):
    print(f"Creating '{raster_type}' raster...")
    raster_path = Path(raster_path)
    md5sum = md5(
        str(raster_path)
    )  # TODO check if raster changed, download from API, then upload.
    data = {"name": raster_path.name, "type": raster_type, "md5sum": md5sum}
    raster_create = threedi.api.schematisations_revisions_rasters_create(
        rev_id, schema_id, data
    )
    if raster_create.file:
        if raster_create.file.state == "uploaded":
            print(f"Raster '{raster_path}' already exists, skipping upload.")
            return

    print(f"Uploading '{raster_path}'...")
    data = {"filename": raster_path.name}
    upload = threedi.api.schematisations_revisions_rasters_upload(
        raster_create.id, rev_id, schema_id, data
    )

    upload_file(upload.put_url, raster_path, timeout=UPLOAD_TIMEOUT)


#%


def commit_revision(rev_id: int, schema_id: int, commit_message):
    # First wait for all files to have turned to 'uploaded'
    for wait_time in [0.5, 1.0, 2.0, 10.0, 30.0, 60.0, 120.0, 300.0]:
        revision = threedi.api.schematisations_revisions_read(rev_id, schema_id)
        states = [revision.sqlite.file.state]
        states.extend([raster.file.state for raster in revision.rasters])

        if all(state == "uploaded" for state in states):
            break
        elif any(state == "created" for state in states):
            print(
                f"Sleeping {wait_time} seconds to wait for the files to become 'uploaded'..."
            )
            time.sleep(wait_time)
            continue
        else:
            raise RuntimeError("One or more rasters have an unexpected state")
    else:
        raise RuntimeError("Some files are still in 'created' state")

    schematisation_revision = threedi.api.schematisations_revisions_commit(
        rev_id, schema_id, {"commit_message": commit_message}
    )

    print(f"Committed revision {revision.number}.")
    return schematisation_revision


def create_threedimodel(
    schematisation,
    revision,
    max_retries_creation=60,
    wait_time_creation=5,
    # max_retries_processing=60,
    # wait_time_processing=5,
):
    threedimodel = None
    for i in range(max_retries_creation):
        try:
            #Check number of models, if more than 2, delete oldest.
            threedimodels = threedi.api.threedimodels_list(revision__schematisation__name=schematisation.name)
            models = threedimodels.to_dict()['results']
            print(f'Found {len(models)} existing models')
            if len(models)> 2:
                print("Max 3 models are allowed. Removing oldest threedi model")
                threedi.api.threedimodels_delete(id=models[-1]['id'])
                #time.sleep(wait_time_creation)

            #Create model
            threedimodel = threedi.api.schematisations_revisions_create_threedimodel(
                id=revision.id, schematisation_pk=schematisation.id
            )
            print(f"Creating threedimodel with id {threedimodel.id}...")
            break
        except ApiException as e:
            print(e)
            time.sleep(wait_time_creation)
            continue
    # if threedimodel:
    #     for i in range(max_retries_processing):
    #         threedimodel = threedi.api.threedimodels_read(threedimodel.id)
    #         if threedimodel.is_valid:
    #             print(f"\nSuccesfully created threedimodel with id {threedimodel.id}")
    #             break
    #         else:
    #             print(f'waiting for model to become valid [{i}/{max_retries_processing}]', end='\r')
    #             time.sleep(wait_time_processing)
    #     if not threedimodel.is_valid:
    #         print(
    #             f"\nFailed to sucessfully process threedimodel with id {threedimodel.id}"
    #         )
    # else:
    #     print("Failed to create threedimodel")
    return threedimodel.id


def upload_and_process(
    schematisation_name: str,
    sqlite_path: Union[str, Path],
    raster_paths: Dict[str, str],
    schematisation_create_tags: List[str] = None,
    commit_message: str = "auto-commit",
):
    # Schematisatie maken als die nog niet bestaat
    schematisation = get_and_create_schematisation(
        schematisation_name, tags=schematisation_create_tags
    )

    # Nieuwe (lege) revisie aanmaken
    revision = threedi.api.schematisations_revisions_create(
        schematisation.id, data={"empty": True}
    )

    # Data uploaden
    # # Spatialite
    sqlite_path = Path(sqlite_path)
    upload_sqlite(
        schematisation=schematisation, revision=revision, sqlite_path=sqlite_path
    )

    # # Rasters
    for raster_type, raster_path in raster_paths.items():
        print(raster_path)
        if raster_path is not None:  # all rasters are passed but are not always used in every model. It is None when thats the case
            upload_raster(
                rev_id=revision.id,
                schema_id=schematisation.id,
                raster_type=raster_type,
                raster_path=raster_path,
            )

    # Commit revision
    commit_revision(
        rev_id=revision.id, schema_id=schematisation.id, commit_message=commit_message
    )

    # 3Di model en simulation template genereren
    threedimodel = create_threedimodel(schematisation, revision)
    return threedimodel

# %%
