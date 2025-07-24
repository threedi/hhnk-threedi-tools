# %%

from pathlib import Path
import pytest
import sys
import os

if sys.version_info < (3, 12) and not os.environ.get("SKIP_DATABASE"):
    os.environ["SKIP_DATABASE"] = "1"

# import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.schematisation_builder.schematisation_builder import DATABASES, make_validated_hydamo_package
from config import TEST_DIRECTORY

skip_db = DATABASES == {}

@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_schematisation_builder():
    # define project folder path and
    project_folder = TEST_DIRECTORY / "test_schematisation_builder"

    # TODO: if polder shapefile automatically is copied, define here folder from where

    # select which tables names to export from DAMO
    TABLE_NAMES = ["HYDROOBJECT", "DUIKERSIFONHEVEL"]

    # run the function to create a validated HyDAMO package
    make_validated_hydamo_package(project_folder, TABLE_NAMES)

    # check if the files are created
    damo_file_path = project_folder / "01_source_data" / "DAMO.gpkg"
    hydamo_file_path = project_folder / "01_source_data" / "HyDAMO.gpkg"
    logger_file_path = project_folder / "log.log"
    validation_result_file_path = project_folder / "01_source_data" / "hydamo_validation" / "results" / "results.gpkg"

    assert damo_file_path.exists()
    assert hydamo_file_path.exists()
    assert logger_file_path.exists()
    assert validation_result_file_path.exists()
#%%
if __name__ == "__main__":
    test_schematisation_builder()

# %%
