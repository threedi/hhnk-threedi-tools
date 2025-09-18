# %%

import os
import sys
from pathlib import Path

import dotenv
import pytest

# import hhnk_research_tools as hrt
from tests.config import TEMP_DIR, TEST_DIRECTORY

dotenv.load_dotenv()


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
@pytest.mark.skipif(
    os.getenv("SKIP_DATABASE") == "1",
    reason="Skipping DB test because no local_settings_htt.py or DATABASES available.",
)
def test_schematisation_builder():
    from hhnk_threedi_tools.core.schematisation_builder.main import make_validated_hydamo_package

    # define project folder path and
    project_folder = TEST_DIRECTORY / "test_schematisation_builder"

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


if __name__ == "__main__":
    test_schematisation_builder()

# %%
