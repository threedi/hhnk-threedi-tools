import os
import shutil
from pathlib import Path

import geopandas as gpd
import numpy as np

from tests.config import TEST_DIRECTORY


def test_HyDAMO_validator():
    # TODO temp method of import validatiemodule, change workdir
    folder = Path(__file__).parents[2] / "HyDAMOValidatieModule"
    os.chdir(folder)
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    os.chdir(Path(__file__).parent)  # change workdir back to this file

    TEST_DIRECTORY_HyDAMO_validator = TEST_DIRECTORY / "test_HyDAMO_validator"
    HyDAMO_path = TEST_DIRECTORY_HyDAMO_validator / "HyDAMO.gpkg"
    validation_rules_json_path = TEST_DIRECTORY_HyDAMO_validator / "rules.json"

    if not HyDAMO_path.exists():
        raise FileNotFoundError(f"File {HyDAMO_path} does not exist")
    if not validation_rules_json_path.exists():
        raise FileNotFoundError(f"File {validation_rules_json_path} does not exist")

    test_coverage_location = r"data/test_HyDAMO_validator/dtm"
    if not Path(test_coverage_location).exists():  # copy it from static data folder
        shutil.copytree(r"D:/github/overmeen/data/test_HyDAMO_validator/dtm", test_coverage_location)

    validate_hydamo(
        hydamo_file_path=HyDAMO_path,
        validation_rules_json_path=validation_rules_json_path,
        validation_directory_path=TEST_DIRECTORY_HyDAMO_validator,
        coverages_dict={"AHN": test_coverage_location},
        output_types=["geopackage", "csv", "geojson"],
    )


# %%
if __name__ == "__main__":
    test_HyDAMO_validator()

# %%
