from pathlib import Path

import geopandas as gpd
import numpy as np

from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo
from tests.config import TEST_DIRECTORY


def test_HyDAMO_validator():
    TEST_DIRECTORY_HyDAMO_validator = TEST_DIRECTORY / "test_HyDAMO_validator"
    HyDAMO_path = TEST_DIRECTORY_HyDAMO_validator / "HyDAMO.gpkg"
    validation_rules_json_path = TEST_DIRECTORY_HyDAMO_validator / "rules_1_3.json"

    if not HyDAMO_path.exists():
        raise FileNotFoundError(f"File {HyDAMO_path} does not exist")
    if not validation_rules_json_path.exists():
        raise FileNotFoundError(f"File {validation_rules_json_path} does not exist")

    validate_hydamo(
        hydamo_file_path=HyDAMO_path,
        validation_rules_json_path=validation_rules_json_path,
        validation_directory_path=TEST_DIRECTORY_HyDAMO_validator,
        coverages_dict={"AHN": r"data/test_HyDAMO_validator/dtm"},
        output_types=["geopackage", "csv", "geojson"],
    )


# %%
if __name__ == "__main__":
    test_HyDAMO_validator()

# %%
