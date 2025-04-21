# %%
import shutil
from pathlib import Path

from tests.config import TEST_DIRECTORY


def test_HyDAMO_validator():
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    validation_directory_path = TEST_DIRECTORY / "test_HyDAMO_validator"
    hydamo_file_path = validation_directory_path / "HyDAMO.gpkg"
    validation_rules_json_path = validation_directory_path / "rules.json"

    if not hydamo_file_path.exists():
        raise FileNotFoundError(f"File {hydamo_file_path} does not exist")
    if not validation_rules_json_path.exists():
        raise FileNotFoundError(f"File {validation_rules_json_path} does not exist")

    test_coverage_location = r"data/test_HyDAMO_validator/dtm"
    if not Path(test_coverage_location).exists():  # copy it from static data folder
        shutil.copytree(r"D:/github/overmeen/data/test_HyDAMO_validator/dtm", test_coverage_location)

    validate_hydamo(
        hydamo_file_path=hydamo_file_path,
        validation_rules_json_path=validation_rules_json_path,
        validation_directory_path=validation_directory_path,
        coverages_dict={"AHN": test_coverage_location},
        output_types=["geopackage", "csv", "geojson"],
    )

    # TODO some checks on the output


if __name__ == "__main__":
    test_HyDAMO_validator()

# %%
