# %%

import hhnk_research_tools as hrt

from tests.config import TEMP_DIR, TEST_DIRECTORY


def test_HyDAMO_validator():
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
    hydamo_file_path = TEST_DIRECTORY / "schema_builder" / "HyDAMO.gpkg"
    validation_rules_json_path = TEST_DIRECTORY / "schema_builder" / "validationrules.json"

    test_coverage_location = TEST_DIRECTORY / "schema_builder" / "dtm"  # should hold index.shp

    result_summary = validate_hydamo(
        hydamo_file_path=hydamo_file_path,
        validation_rules_json_path=validation_rules_json_path,
        validation_directory_path=validation_directory_path,
        coverages_dict={"AHN": test_coverage_location},
        output_types=["geopackage", "csv", "geojson"],
    )

    assert result_summary["success"] is True
    assert validation_directory_path.joinpath("datasets", "HyDAMO.gpkg").exists()
    # TODO some checks on the output


if __name__ == "__main__":
    test_HyDAMO_validator()

# %%
