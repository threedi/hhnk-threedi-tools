import shutil
import sys

import fiona
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_fixer import fix_hydamo
from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevel"]

RUN_VALIDATION = False


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_hydamo_fixer():
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"

    if RUN_VALIDATION:
        validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
        hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
        template_file_path = TEST_DIRECTORY / "schematisation_builder" / "style.gpkg"
        validation_rules_json_path = hrt.get_pkg_resource_path(
            schematisation_builder_resources, "validationrules_test.json"
        )
        test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp

        datamodel, _ = validate_hydamo(
            hydamo_file_path=hydamo_file_path,
            validation_rules_json_path=validation_rules_json_path,
            validation_directory_path=validation_directory_path,
            template_file_path=template_file_path,
            coverages_dict={"AHN": test_coverage_location},
            output_types=["geopackage", "csv", "geojson"],
        )
        datamodel.to_geopackage(validation_directory_path / "HyDAMO_validated.gpkg", use_schema=False)
        shutil.copy2(validation_directory_path / "HyDAMO_validated.gpkg", hydamo_file_path)

    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"
    validation_rules_json_path = hrt.get_pkg_resource_path(
        schematisation_builder_resources, "validationrules_test.json"
    )
    results_gpkg_path = TEST_DIRECTORY / "schematisation_builder" / "results.gpkg"
    fix_directory_path = TEMP_DIR / f"temp_hydamo_fixer_{hrt.current_time(date=True)}"

    fix_directory_path.mkdir(parents=True, exist_ok=True)
    fix_summary_manual_test_path = TEST_DIRECTORY / "schematisation_builder" / "fix_summary_manual.gpkg"
    shutil.copy2(fix_summary_manual_test_path, fix_directory_path / "fix_summary_manual.gpkg")

    test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp
    coverages_dict = {"AHN": test_coverage_location}

    datamodel, layer_summary, result_summary = fix_hydamo(
        hydamo_file_path=hydamo_file_path,
        validation_rules_json_path=validation_rules_json_path,
        results_gpkg_path=results_gpkg_path,
        fix_directory_path=fix_directory_path,
        coverages_dict=coverages_dict,
        output_types=["geopackage"],
    )

    # assert
    hydamo_fix_review_path = fix_directory_path / "review" / "fix_summary.gpkg"
    assert hydamo_fix_review_path.exists()

    hydamo_fix_log_path = fix_directory_path / "results" / "fixer.log"
    assert hydamo_fix_log_path.exists()

    hydamo_fix_results_path = fix_directory_path / "results" / "fix_result.json"
    assert hydamo_fix_results_path.exists()

    # check if expected layers are in report gpkg
    fix_layers = fiona.listlayers(hydamo_fix_review_path)
    expected_layers = LAYERS
    for layer in expected_layers:
        assert layer in fix_layers
    # check if expected columns are in one of the layers

    # fix_gdf = gpd.read_file(hydamo_fixed_gpkg_path, layer="duikersifonhevel")
    # assert "is_usable" in fix_gdf.columns


if __name__ == "__main__":
    test_hydamo_fixer()
