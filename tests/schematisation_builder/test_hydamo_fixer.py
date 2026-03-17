import shutil
import sys
from pathlib import Path

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.utils.hydamo_fixes import ExtendedHyDAMO
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevel"]


# test for creation of summary validation and fix report gpkg
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_hydamo_fixer():
    # import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
    # from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo
    # from hydamo_validation.datamodel import HyDAMO

    # validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
    # hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
    # validation_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")

    # test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp

    # datamodel, result_summary = validate_hydamo(
    #     hydamo_file_path=hydamo_file_path,
    #     validation_rules_json_path=validation_rules_json_path,
    #     validation_directory_path=validation_directory_path,
    #     coverages_dict={"AHN": test_coverage_location},
    #     output_types=["geopackage", "csv", "geojson"],
    # )
    # datamodel.to_geopackage(validation_directory_path / "HyDAMO_validated.gpkg", use_schema=False)
    # stop

    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_fixer import HyDAMOFixer

    # define paths
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"
    # validated_hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"
    validation_directory_path = TEMP_DIR / f"temp_hydamo_fixer_{hrt.current_time(date=True)}"
    hydamo_file_path2 = validation_directory_path.joinpath("datasets", hydamo_file_path.name)
    hydamo_file_path2.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(hydamo_file_path, hydamo_file_path2)
    # validated_hydamo_file_path2 = validation_directory_path.joinpath("datasets", validated_hydamo_file_path.name)
    # validated_hydamo_file_path2.parent.mkdir(parents=True, exist_ok=True)
    # shutil.copy2(validated_hydamo_file_path, validated_hydamo_file_path2)

    validation_rules_json_path = hrt.get_pkg_resource_path(
        schematisation_builder_resources, "validationrules_test.json"
    )
    validation_rules_json_path2 = validation_directory_path.joinpath("validationrules.json")
    if not validation_rules_json_path2.exists():
        shutil.copy2(validation_rules_json_path, validation_rules_json_path2)

    fix_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json")
    fix_rules_json_path2 = validation_directory_path.joinpath("FixConfig.json")
    if not fix_rules_json_path2.exists():
        shutil.copy2(fix_rules_json_path, fix_rules_json_path2)

    # create folder results and fix_phase
    (validation_directory_path / "results").mkdir(parents=True, exist_ok=True)

    # copy validation results gpkg to results folder
    validation_results_src = TEST_DIRECTORY / "schematisation_builder" / "results.gpkg"
    validation_results_dst = validation_directory_path / "results.gpkg"
    shutil.copy2(validation_results_src, validation_results_dst)

    test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp
    coverages_dict = {"AHN": test_coverage_location}

    fixer = HyDAMOFixer(
        hydamo_file_path=hydamo_file_path,
        validation_directory_path=validation_directory_path,
    )
    hydamo_fixer = fixer.fixer(coverages=coverages_dict, output_types=["geopackage"])
    datamodel, layer_summary, result_summary = hydamo_fixer(directory=validation_directory_path, raise_error=True)

    # assert
    hydamo_fix_review_path = validation_directory_path / "review" / "fix_overview.gpkg"
    assert hydamo_fix_review_path.exists()

    hydamo_fix_log_path = validation_directory_path / "results" / "fixer.log"
    assert hydamo_fix_log_path.exists()

    hydamo_fix_results_path = validation_directory_path / "results" / "fix_result.json"
    assert hydamo_fix_results_path.exists()

    # check if expected layers are in report gpkg
    fix_layers = fiona.listlayers(hydamo_fix_review_path)
    expected_layers = LAYERS
    for layer in expected_layers:
        assert layer in fix_layers
    # check if expected columns are in one of the layers

    # fix_gdf = gpd.read_file(hydamo_fixed_gpkg_path, layer="duikersifonhevel")
    # assert "is_usable" in fix_gdf.columns


# %%
if __name__ == "__main__":
    test_hydamo_fixer()
