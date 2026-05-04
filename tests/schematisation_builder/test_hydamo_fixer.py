import shutil
import sys

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_fixer import fix_hydamo
from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevel"]

RUN_VALIDATION = False
MANUAL_FIX = False


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_hydamo_fixer():
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"

    if RUN_VALIDATION:
        validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
        hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
        template_file_path = TEST_DIRECTORY / "schematisation_builder" / "style.gpkg"
        validation_rules_json_path = hrt.get_pkg_resource_path(
            schematisation_builder_resources, "validationrules.json"
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
    validation_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")
    results_gpkg_path = TEST_DIRECTORY / "schematisation_builder" / "results.gpkg"
    fix_directory_path = TEMP_DIR / f"temp_hydamo_fixer_{hrt.current_time(date=True)}"

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

    # check if validation rule 5 of duikersifonhevel is fixed correctly
    # open hydamo_fixed_gpkg_path and check if column breedteopening is added and if value of breedteopening is correct based on fix message in review gpkg
    gdf_review_duikersifonhevel = gpd.read_file(hydamo_fix_review_path, layer="duikersifonhevel")

    assert "breedteopening" in gdf_review_duikersifonhevel.columns
    assert "fixes_breedteopening" in gdf_review_duikersifonhevel.columns

    # specific check for feature with id 14 which should have fix applied based on validation rule 5
    assert (
        "if duikersifonhevel in primair watersysteem: breedteopening = 0.8m, anders breedteopening = 0.5m"
        in gdf_review_duikersifonhevel["fixes_breedteopening"][13]
    )
    assert gdf_review_duikersifonhevel["breedteopening"][13] == 0.8

    # TODO: make check for hoogteopening and check if this fis is also applied correctly

    # Check if manual fix is applied correctly. Set variable MANUAL_FIX to True to apply this check
    assert "manual_overwrite_breedteopening" in gdf_review_duikersifonhevel.columns
    if MANUAL_FIX:
        # NOTE: If command prompt ask you for input, fill in 0.6 for feature with id 2 in column manual_overwrite_breedteopening
        # check if manual overwrite value is applied correctly for feature with id 2
        assert gdf_review_duikersifonhevel["breedteopening"][1] == 0.6

    # Check if is_usable column is added
    assert "is_usable" in gdf_review_duikersifonhevel.columns
    # NOTE: if code includes function to set features to unusable if topological fix is required, check if value in is_usable column is correct for one of the features of which you are sure that it is unusable or usable


if __name__ == "__main__":
    test_hydamo_fixer()
