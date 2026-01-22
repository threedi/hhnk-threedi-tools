import shutil
import sys

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevel"]
temp_dir_out = TEMP_DIR / f"temp_Fixer1_converter_{hrt.current_time(date=True)}"


# test for creation of summary validation and fix report gpkg
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_creation_validation_fixes_summary():
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_fixer import HYDAMOFixer

    # define paths
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
    validation_directory_path = TEMP_DIR / f"temp_HyDAMO_Fixer1_ValFix_summary_{hrt.current_time(date=True)}"

    # create folder results and fix_phase
    (validation_directory_path / "results").mkdir(parents=True, exist_ok=True)
    (validation_directory_path / "fix_phase").mkdir(parents=True, exist_ok=True)

    # copy validation results gpkg to results folder
    validation_results_src = TEST_DIRECTORY / "schematisation_builder" / "results.gpkg"
    validation_results_dst = validation_directory_path / "results" / "results.gpkg"
    shutil.copy(validation_results_src, validation_results_dst)

    fixer = HYDAMOFixer(
        hydamo_file_path=hydamo_file_path,
        validation_directory_path=validation_directory_path,
    )

    # act
    fixer.create_validation_fix_reports()

    # assert
    report_gpkg_path = validation_directory_path / "fix_phase" / "summary_val_fix.gpkg"
    assert report_gpkg_path.exists()

    # check if expected layers are in report gpkg
    report_layers = fiona.listlayers(report_gpkg_path)

    # TODO: add more layers when more layers are in fixconfig
    expected_layers = LAYERS
    for layer in expected_layers:
        assert layer in report_layers
    # check if expected columns are in one of the layers
    report_gdf = gpd.read_file(report_gpkg_path, layer="duikersifonhevel")
    expected_columns = [
        "code",
        "valid",
        "invalid_critical",
        "invalid_non_critical",
        "ignored",
        "geometry",
        "hoogtebinnenonderkantbov",
        "validation_sum_hoogtebinnenonderkantbov",
        "fixes_hoogtebinnenonderkantbov",
        "manual_overwrite_hoogtebinnenonderkantbov",
    ]
    for col in expected_columns:
        assert col in report_gdf.columns


# %%
if __name__ == "__main__":
    test_creation_validation_fixes_summary()
