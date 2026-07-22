import shutil
import sys

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevel"]


# test for creation of summary validation and fix report gpkg
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_apply_validation_fixes():
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_fixer import HyDAMOFixer

    # define paths
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
    validation_directory_path = TEMP_DIR / f"temp_hydamo_fixer_apply_fixes_{hrt.current_time(date=True)}"

    # create folder results and fix_phase
    (validation_directory_path / "results").mkdir(parents=True, exist_ok=True)
    (validation_directory_path / "fix_phase").mkdir(parents=True, exist_ok=True)

    # copy validation results gpkg to results folder
    validation_results_src = TEST_DIRECTORY / "schematisation_builder" / "results.gpkg"
    validation_results_dst = validation_directory_path / "results" / "results.gpkg"
    shutil.copy(validation_results_src, validation_results_dst)

    fixer = HyDAMOFixer(
        hydamo_file_path=hydamo_file_path,
        validation_directory_path=validation_directory_path,
    )
    fixer.create_validation_fix_reports()
    fixer.execute()

    # assert
    hydamo_fixed_gpkg_path = validation_directory_path / "results" / "HyDAMO_fix.gpkg"
    assert hydamo_fixed_gpkg_path.exists()

    # check if expected layers are in report gpkg
    fix_layers = fiona.listlayers(hydamo_fixed_gpkg_path)
    expected_layers = LAYERS
    for layer in expected_layers:
        assert layer in fix_layers
    # check if expected columns are in one of the layers
    fix_gdf = gpd.read_file(hydamo_fixed_gpkg_path, layer="duikersifonhevel")
    assert "is_usable" in fix_gdf.columns


# %%
if __name__ == "__main__":
    test_apply_validation_fixes()
