import sys
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools as htt
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevels"]
temp_dir_out = TEMP_DIR / f"temp_Fixer1_converter_{hrt.current_time(date=True)}"


# test for creation of summary validation and fix report gpkg
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_creation_validation_fixes_summary(tmp_path: Path):
    # arrange
    resources_testdata_path = hrt.get_pkg_resource_path(htt.resources.schematisation_builder, "testdata_hydamo_fixer")
    hydamo_gpkg_path = resources_testdata_path / "hydamo_testdata.gpkg"
    validation_directory_path = resources_testdata_path / "validation_results"

    fixer = htt.core.schematisation_builder.Hydamo_fixer(
        hydamo_gpkg_path=str(hydamo_gpkg_path),
        validation_directory_path=validation_directory_path,
    )

    # act
    fixer.create_validation_fix_reports()

    # assert
    report_gpkg_path = validation_directory_path / "fix_tussenstappen" / "summary_val_fix.gpkg"
    assert report_gpkg_path.exists()

    # check if expected layers are in report gpkg
    report_layers = gpd.io.file.fiona.listlayers(report_gpkg_path)

    # TODO: add more layers when more layers are in fixconfig
    expected_layers = LAYERS
    for layer in expected_layers:
        assert layer in report_layers
    # check if expected columns are in one of the layers
    report_gdf = gpd.read_file(report_gpkg_path, layer="watergangen")
    expected_columns = [
        "code",
        "valid",
        "invalid_critical",
        "invalid_non_critical",
        "ignored",
        "bok_boven_plausibel",
        "validation_sum_bok_boven_plausibel",
        "fixes_bok_boven_plausibel",
        "manual_overwrite_bok_boven_plausibel",
    ]
    for col in expected_columns:
        assert col in report_gdf.columns
