# %%
"""Test for DAMO exporter function"""

import os
from pathlib import Path

working_dir = Path(__file__).parents[1]
os.chdir(working_dir)

import geopandas as gpd

from hhnk_threedi_tools.core.schematisation_builder.DAMO_exporter import DAMO_exporter
from tests.config import TEST_DIRECTORY


def test_DAMO_exporter_one_feature():
    GPKG_PATH = TEST_DIRECTORY / r"test_damo_exporter/area_test_sql_helsdeur.gpkg"
    TEST_RESULT_FILE = TEST_DIRECTORY / r"test_damo_exporter/resultaat/DAMO_gemaal.gpkg"

    gebied = gpd.read_file(GPKG_PATH)
    table_list = ["GEMAAL"]

    logging = DAMO_exporter(gebied, table_list, TEST_RESULT_FILE)

    gdf_result = gpd.read_file(TEST_RESULT_FILE)

    assert gdf_result.loc[0, "code"] == "KGM-Q-29234"
    assert logging == []


def test_DAMO_exporter_polders():
    GPKG_PATH = TEST_DIRECTORY / r"test_damo_exporter/input_anna_paulowna/polder_polygon.shp"
    TEST_RESULT_FILE = TEST_DIRECTORY / r"test_damo_exporter/resultaat/DAMO_anna_paulowna.gpkg"

    gebied = gpd.read_file(GPKG_PATH)
    table_list = ["HYDROOBJECT"]

    logging = DAMO_exporter(gebied, table_list, TEST_RESULT_FILE)

    gdf_result = gpd.read_file(TEST_RESULT_FILE)

    assert logging == []


# %%
# Test
if __name__ == "__main__":
    test_DAMO_exporter_one_feature()
    test_DAMO_exporter_polders()


# %%
