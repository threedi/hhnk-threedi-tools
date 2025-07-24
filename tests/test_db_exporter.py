# %%
"""Tests for database (db) exporter function"""
from pathlib import Path
import os
import sys

if sys.version_info < (3, 12) and not os.environ.get("SKIP_DATABASE"):
    os.environ["SKIP_DATABASE"] = "1"

import pytest
import geopandas as gpd
import hhnk_research_tools as hrt
from tests.config import TEMP_DIR, TEST_DIRECTORY

from hhnk_threedi_tools.core.schematisation_builder.DB_exporter import DATABASES, db_exporter

TEST_DIRECTORY_SB = TEST_DIRECTORY / "schematisation_builder"
# Create output directory for db exporter tests
db_export_output_dir = TEMP_DIR / f"temp_db_exporter_{hrt.current_time(date=True)}"

skip_db = DATABASES == {}

@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_db_exporter_one_feature():
    """Test the db_exporter function with a single feature from the GEMAAL table from DAMO."""
    model_extent_path = TEST_DIRECTORY_SB / "area_test_sql_helsdeur.gpkg"
    output_file = db_export_output_dir / "test_damo_gemaal_helsdeur.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["GEMAAL_DAMO"]

    logging_DAMO = db_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
        update_extent=False,
    )

    assert output_file.exists() is True

    gdf_result = gpd.read_file(output_file, engine="pyogrio")

    assert gdf_result.loc[0, "code"] == "KGM-Q-29234"
    assert logging_DAMO == []

@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_db_exporter_GEMAAL_and_POMP_from_CSO():
    model_extent_path = TEST_DIRECTORY_SB / "area_test_sql_helsdeur.gpkg"
    output_file = db_export_output_dir / "test_cso_gemaal_pomp_helsdeur.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["GEMAAL"]

    logging_DAMO = db_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
        update_extent=False,
    )

    gemaal_gdf = gpd.read_file(output_file, layer="GEMAAL", engine="pyogrio")
    pomp_gdf = gpd.read_file(output_file, layer="POMP", engine="pyogrio")

    assert gemaal_gdf.loc[0, "code"] == "KGM-Q-29234"
    assert len(pomp_gdf) == 4
    assert logging_DAMO == []  # test geen errors

@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_db_exporter_polder():
    """Test the db_exporter function using all defeault tables for the test polder."""

    model_extent_path = TEST_DIRECTORY_SB / r"test_schematisation_builder\01_source_data\polder_polygon.shp"
    output_file = db_export_output_dir / "test_export.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["HYDROOBJECT", "DUIKERSIFONHEVEL"]

    logging_DAMO = db_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
    )

    assert output_file.exists() is True

    gdf_result = gpd.read_file(output_file, engine="pyogrio")

    assert logging_DAMO == []
    # assert gdf not empty
    assert gdf_result.shape[0] > 0


# %%
# Test
if __name__ == "__main__":
    print(os.environ.get("SKIP_DATABASE"))

    Path(db_export_output_dir).mkdir(exist_ok=True, parents=True)
    test_db_exporter_one_feature()
    test_db_exporter_GEMAAL_and_POMP_from_CSO()
    test_db_exporter_polder()
    # else:
    #     print("Skipping DB test because no local_settings_htt.py or DATABASES available.")


# %%
