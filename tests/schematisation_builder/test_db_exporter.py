# %%
"""Tests for database (db) exporter function"""

import os
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

from hhnk_threedi_tools.core.schematisation_builder.DB_exporter import DATABASES, db_exporter
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

TEST_DIRECTORY_SB = TEST_DIRECTORY / "schematisation_builder"
# Create output directory for db exporter tests
db_export_output_dir = TEMP_DIR / f"temp_db_exporter_{hrt.current_time(date=True)}"

skip_db = DATABASES == {}


@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_db_exporter_one_feature():
    """
    Test the db_exporter function with a single feature from the GEMAAL table from DAMO and CSO
    Includes test of sub table.
    """
    model_extent_path = TEST_DIRECTORY_SB / "area_test_sql_helsdeur.gpkg"
    output_file = db_export_output_dir / "test_damo_gemaal_helsdeur.gpkg"
    db_export_output_dir.mkdir(exist_ok=True)

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["GEMAAL_DAMO", "GEMAAL"]

    logging_DAMO = db_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
        update_extent=False,
    )

    assert output_file.exists()

    gemaal_damo_gdf = gpd.read_file(output_file, layer="GEMAAL_DAMO")
    gemaal_cso_gdf = gpd.read_file(output_file, layer="GEMAAL")
    pomp_gdf = gpd.read_file(output_file, layer="POMP")

    assert gemaal_damo_gdf.loc[0, "code"] == "KGM-Q-29234"  # export uit DAMO_W gelukt
    assert gemaal_cso_gdf.loc[0, "code"] == "KGM-Q-29234"  # export uit CSO gelukt
    assert len(pomp_gdf) == 4  # Export van sub tabel uit cso gelukt
    assert "afvoeren" in gemaal_damo_gdf["functiegemaal"].unique()  # omzetten domeinen gelukt
    assert logging_DAMO == []  # test geen errors


@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_db_exporter_polder():
    """Test the db_exporter function using all default tables for the test polder."""

    model_extent_path = TEST_DIRECTORY / r"model_test\01_source_data\polder_polygon.shp"
    output_file = db_export_output_dir / "test_export.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")

    logging_DAMO = db_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=None,
        output_file=output_file,
    )

    assert output_file.exists()
    assert logging_DAMO == []


# %%
# Test
if __name__ == "__main__":
    print(os.getenv("SKIP_DATABASE"))

    Path(db_export_output_dir).mkdir(exist_ok=True, parents=True)
    test_db_exporter_one_feature()
    test_db_exporter_polder()


# %%
