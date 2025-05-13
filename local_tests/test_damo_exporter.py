# %%
"""Test for DAMO exporter function"""

from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.schematisation_builder.DAMO_exporter import DAMO_exporter
from tests.config import TEMP_DIR, TEST_DIRECTORY

damo_export_output_dir = TEMP_DIR / f"temp_damo_exporter_{hrt.current_time(date=True)}"


def test_DAMO_exporter_one_feature():
    model_extent_path = TEST_DIRECTORY / "schema_builder" / "area_test_sql_helsdeur.gpkg"
    output_file = damo_export_output_dir / "DAMO_gemaal.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["GEMAAL"]

    logging_DAMO = DAMO_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
    )

    assert output_file.exists() is True

    gdf_result = gpd.read_file(output_file, engine="pyogrio")

    assert gdf_result.loc[0, "code"] == "KGM-Q-29234"
    assert logging_DAMO == []


def test_DAMO_exporter_polders():
    model_extent_path = TEST_DIRECTORY / r"model_test\01_source_data\polder_polygon.shp"
    output_file = damo_export_output_dir / "DAMO.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["HYDROOBJECT", "DUIKERSIFONHEVEL"]

    logging_DAMO = DAMO_exporter(
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
    Path(damo_export_output_dir).mkdir(exist_ok=True, parents=True)
    test_DAMO_exporter_one_feature()
    test_DAMO_exporter_polders()


# %%
