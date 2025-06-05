# %%
"""Test for DAMO exporter function"""

import geopandas as gpd
import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.schematisation_builder.DAMO_exporter import DAMO_exporter
from tests.config import TEMP_DIR, TEST_DIRECTORY

damo_export_output_dir = TEMP_DIR / f"temp_damo_exporter_{hrt.current_time(date=True)}"
damo_export_output_dir.mkdir(exist_ok=True)


def test_DAMO_exporter_one_DAMO_feature():
    model_extent_path = TEST_DIRECTORY / "schema_builder" / "area_test_sql_helsdeur.gpkg"
    output_file = damo_export_output_dir / "DAMO_gemaal.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["GEMAAL_DAMO"]

    logging_DAMO = DAMO_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
    )

    gdf_result = gpd.read_file(output_file, engine="pyogrio")

    assert gdf_result.loc[0, "code"] == "KGM-Q-29234"
    assert logging_DAMO == []  # test geen errors


def test_DAMO_exporter_GEMAAL_and_POMP_from_CSO():
    model_extent_path = TEST_DIRECTORY / "schema_builder" / "area_test_sql_helsdeur.gpkg"
    output_file = damo_export_output_dir / "DAMO_gemaal.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")
    table_names = ["GEMAAL"]

    logging_DAMO = DAMO_exporter(
        model_extent_gdf=model_extent_gdf,
        table_names=table_names,
        output_file=output_file,
    )

    gemaal_gdf = gpd.read_file(output_file, layer="GEMAAL", engine="pyogrio")
    pomp_gdf = gpd.read_file(output_file, layer="POMP", engine="pyogrio")

    assert gemaal_gdf.loc[0, "code"] == "KGM-Q-29234"
    assert len(pomp_gdf) == 4
    assert logging_DAMO == []  # test geen errors


def test_DAMO_exporter_polders():
    model_extent_path = TEST_DIRECTORY / r"model_test\01_source_data\polder_polygon.shp"
    output_file = damo_export_output_dir / "DAMO.gpkg"

    model_extent_gdf = gpd.read_file(model_extent_path, engine="pyogrio")

    logging_DAMO = DAMO_exporter(
        model_extent_gdf=model_extent_gdf,
        output_file=output_file,
    )  # no table , so whole set in layer mapping

    gdf_result = gpd.read_file(output_file, engine="pyogrio")

    assert logging_DAMO == []
    # assert gdf not empty
    assert gdf_result.shape[0] > 0


# %%
# Test
if __name__ == "__main__":
    test_DAMO_exporter_one_DAMO_feature()
    test_DAMO_exporter_GEMAAL_and_POMP_from_CSO
    test_DAMO_exporter_polders()


# %%
