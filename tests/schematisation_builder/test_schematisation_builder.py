# %%

import os
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

from hhnk_threedi_tools.core.schematisation_builder.main import SchematisationBuilder
from tests.config import TEMP_DIR, TEST_DIRECTORY


@pytest.mark.skipif(skip_db, reason="Skipping DB test because no local_settings_htt.py or DATABASES available.")
def test_main():
    logger = hrt.logging.get_logger(__name__)

    # create temporary project folder path
    temp_project_folder = TEMP_DIR / f"temp_schematisation_builder_{hrt.current_time(date=True)}"

    # default polder polygon is of polder 't hoekje
    default_polder_polygon_path = TEST_DIRECTORY / "model_test" / "01_source_data" / "polder_polygon.shp"

    # all layers which are ready to be exported from DAMO/CSO, convertered to HyDAMO and to be validated
    TABLE_NAMES = ["HYDROOBJECT", "GEMAAL", "COMBINATIEPEILGEBIED", "DUIKERSIFONHEVEL"]

    # run schematisation builder
    logger.info(f"Starting SchematisationBuilder test with project folder: {temp_project_folder}")
    builder = SchematisationBuilder(temp_project_folder, default_polder_polygon_path, TABLE_NAMES)

    # Part 1: Make DAMO and HyDAMO package
    builder.make_hydamo_package()
    # check if DAMO and HyDAMO files are created
    logger.info("Checking if output files are created.")
    damo_file_path = temp_project_folder / "01_source_data" / "DAMO.gpkg"
    hydamo_file_path = temp_project_folder / "01_source_data" / "HyDAMO.gpkg"
    assert damo_file_path.exists()
    assert hydamo_file_path.exists()

    # Part 2: Validate HyDAMO package
    builder.validate_hydamo_package()
    # check if validation results are created
    validation_result_file_path = (
        temp_project_folder / "01_source_data" / "hydamo_validation" / "results" / "results.gpkg"
    )
    assert validation_result_file_path.exists()

    logger.info(
        "SchematisationBuilder test run completed. Now check if output files contain expected layers, columns and data."
    )

    # Test if the output files contain expected layers
    damo_layers = hrt.vector.get_layers(damo_file_path)
    hydamo_layers = hrt.vector.get_layers(hydamo_file_path)
    validation_layers = hrt.vector.get_layers(validation_result_file_path)
    for layer in TABLE_NAMES:
        assert layer in damo_layers, f"Layer {layer} not found in DAMO file."
        assert layer in hydamo_layers, f"Layer {layer} not found in HyDAMO file."
        assert layer in validation_layers, f"Layer {layer} not found in validation results."

    # Test if the output files contain expected columns and data
    # TODO: check if these are the right columns
    expected_columns = {
        "GEMAAL": ["code", "functiegemaal", "vermogen", "peilgebied"],
        "HYDROOBJECT": ["code", "typeobject", "naam", "lengte"],
        "COMBINATIEPEILGEBIED": ["code", "naam", "gempeil"],
        "DUIKERSIFONHEVEL": ["code", "typeobject", "doorsnede", "materiaal"],
    }
    for layer, columns in expected_columns.items():
        damo_gdf = gpd.read_file(damo_file_path, layer=layer)
        hydamo_gdf = gpd.read_file(hydamo_file_path, layer=layer)
        validation_gdf = gpd.read_file(validation_result_file_path, layer=layer)

        # check if layers contain data/are not empty
        assert not damo_gdf.empty, f"DAMO layer {layer} is empty."
        assert not hydamo_gdf.empty, f"HyDAMO layer {layer} is empty."
        assert not validation_gdf.empty, f"Validation layer {layer} is empty."

        # check if expected columns are present in the layers
        for column in columns:
            assert column in damo_gdf.columns, f"Column {column} not found in DAMO layer {layer}."
            assert column in hydamo_gdf.columns, f"Column {column} not found in HyDAMO layer {layer}."
            assert column in validation_gdf.columns, f"Column {column} not found in validation layer {layer}."

        # check if layers contain expected data
        for index, row in damo_gdf.iterrows():
            assert row.notnull().all(), f"DAMO layer {layer} has null values in row {index}."
        for index, row in hydamo_gdf.iterrows():
            assert row.notnull().all(), f"HyDAMO layer {layer} has null values in row {index}."
        for index, row in validation_gdf.iterrows():
            assert row.notnull().all(), f"Validation layer {layer} has null values in row {index}."

    logger.info("All tests passed.")


# %%
if __name__ == "__main__":
    print(os.getenv("SKIP_DATABASE"))
    test_main()
