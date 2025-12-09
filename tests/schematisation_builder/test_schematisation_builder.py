# %%
import json
import os
from pathlib import Path

import dotenv
import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources

# Import DATABASES to check if database settings are available
from tests.config import TEMP_DIR, TEST_DIRECTORY

dotenv.load_dotenv()


@pytest.mark.skipif(
    str(os.getenv("SKIP_DATABASE")) == "1",
    reason="Skipping DB test because no local_settings_htt.py or DATABASES available.",
)
def test_main():
    """Test the main functionality of the SchematisationBuilder class.
    This test runs the full process of creating a HyDAMO package from a DAMO/CSO databases.
    It checks if the output files are created and contain expected layers, columns and data.
    """

    logger = hrt.logging.get_logger(__name__)

    # import SchematisationBuilder here to avoid import issues related to missing database settings
    import sqlite3

    from hhnk_threedi_tools.core.schematisation_builder.main import SchematisationBuilder

    # create temporary project folder path
    temp_project_folder = TEMP_DIR / f"temp_schematisation_builder_{hrt.current_time(date=True)}"

    # default polder polygon is of polder 't hoekje
    default_polder_polygon_path = TEST_DIRECTORY / "model_test" / "01_source_data" / "polder_polygon.shp"

    # all layers which are ready to be exported from DAMO/CSO, converted to HyDAMO and to be validated
    TABLE_NAMES = ["HYDROOBJECT", "GEMAAL", "DUIKERSIFONHEVEL"]

    # run schematisation builder
    logger.info(f"Starting SchematisationBuilder test with project folder: {temp_project_folder}")
    builder = SchematisationBuilder(
        project_folder=temp_project_folder, default_polder_polygon_path=default_polder_polygon_path
    )

    # Part 1: Make DAMO and HyDAMO package
    builder.make_hydamo_package()
    # check if DAMO and HyDAMO files are created
    logger.info("Checking if output files are created.")
    damo_file_path = temp_project_folder / "01_source_data" / "DAMO.gpkg"
    hydamo_file_path = temp_project_folder / "01_source_data" / "HyDAMO.gpkg"
    assert damo_file_path.exists()
    assert hydamo_file_path.exists()

    # Part 2: Validate HyDAMO package

    # TODO TEMP REMOVE LATER
    # Remove unsupported layers from the HyDAMO package for now.
    # A raw export to DAMO converter should be made for these layers in the future.

    if hydamo_file_path.exists():
        hydamo_layers = hrt.SpatialDatabase(hydamo_file_path).available_layers()
        if "kunstwerkopening" not in [layer.lower() for layer in hydamo_layers]:
            logger.warning("Removing unsupported layers from HyDAMO package for now.")
            with sqlite3.connect(hydamo_file_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {'stuw'};")
                cursor.execute(f"DROP TABLE IF EXISTS {'brug'};")
                conn.commit()

    # try:
    #     static_data = json.loads(
    #         hrt.get_pkg_resource_path(schematisation_builder_resources, "static_data_paths.json").read_text()
    #     )
    #     coverage_location = Path(static_data["dtm_path"])
    # except FileNotFoundError:
    #     raise FileNotFoundError(
    #         f"Coverage data for validation not found in {hrt.get_pkg_resource_path(schematisation_builder_resources, 'static_data_paths.json')}. Please provide location with index.shp and dtm tiles to this location."
    #     )

    # path to dtm data
    coverage_location = TEST_DIRECTORY / "model_test" / "02_schematisation" / "00_basis" / "rasters"

    builder.validate_hydamo_package(coverage_location=coverage_location)
    # check if validation results are created
    validation_result_file_path = (
        temp_project_folder / "01_source_data" / "hydamo_validation" / "results" / "results.gpkg"
    )
    assert validation_result_file_path.exists()

    logger.info(
        "SchematisationBuilder test run completed. Now check if output files contain expected layers, columns and data."
    )

    # Test if the output files contain expected layers
    damo_layers = hrt.SpatialDatabase(damo_file_path).available_layers()
    hydamo_layers = hrt.SpatialDatabase(hydamo_file_path).available_layers()
    validation_layers = hrt.SpatialDatabase(validation_result_file_path).available_layers()
    for layer in TABLE_NAMES:
        assert layer.lower() in [l.lower() for l in damo_layers], f"Layer {layer} not found in DAMO file."
        assert layer.lower() in [l.lower() for l in hydamo_layers], f"Layer {layer} not found in HyDAMO file."
        assert layer.lower() in [l.lower() for l in validation_layers], (
            f"Layer {layer} not found in validation results."
        )

    # Test if the output files contain expected columns and data
    expected_columns_hydamo = {
        "GEMAAL": ["code", "functiegemaal", "globalid", "NEN3610id"],
        "HYDROOBJECT": ["code", "categorieoppwaterlichaamcode", "NEN3610id", "lengte"],
        "DUIKERSIFONHEVEL": ["code", "lengtebeheerobject", "NEN3610id"],
    }
    for layer, columns in expected_columns_hydamo.items():
        damo_gdf = gpd.read_file(damo_file_path, layer=layer)
        hydamo_gdf = gpd.read_file(hydamo_file_path, layer=layer)
        validation_gdf = gpd.read_file(validation_result_file_path, layer=layer)

        # check if layers contain data/are not empty
        assert not damo_gdf.empty, f"DAMO layer {layer} is empty."
        assert not hydamo_gdf.empty, f"HyDAMO layer {layer} is empty."
        assert not validation_gdf.empty, f"Validation layer {layer} is empty."

        # check if expected columns are present in the layers in hydamo and validation files
        for column in columns:
            assert column in hydamo_gdf.columns, f"Column {column} not found in HyDAMO layer {layer}."

    logger.info("All tests passed.")


# %%
if __name__ == "__main__":
    print(f"SKIP_DATABASE: {os.getenv('SKIP_DATABASE')}")
    test_main()

# %%
