r"""This script is used to export data from the DAMO database to a HyDAMO file and validate it.
First a project folders is created.
Than the polder_polygon.shp file have to copied (manually) to the project folder.
Define TABLE_NAMES to select which tables to export from DAMO.
The script will export the data from DAMO to a HyDAMO file and validate it.
Result:
- DAMO.gpkg: exported data from DAMO
- HyDAMO.gpkg: converted data from DAMO to HyDAMO
- 01_source_data\hydamo_validation\results: validation results (gpkg. csv, geojson)
After:
Use the schematisation builder plugin in QGIS to build the schematisation based on the validated HyDAMO file.
"""

# %%
import logging
import shutil
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.folders import Project
from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from hhnk_threedi_tools.core.schematisation_builder.DB_exporter import DATABASES, db_exporter
from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

# %%


def make_validated_hydamo_package(project_folder: Path, table_names: list) -> None:
    """
    Export DAMO data for given area and table layers, and saves it to a DAMO.gpkg file.
    Converts the DAMO data to HyDAMO format and saves it to a HyDAMO.gpkg file.
    Validates the HyDAMO file and saves the results to a validation directory.
    Parameter
    ----------
    project_folder : Path
        Path to the project folder where the data will be saved.
    table_names : list
        List of table names to export from DAMO.
    Return
    -------
    None
        The function does not return anything. It saves the exported and converted data and validation results to the project folder.
    Raise
    ------
    SystemExit
        If the polder_polygon.shp file is not found in the project folder.
    FileNotFoundError
        If the HyDAMO file is not found in the project folder.

    """
    Project(str(project_folder))
    # initialize logger
    logger = hrt.logging.get_logger(__name__, filepath=Path(project_folder) / "log.log")
    logger.setLevel(logging.INFO)

    polder_file_path = project_folder / "01_source_data" / "polder_polygon.shp"
    damo_file_path = project_folder / "01_source_data" / "DAMO.gpkg"
    hydamo_file_path = project_folder / "01_source_data" / "HyDAMO.gpkg"

    # check if polder_polygon.shp exists
    if polder_file_path:
        logger.info(f"Start export from source databases for file: {polder_file_path}")
        # DAMO export
        gdf_polder = gpd.read_file(polder_file_path)
        logging_DAMO = db_exporter(
            model_extent_gdf=gdf_polder,
            output_file=damo_file_path,
            table_names=table_names,
        )

        if logging_DAMO:
            logger.warning("Not all tables have been exported from the DAMO database.")

        # Conversion to HyDAMO
        logger.info(f"DAMO export was succesfull. Now, start conversion to HyDAMO for file: {polder_file_path}")
        converter = DAMO_to_HyDAMO_Converter(
            damo_file_path=damo_file_path, hydamo_file_path=hydamo_file_path, layers=table_names, overwrite=True
        )
        converter.run()

        logger.info(f"HyDAMO exported for file: {polder_file_path}")

    else:
        logger.error("No polder_polygon.shp available, so co file selected for export.")
        # stop the script
        raise SystemExit

    if hydamo_file_path:
        logger.info(f"Start validation of HyDAMO file: {hydamo_file_path}")

        # HyDAMO validation
        validation_directory_path = project_folder / "01_source_data" / "hydamo_validation"

        # copy validationrules.json from resources to project folder
        resources_validationrules_path = hrt.get_pkg_resource_path(
            schematisation_builder_resources, "validationrules.json"
        )
        validation_rules_json_path = validation_directory_path / "validationrules.json"

        if not validation_rules_json_path.exists():
            shutil.copyfile(resources_validationrules_path, validation_rules_json_path)

        coverage_location = validation_directory_path / "dtm"  # r"data/test_HyDAMO_validator/dtm"
        if not Path(coverage_location).exists():  # copy it from static data folder
            shutil.copytree(r"D:/github/evanderlaan/data/test_HyDAMO_validator/dtm_orgineel", coverage_location)

        result_summary = validate_hydamo(
            hydamo_file_path=hydamo_file_path,
            validation_rules_json_path=validation_rules_json_path,
            validation_directory_path=validation_directory_path,
            coverages_dict={"AHN": coverage_location},
            output_types=["geopackage", "csv", "geojson"],
        )

        logger.info(f"Validation of HyDAMO file: {hydamo_file_path} is done.")
        logger.info(f"Validation result: {result_summary}")
        logger.info(f"Validation result is saved in {validation_directory_path}")
        logger.info(
            "Go to QGIS, open this project and use the schematisation builder plugin to build the schematisation based on this validated HyDAMO file."
        )

    else:
        raise FileNotFoundError(f"File {hydamo_file_path} does not exist")


# %%

if __name__ == "__main__":
    # define project folder path and
    project_folder = Path("E:/09.modellen_speeltuin/test_jk1")

    # select which tables names to export from DAMO
    # only 'main'tables have to be selected (like "GEMAAL"), so no 'sub' tables (like "POMP")
    TABLE_NAMES = ["HYDROOBJECT", "DUIKERSIFONHEVEL", "COMBINATIEPEILGEBIED", "PEILGEBIEDPRAKTIJK"]

    # run the function to create a validated HyDAMO package
    make_validated_hydamo_package(project_folder, TABLE_NAMES)


# %%
