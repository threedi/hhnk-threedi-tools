"""This script is used to export data from the DAMO database to a HyDAMO file and validate it.
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
import os
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
from DAMO_exporter import DAMO_exporter
from DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from HyDAMO_validator import validate_hydamo

from hhnk_threedi_tools.core.project import Project

# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# %%

# define project folder path and create project if it does not exist yet
project_folder = Path("E:/09.modellen_speeltuin/demo_van_demo")
Project(str(project_folder))

# TODO: copy polder_polygon.shp of area of interest to project_folder/01_source_data


# %%
polder_file_path = project_folder / "01_source_data" / "polder_polygon.shp"
damo_file_path = project_folder / "01_source_data" / "DAMO.gpkg"
hydamo_file_path = project_folder / "01_source_data" / "HyDAMO.gpkg"

# select which tables names to export from DAMO
TABLE_NAMES = ["HYDROOBJECT", "DUIKERSIFONHEVEL"]

if polder_file_path:
    logger.info(f"Start export from DAMO database for file: {polder_file_path}")
    # DAMO export
    gdf_polder = gpd.read_file(polder_file_path)
    logging_DAMO = DAMO_exporter(gdf_polder, TABLE_NAMES, damo_file_path)

    if logging_DAMO:
        logger.warning("Not all tables have been exported from the DAMO database.")

    # Conversion to HyDAMO
    logger.info(f"DAMO export was succesfull. Now, start conversion to HyDAMO for file: {polder_file_path}")
    converter = DAMO_to_HyDAMO_Converter(
        DAMO_path=damo_file_path, HyDAMO_path=hydamo_file_path, layers=TABLE_NAMES, overwrite=True
    )
    converter.run()

    logger.info(f"HyDAMO exported for file: {polder_file_path}")

else:
    logger.error("No file selected for export.")
    # stop the script
    raise SystemExit

# %%

if hydamo_file_path:
    logger.info(f"Start validation of HyDAMO file: {hydamo_file_path}")

    # HyDAMO validation
    # TODO: nu nog handmatig regels neerzetten, later automatiseren bij aanmaken project
    validation_rules_json_path = project_folder / "00_config" / "validation" / "validationrules.json"
    validation_directory_path = project_folder / "01_source_data" / "hydamo_validation"

    if not validation_rules_json_path.exists():
        shutil.copyfile(r"E:/09.modellen_speeltuin/validationrules.json", validation_rules_json_path)

    coverage_location = validation_directory_path / "dtm"  # r"data/test_HyDAMO_validator/dtm"
    if not Path(coverage_location).exists():  # copy it from static data folder
        shutil.copytree(r"D:/github/evanderlaan/data/test_HyDAMO_validator/dtm", coverage_location)

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
        f"Go to QGIS, open this project and use the schematisation builder plugin to build the schematisation based on this validated HyDAMO file."
    )

else:
    raise FileNotFoundError(f"File {hydamo_file_path} does not exist")

# %%
