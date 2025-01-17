# %%
import logging
import os
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
from DAMO_exporter import DAMO_exporter
from DAMO_HyDAMO_converter import Converter
from HyDAMO_validator import validate_hydamo

from hhnk_threedi_tools.core.project import Project

# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# %%

# define project folder path and create project if it does not exist yet
project_folder = Path("E:/09.modellen_speeltuin/egmondermeer_leggertool_test4")
Project(str(project_folder))


# %%
polder_file_path = project_folder / "01_source_data" / "polder_polygon.shp"
damo_file_path = project_folder / "01_source_data" / "DAMO.gpkg"
hydamo_file_path = project_folder / "01_source_data" / "HyDAMO.gpkg"

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
    converter = Converter(DAMO_path=damo_file_path, HyDAMO_path=hydamo_file_path, layers=TABLE_NAMES)
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
