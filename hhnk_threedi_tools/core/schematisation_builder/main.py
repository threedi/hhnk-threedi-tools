import json
import logging
import shutil
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.folders import Project
from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from hhnk_threedi_tools.core.schematisation_builder.DB_exporter import db_exporter
from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.gemaal_converter import (
    GemaalConverter,
)


class SchematisationBuilder:
    def __init__(
        self,
        project_folder: Path,
        default_polder_polygon_path: Path,
        table_names: list,
        damo_version: str = "2.4.1",
        hydamo_version: str = "2.4",
    ):
        self.project_folder = Path(project_folder)
        self.default_polder_polygon_path = Path(default_polder_polygon_path)
        self.table_names = table_names
        self.damo_version = damo_version
        self.hydamo_version = hydamo_version

        self.project = Project(str(self.project_folder))
        self.polder_file_path = self.project.folders.source_data.polder_polygon.path
        self.raw_export_file_path = self.project_folder / "01_source_data" / "raw_export.gpkg"
        self.damo_file_path = self.project.folders.source_data.damo.path
        self.hydamo_file_path = self.project.folders.source_data.hydamo.path

        self.logger = hrt.logging.get_logger(__name__, filepath=self.project_folder / "log.log")
        self.logger.setLevel(logging.INFO)

    def make_hydamo_package(self):
        # Check if polder_polygon.shp exists, copy if not
        if (
            not self.polder_file_path.exists()
        ):  # TODO remove once implemented in plugin, then polder_polygon.shp is always present
            self.logger.info(
                f"polder_polygon.shp not found in {self.project_folder}/01_source_data, copying from default location."
            )
            shutil.copytree(
                self.default_polder_polygon_path,
                self.project_folder / "01_source_data",
                dirs_exist_ok=True,
            )
            self.logger.info(f"polder_polygon.shp copied to {self.project_folder}/01_source_data")
        else:
            self.logger.info(
                f"polder_polygon.shp found in {self.project_folder}/01_source_data, using this file for export."
            )

        if self.polder_file_path.exists():
            self.logger.info(f"Start export from source databases for file: {self.polder_file_path}")
            gdf_polder = gpd.read_file(self.polder_file_path)
            logging_DAMO = db_exporter(
                model_extent_gdf=gdf_polder,
                output_file=self.raw_export_file_path,
                table_names=self.table_names,
            )

            if len(logging_DAMO) > 0:
                self.logger.warning("Not all tables have been exported from the DAMO database.")

            self.logger.info(
                f"DAMO export was succesfull. Now, start conversion to HyDAMO for file: {self.polder_file_path}"
            )

            gdf_polder.to_file(self.raw_export_file_path, layer="polder", driver="GPKG")

            converter = GemaalConverter(
                raw_export_file_path=self.raw_export_file_path,
                output_file_path=self.damo_file_path,
                logger=self.logger,
            )
            converter.run()

            converter = DAMO_to_HyDAMO_Converter(
                damo_file_path=self.damo_file_path,
                damo_version=self.damo_version,
                hydamo_file_path=self.hydamo_file_path,
                hydamo_version=self.hydamo_version,
                layers=None,
                overwrite=True,
                convert_domain_values=False,
            )

            converter.run()
            self.logger.info(f"HyDAMO exported for file: {self.polder_file_path}")

        else:
            self.logger.error("No polder_polygon.shp available, so no file selected for export.")
            raise SystemExit

    def validate_hydamo_package(self):
        if self.hydamo_file_path.exists():
            self.logger.info(f"Start validation of HyDAMO file: {self.hydamo_file_path}")

            validation_directory_path = self.project_folder / "01_source_data" / "hydamo_validation"

            resources_validationrules_path = hrt.get_pkg_resource_path(
                schematisation_builder_resources, "validationrules.json"
            )
            validation_rules_json_path = validation_directory_path / "validationrules.json"

            if not validation_rules_json_path.exists():
                shutil.copyfile(resources_validationrules_path, validation_rules_json_path)

            coverage_location = validation_directory_path / "dtm"
            if not Path(coverage_location).exists():
                static_data = json.loads(
                    hrt.get_pkg_resource_path(schematisation_builder_resources, "static_data_paths.json").read_text()
                )
                dtm_path = Path(static_data["dtm_path"])
                shutil.copytree(dtm_path, coverage_location)

            result_summary = validate_hydamo(
                hydamo_file_path=self.hydamo_file_path,
                validation_rules_json_path=validation_rules_json_path,
                validation_directory_path=validation_directory_path,
                coverages_dict={"AHN": coverage_location},
                output_types=["geopackage", "csv", "geojson"],
            )

            self.logger.info(f"Validation of HyDAMO file: {self.hydamo_file_path} is done.")
            self.logger.info(f"Validation result: {result_summary}")
            self.logger.info(f"Validation result is saved in {validation_directory_path}")
            self.logger.info(
                "Go to QGIS, open this project and use the schematisation builder plugin to build the schematisation based on this validated HyDAMO file."
            )
        else:
            raise FileNotFoundError(f"File {self.hydamo_file_path} does not exist")

    def fix_hydamo_package(self):
        self.logger.info("fix_hydamo_package not implemented yet.")
        pass

    def convert_to_3Di(self):
        self.logger.info("convert_to_3Di not implemented yet.")
        pass


if __name__ == "__main__":
    # ask input from user
    project_folder = Path("E:/09.modellen_speeltuin/test_main_esther2")
    default_polder_polygon_path = Path("E:/09.modellen_speeltuin/place_polder_polygon_here_for_schematisationbuilder")
    TABLE_NAMES = ["HYDROOBJECT", "GEMAAL", "COMBINATIEPEILGEBIED", "DUIKERSIFONHEVEL"]

    builder = SchematisationBuilder(project_folder, default_polder_polygon_path, TABLE_NAMES)
    builder.make_hydamo_package()
    builder.validate_hydamo_package()
    # builder.fix_hydamo_package()
    # builder.convert_to_3Di()
