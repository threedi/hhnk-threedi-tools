import json
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources


class Hydamo_fixer:
    def __init__(
        self,
        hydamo_gpkg_path: str,
        validation_directory_path: Path,
        logger: hrt.logging.Logger = None,
    ) -> None:
        if not logger:
            logger = hrt.logging.get_logger(__name__)
        self.logger = logger

        self.hydamo_gpkg_path = hydamo_gpkg_path

        self.hydamo_layers = gpd.read_file(hydamo_gpkg_path, layer=None)
        self.validation_directory_path = validation_directory_path

        # open validation rules and fix config
        resources_validationrules_path = hrt.get_pkg_resource_path(
            schematisation_builder_resources, "validationrules.json"
        )
        with open(resources_validationrules_path, "r") as f:
            self.validation_rules = json.load(f)

        resources_fixconfig_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json")
        with open(resources_fixconfig_path, "r") as f:
            self.fix_config = json.load(f)

    def create_validation_fix_reports(self):
        standard_columns = ["CODE", "valid", "invalid_critical", "invalid_warning", "ignored"]
        for rules_layer in self.validation_rules["objects"]:
            layer_name = rules_layer["object"]
            layer_specific_columns = standard_columns.copy()

            # define validation columns for table
            # for rule in rules_layer['validation_rules']:
            # print(rule)
            # TODO: add in validation_rules.json a variable: "validated_attribute"
            # TODO: take this variable and add it to layer_specific_columns

            # read hydamo layer
            # hydamo_layer_gdf = gpd.read_file(self.hydamo_gpkg_path, layer=layer_name)
