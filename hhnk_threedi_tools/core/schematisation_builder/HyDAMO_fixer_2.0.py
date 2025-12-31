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

        # define path to results.gpkg of validation
        self.validation_results_gpkg_path = validation_directory_path / "results" / "results.gpkg"

        self.report_gpkg_path = self.validation_directory_path / "fix_tussenstappen" / "summary_val_fix.gpkg"

    def create_validation_fix_reports(self):
        for layer in self.fix_config["objects"]:
            layer_name = layer["object"]

            # fill in standard columns based on validation results
            val_results_layer_gdf = gpd.read_file(self.validation_results_gpkg_path, layer=layer_name)

            layer_report_gdf = val_results_layer_gdf[
                ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
            ]

            # remove rows where invalid columns are empty strings
            layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
            layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)
            layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")

            # add layer specifivc columns based on fix config
            for fix in layer["fixes"]:
                attribute_name = fix["attribute_name"]
                if attribute_name not in list(layer_report_gdf.columns):
                    # add columns to layer report gdf
                    layer_report_gdf[attribute_name] = None
                    layer_report_gdf[f"validation_sum_{attribute_name}"] = None
                    layer_report_gdf[f"fixes_{attribute_name}"] = None
                    layer_report_gdf[f"manual_overwrite_{attribute_name}"] = None

            for index, row in layer_report_gdf.iterrows():
                # connect IDs of invalid_critical to validation_id in fix config
                if row["invalid_critical"] is not None or row["invalid_non_critical"] is not None:
                    invalid_ids = []
                    if row["invalid_critical"] is not None:
                        invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
                    if row["invalid_non_critical"] is not None:
                        invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

                    for fix in layer["fixes"]:
                        validation_id = fix["validation_id"]
                        attribute_name = fix["attribute_name"]
                        fix_id = fix["fix_id"]
                        fix_name = fix["fix_name"]

                        if validation_id in invalid_ids:
                            # define validation sum text
                            if fix["error_type"] == "critical":
                                text_val_sum = f"C{validation_id}:{fix['error_message']}"
                            else:
                                text_val_sum = f"W{validation_id}:{fix['error_message']}"

                            # fill in the validation sum column
                            current_val_sum = layer_report_gdf.at[index, f"validation_sum_{attribute_name}"]
                            if current_val_sum is None:
                                layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = text_val_sum
                            else:
                                layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = (
                                    f"{current_val_sum};{text_val_sum}"
                                )

                            # define fix suggestion text
                            if fix["fix_type"] == "automatic":
                                text_fix_suggestion = f"AF{fix_id}:{fix_name}"
                            else:
                                text_fix_suggestion = f"MF{fix_id}:{fix_name}"
                            # TODO: als er een aanname of berekning is, deze ook toevoegen aan fix suggestion

                            # fill in fix suggestion column
                            current_fix = layer_report_gdf.at[index, f"fixes_{attribute_name}"]
                            if current_fix is None:
                                layer_report_gdf.at[index, f"fixes_{attribute_name}"] = text_fix_suggestion
                            else:
                                layer_report_gdf.at[index, f"fixes_{attribute_name}"] = (
                                    f"{current_fix};{text_fix_suggestion}"
                                )
                            # TODO: fill met attribute value if present

            # remove columns with no data
            for col in layer_report_gdf.columns:
                # TODO: attribute en manual overwrite moeten blijven staan
                if col not in ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]:
                    if layer_report_gdf[col].isnull().all():
                        layer_report_gdf = layer_report_gdf.drop(columns=[col])
            # save layer report gdf to gpkg

            layer_report_gdf.to_file(self.report_gpkg_path, layer=layer_name, driver="GPKG")
