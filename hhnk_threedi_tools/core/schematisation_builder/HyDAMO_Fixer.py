import json
import logging
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources


class Hydamo_fixer:
    """Class to fix HyDAMO validation issues and create summary reports.
    Parameters
    ----------
    hydamo_gpkg_path : Path
        Path to the HyDAMO geopackage directory.
    validation_directory_path : Path
        Path to the validation directory containing results.
    logger : Optional[logging.Logger], optional
        Logger for logging messages. If not provided, a default logger is used. By default None.
    Returns
    -------
    intermediate results are saved in the fix_phase folder in the validation_directory_path
    intermediate results are (until now):
    - summary_val_fix.gpkg : geopackage with per layer a summary of validation and fix suggestions

    """

    def __init__(
        self,
        hydamo_gpkg_path: Path,
        validation_directory_path: Path,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if not logger:
            logger = hrt.logging.get_logger(__name__)
        self.logger = logger

        # define paths
        self.hydamo_gpkg_path = hydamo_gpkg_path / "HyDAMO.gpkg"
        self.validation_directory_path = validation_directory_path
        self.validation_results_gpkg_path = validation_directory_path / "results" / "results.gpkg"
        self.report_gpkg_path = self.validation_directory_path / "fix_phase" / "summary_val_fix.gpkg"

        # check if hydamo gpkg exists
        if not self.hydamo_gpkg_path.exists():
            raise FileNotFoundError(f"HyDAMO gpkg not found at {self.hydamo_gpkg_path}")

        # check if validation results gpkg exists
        if not self.validation_results_gpkg_path.exists():
            raise FileNotFoundError(f"Validation results gpkg not found at {self.validation_results_gpkg_path}")

        # create fix phase directory if not exists
        if not self.report_gpkg_path.parent.exists():
            self.logger.info(f"Creating fix phase directory at {self.report_gpkg_path.parent}")
            self.report_gpkg_path.parent.mkdir(parents=True, exist_ok=True)

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
        """
        Create validation and fix summary report geopackage in which:
          - current attribute values are updated (from hydamo.gpkg)
          - manual adjustments can be made (see manual_overwrite_* columns)
        Returns:
            None
        """
        # create report gpkg with per layer a summary of validation and fix suggestions
        for layer in self.fix_config["objects"]:
            layer_name = layer["object"]

            self.logger.info(f"Start creating validation and fix summary for layer: {layer_name}")

            # open hydamo layer gdf
            hydamo_layer_gdf = gpd.read_file(self.hydamo_gpkg_path, layer=layer_name)

            # fill in standard columns based on validation results
            val_results_layer_gdf = gpd.read_file(self.validation_results_gpkg_path, layer=layer_name)

            layer_report_gdf = val_results_layer_gdf[
                ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
            ].copy()

            # remove rows where invalid columns are empty strings
            layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
            layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)
            layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")

            self.logger.info(f"Created base report gdf with {len(layer_report_gdf)} objects which need fixes")

            # add layer specific columns based on fix config
            add_specific_columns = []
            for fix in layer["fixes"]:
                attribute_name = fix["attribute_name"]
                if attribute_name not in add_specific_columns:
                    add_specific_columns.append(attribute_name)
                    # add columns to layer report gdf
                    if attribute_name != "geometry":
                        layer_report_gdf[attribute_name] = None
                    layer_report_gdf[f"validation_sum_{attribute_name}"] = None
                    layer_report_gdf[f"fixes_{attribute_name}"] = None
                    layer_report_gdf[f"manual_overwrite_{attribute_name}"] = None

            self.logger.info(f"Added specific columns to report gdf for following attributes: {add_specific_columns}")

            # fill in validation and fix information
            self.logger.info(f"Filling in validation, fix and attribute information for layer: {layer_name}")
            list_attributes_filled = []
            for index, row in layer_report_gdf.iterrows():
                # connect IDs of invalid_critical to validation_id in fix config
                if row["invalid_critical"] is not None or row["invalid_non_critical"] is not None:
                    invalid_ids = []
                    if row["invalid_critical"] is not None:
                        invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
                    if row["invalid_non_critical"] is not None:
                        invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

                    for attribute_fix in layer["fixes"]:
                        validation_ids = attribute_fix["validation_ids"]
                        attribute_name = attribute_fix["attribute_name"]
                        fix_id = attribute_fix["fix_id"]
                        fix_description = attribute_fix["fix_description"]

                        if any(validation_id in invalid_ids for validation_id in validation_ids):
                            # mark attribute as filled
                            if attribute_name not in list_attributes_filled:
                                list_attributes_filled.append(attribute_name)

                            # open validation_rules.json for specific layer
                            validation_rules_layer = self._select_validation_rules_layer(layer_name)

                            # TODO loop through all validation ids of attribute fix which are present in invalid ids'
                            for validation_id in validation_ids:
                                if validation_id in invalid_ids:
                                    # based on validation_rules.json, check error type and message
                                    for rule in validation_rules_layer:
                                        if rule["id"] == validation_id:
                                            # define validation sum text
                                            if rule["error_type"] == "critical":
                                                text_val_sum = f"C{validation_id}:{rule['error_message']}"
                                            else:
                                                text_val_sum = f"W{validation_id}:{rule['error_message']}"

                                            # fill in the validation sum column
                                            current_val_sum = layer_report_gdf.at[
                                                index, f"validation_sum_{attribute_name}"
                                            ]
                                            if current_val_sum is None:
                                                layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = (
                                                    text_val_sum
                                                )
                                            else:
                                                layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = (
                                                    f"{current_val_sum};{text_val_sum}"
                                                )

                            # define fix suggestion text
                            if attribute_fix["fix_type"] == "automatic":
                                text_fix_suggestion = f"AF{fix_id}:{fix_description}"
                            else:
                                text_fix_suggestion = f"MF{fix_id}:{fix_description}"

                            # fill in fix suggestion column
                            current_fix = layer_report_gdf.at[index, f"fixes_{attribute_name}"]
                            if current_fix is None:
                                layer_report_gdf.at[index, f"fixes_{attribute_name}"] = text_fix_suggestion
                            else:
                                layer_report_gdf.at[index, f"fixes_{attribute_name}"] = (
                                    f"{current_fix};{text_fix_suggestion}"
                                )

                            # fill in attribute value if present in hydamo layer
                            if attribute_name in hydamo_layer_gdf.columns and attribute_name != "geometry":
                                code = row["code"]
                                hydamo_value = hydamo_layer_gdf.loc[
                                    hydamo_layer_gdf["code"] == code, attribute_name
                                ].values
                                if len(hydamo_value) > 0:
                                    layer_report_gdf.loc[index, attribute_name] = hydamo_value[0]
                                else:
                                    self.logger.warning(
                                        f"Could not find attribute value for code {code} and attribute {attribute_name}"
                                    )

            self.logger.info(
                f"Filled in validation,fix and attribute information for {list_attributes_filled} attributes in layer {layer_name}"
            )

            # remove columns with no values filled in
            cols_to_save = ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
            for attribute_name in list_attributes_filled:
                cols_to_save += [
                    attribute_name,
                    f"validation_sum_{attribute_name}",
                    f"fixes_{attribute_name}",
                    f"manual_overwrite_{attribute_name}",
                ]
            layer_report_gdf = layer_report_gdf[cols_to_save]

            # save layer report gdf to report gpkg
            layer_report_gdf.to_file(self.report_gpkg_path, layer=layer_name, driver="GPKG")
            self.logger.info(f"Finshed and saved report gdf for layer {layer_name} to {self.report_gpkg_path}")

    def _select_validation_rules_layer(self, layer_name: str):
        """
        Select validation rules for a specific layer from the validation_rules.json
        Parameters:
            layer_name (str): Name of the layer to select validation rules for.
        Returns:
            List of validation rules for the specified layer.
        """
        for rules_layer in self.validation_rules["objects"]:
            if rules_layer["object"] == layer_name:
                return rules_layer["validation_rules"]
        return []  # Return empty list if layer not found
