import json
import logging
import re
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.utils import logical_fix


class FixConfig:
    def __init__(self):
        resources_fixconfig_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json")
        with open(resources_fixconfig_path, "r") as f:
            fix_config = json.load(f)

        self.schema = fix_config["schema"]
        self.hydamo_version = fix_config["hydamo_version"]
        self.objects = fix_config["objects"]

        class Objects:
            def __init__(self, objects):
                for obj in objects:
                    setattr(self, obj["object"])


class FIX_MAPPING:
    skip = 1
    edit = 2
    multi_edit = 3


class HyDAMOFixer:
    """Class to fix HyDAMO validation issues and create summary reports.
    Parameters
    ----------
    hydamo_file_path : Path
        Path to the HyDAMO geopackage file.
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
        hydamo_file_path: Path,
        validation_directory_path: Path,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if not logger:
            logger = hrt.logging.get_logger(__name__)
        self.logger = logger

        # define paths
        self.hydamo_file_path = hydamo_file_path / "HyDAMO.gpkg"
        self.validation_directory_path = validation_directory_path
        self.validation_results_gpkg_path = validation_directory_path / "results" / "results.gpkg"
        self.report_gpkg_path = self.validation_directory_path / "fix_phase" / "summary_val_fix.gpkg"

        # check if hydamo gpkg exists
        if not self.hydamo_file_path.exists():
            raise FileNotFoundError(f"HyDAMO gpkg not found at {self.hydamo_file_path}")

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

        self.fix_overview = {}

    def create_validation_fix_reports(self):
        """
        Create validation and fix overview report
        Inputs:
            1. HyDAMO.gpkg
            2. validation results gpkg (results.gpkg in validation directory)
            3. validation_rules.json
            4. FixConfig.json
        Report includes per layer:
          - current attribute values (from hydamo.gpkg)
          - colomn for manual adjustments (see manual_overwrite_* columns)
          - validation summary per attribute
          - fix suggestions per attribute
          - summary columns (similar to validation results gpkg)
        Returns:
            NoneSS
        """
        # create report gpkg with per layer a summary of validation and fix suggestions
        for layer in self.fix_config["objects"]:
            layer_name = layer["object"]

            self.logger.info(f"Start creating validation and fix summary for layer: {layer_name}")

            # open hydamo layer gdf
            hydamo_layer_gdf = gpd.read_file(self.hydamo_file_path, layer=layer_name)

            # fill in standard columns based on validation results
            val_results_layer_gdf = gpd.read_file(self.validation_results_gpkg_path, layer=layer_name)

            layer_report_gdf = val_results_layer_gdf[
                ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
            ].copy()

            # remove rows where invalid columns are empty strings
            layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
            layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)
            layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")

            if layer_report_gdf.empty:
                self.logger.info(
                    f"No invalid features found in layer {layer_name}, fixing is not needed/finished for this layer."
                )
                return

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

                            # Loop through all validation ids of attribute fix which are present in invalid ids'
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

            self.fix_overview[layer_name] = layer_report_gdf

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

    ## remove: woord dekt niet de lading, even over nadenken
    ## validation ids: gaat er meer om dat een attribuut een fix moet krijgen als ie om wat voor reden dan ook niet valide is
    ## create our own version of logical_validation.execute() to use the general, logical and topological functions of the hydamo validator
    ## maybe call is logical_fix.execute()
    ## while running a fix, keep track of the inputs. If an import

    def execute(
        ## read validation fix report
        ## apply fix actions to hydamo gpkg
        ## update validation fix report with history
        self,
    ):
        """Execute the logical fixes."""

        object_dict: dict[str, gpd.GeoDataFrame] = self.fix_overview
        execution_dict = {}
        for (
            layer_name,
            object_gdf,
        ) in (
            object_dict.items()
        ):  ## for now use this, but in the end we need to figure out an order to do certain layers before the other.
            fix_config_object = next(obj for obj in self.fix_config["objects"] if obj["object"] == layer_name)
            fix_config_fixes = fix_config_object["fixes"]  ## list with all the fixes for every attribute possible
            # fix_type = fix_config_fixes["fix_type"]
            # fix_action = fix_config_fixes["fix_action"]

            fix_cols = [c for c in object_gdf.columns if c.startswith("fixes_")]
            manual_cols = [c for c in object_gdf.columns if c.startswith("manual_overwrite_")]
            results = []
            for idx, row in object_gdf.iterrows():
                feature_dict = {
                    "id": idx,
                    "code": object_gdf["code"],
                    "fixes": {col.replace("fixes_", ""): row[col] for col in fix_cols},
                    "manual_inputs": {col.replace("manual_overwrite_", ""): row[col] for col in manual_cols},
                }
                results.append(feature_dict)

            ## execution_dict: {
            #   "fix_id": ...
            #   "fix": ...
            # }
            execution_dict[layer_name] = {}
            list_features_to_remove = []  ## codes
            highest_prios = []
            for result in results:
                fixes: dict = result["fixes"]
                fix_suggestions = fixes.values()
                fix_ids = [int(re.search(r"^[A-Za-z]{2}(\d+):", fix).group(1)) for fix in fix_suggestions]
                highest_prio_fix = min(fix_ids)
                highest_prios.append(highest_prio_fix)
                if highest_prio_fix == 1:
                    list_features_to_remove.append(result["code"])
                    execution_dict[layer_name]["fix_id"] = min(highest_prios)
                    execution_dict[layer_name]["inputs"] = list_features_to_remove

        for layer, execution in execution_dict.items():
            if execution["fix_id"] == FIX_MAPPING.skip:
                logical_fix.skip_features(
                    gdf_HyDAMO=self.hydamo_file_path, layer=layer, list_features=execution["inputs"]
                )
            if execution["fix_id"] == FIX_MAPPING.edit:
                pass
                ## do the edit logic
            if execution["fix_id"] == FIX_MAPPING.multi_edit:
                pass

            ## check fixes
            ## for each fix suggested per feature, execute the fix
            ## compile a dict of fixes per feature
            ## use typing to rank the used fix
            ## maybe use levels in fixconfig to denote importance

            # if fix_type == "automatic":
            #     ## use the rules
            #     pass
            # elif fix_type == "manual":
            #     ## use the value that is filled in the manual column
            #     pass

            # if fix_action == "Remove":
            #     pass

            # elif rule["type"] == "logic":
            #     object_gdf.loc[indices, (result_variable)] = (
            #         _process_logic_function(
            #             object_gdf.loc[indices], function, input_variables
            #         )
            #     )
            # elif (rule["type"] == "topologic") and (
            #     hasattr(datamodel, "hydroobject")
            # ):
            #     result_series = _process_topologic_function(
            #         # getattr(
            #         #     datamodel, object_layer
            #         # ),  # FIXME: commented as we need to apply filter in topologic functions as well. Remove after tests pass
            #         object_gdf,
            #         datamodel,
            #         function,
            #         input_variables,
            #     )
            #     object_gdf.loc[indices, (result_variable)] = result_series.loc[
            #         indices
            #     ]
