import json
import logging
import re
import shutil
import time
from functools import partial
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, List, Literal, Optional, Tuple, Union

import geopandas as gpd
import hhnk_research_tools as hrt
import hydamo_validation.schemas as hydamo_validation_schemas
import pandas as pd
from core.schematisation_builder.utils import hydamo_fixes
from core.schematisation_builder.utils.hydamo_fixes import ExtendedHyDAMO
from hydamo_validation import logical_validation
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.datasets import DataSets
from hydamo_validation.logical_validation import (
    _process_general_function,
    _process_logic_function,
    _process_topologic_function,
)
from hydamo_validation.syntax_validation import (
    datamodel_layers,
    fields_syntax,
    missing_layers,
)
from hydamo_validation.utils import Timer
from hydamo_validation.validator import read_validation_rules
from jsonschema import ValidationError, validate

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.utils.summaries import ExtendedLayersSummary, ExtendedResultSummary

SCHEMAS_PATH = hrt.get_pkg_resource_path(schematisation_builder_resources, "schemas")
HYDAMO_SCHEMAS_PATH = hrt.get_pkg_resource_path(hydamo_validation_schemas, "hydamo")
INCLUDE_COLUMNS = []


def fix_hydamo():
    hydamo_fixer = HyDAMOFixer.fixer()
    hydamo = HyDAMO().from_geopackage(...)
    ## do a run to get the fix_overview
    datamodel, layer_summary, result_summary = hydamo_fixer(
        directory=validation_directory_path, datamodel=hydamo, raise_error=True
    )
    ## then do a run to fix hydamo
    return result_summary.to_dict()


class FixConfig:
    def __init__(self):
        self.fix_config = self._read_fix_config()
        resources_fixconfig_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json")
        with open(resources_fixconfig_path, "r") as f:
            fix_config = json.load(f)

        self.schema = fix_config["schema"]
        self.hydamo_version = fix_config["hydamo_version"]
        self.objects = fix_config["objects"]

    def _read_fix_config(self):
        pass


class FIX_MAPPING:
    omit = 1
    edit = 2
    multi_edit = 3


## FIXME: Make HyDAMOFixer subclass of FixConfig (?)
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
    - validaton_fix_overview.gpkg : geopackage with per layer a summary of validation and fix suggestions

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
        # self.hydamo_file_path = hydamo_file_path
        # self.validation_directory_path = validation_directory_path
        # self.validation_results_gpkg_path = validation_directory_path / "results" / "results.gpkg"
        # self.report_gpkg_path = self.validation_directory_path / "fix_phase" / "validaton_fix_overview.gpkg"
        # self.hydamo_fixed_file_path = self.validation_directory_path / "results" / "HyDAMO_fix.gpkg"

        # # check if hydamo gpkg exists
        # if not self.hydamo_file_path.exists():
        #     raise FileNotFoundError(f"HyDAMO gpkg not found at {self.hydamo_file_path}")

        # # check if validation results gpkg exists
        # if not self.validation_results_gpkg_path.exists():
        #     raise FileNotFoundError(f"Validation results gpkg not found at {self.validation_results_gpkg_path}")

        # # create fix phase directory if not exists
        # if not self.report_gpkg_path.parent.exists():
        #     self.logger.info(f"Creating fix phase directory at {self.report_gpkg_path.parent}")
        #     self.report_gpkg_path.parent.mkdir(parents=True, exist_ok=True)

        # # open validation rules and fix config
        # resources_validationrules_path = hrt.get_pkg_resource_path(
        #     schematisation_builder_resources, "validationrules.json"
        # )
        # with open(resources_validationrules_path, "r") as f:
        #     self.validation_rules = json.load(f)

        # resources_fixconfig_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json")
        # with open(resources_fixconfig_path, "r") as f:
        #     self.fix_config = json.load(f)

        # self.fix_overview = {}

        # if not self.hydamo_fixed_file_path.exists():
        #     shutil.copy(self.hydamo_file_path, self.hydamo_fixed_file_path)

    def _load_attribute_data(self):
        pass

    def create_validation_fix_reports(self):
        ## FIXME: populate FixSummary class
        """
        Create validation and fix overview report
        Inputs:
            1. HyDAMO.gpkg
            2. validation results gpkg (results.gpkg in validation directory)
            3. validation_rules.json
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
            layer_report_gdf["invalid_critical"] = (
                layer_report_gdf["invalid_critical"].fillna("").astype(str)
                + layer_report_gdf["invalid_critical"].apply(lambda x: "; " if x else "")
                + layer_report_gdf["ignored"].fillna("").astype(str)
            )
            layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
            layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)

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
                    # # add columns to layer report gdf
                    if attribute_name != "geometry":
                        #     layer_report_gdf[attribute_name] = hydamo_layer_gdf[attribute_name] if attribute_name in hydamo_layer_gdf.columns else None
                        layer_report_gdf[attribute_name] = None
                    layer_report_gdf[f"validation_sum_{attribute_name}"] = None
                    layer_report_gdf[f"fixes_{attribute_name}"] = None
                    layer_report_gdf[f"manual_overwrite_{attribute_name}"] = None

            layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")
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
                        fix_method = attribute_fix["fix_method"]
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
                                if fix_id == 2 and "equal" in list(fix_method.keys()):
                                    hydamo_value = hydamo_layer_gdf[fix_method["equal"]["to"]].values
                                else:
                                    hydamo_value = hydamo_layer_gdf.loc[
                                        hydamo_layer_gdf["code"] == code, attribute_name
                                    ].values
                                if len(hydamo_value) > 0:
                                    layer_report_gdf.loc[index, attribute_name] = hydamo_value[0]
                                else:
                                    self.logger.warning(
                                        f"Could not find attribute value for code {code} and attribute {attribute_name}"
                                    )
            print(layer_report_gdf["breedteopening"])

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

    def _check_fix_history():
        pass

    def _continue(self, message="Would you like to proceed? (y/n): "):
        while True:
            choice = input(message).strip().lower()
            if choice in ("y", "yes"):
                return True
            elif choice in ("n", "no"):
                return False
            else:
                print("Please answer with 'y' or 'n'.")

    def _read_schema(self, version: str, schemas_path: Path):
        schema_json = schemas_path.joinpath(rf"fixes_{version}.json").resolve()
        with open(schema_json) as src:
            schema = json.load(src)
        return schema

    def _check_attributes(gdf, attributes):
        for i in attributes:
            if type(i) == str:
                if not i in gdf.columns:
                    raise KeyError(rf"'{i}' not in columns: {gdf.columns.to_list()}. Rule cannot be executed")
        ## maybe write logic to check whether attributes are dependent on another dataframe or smth

    def _validate_fix_summary():
        pass

    def _init_logger(self, log_level: str):
        """Init logger for validator."""
        self.logger.setLevel(getattr(logging, log_level))
        return self.logger

    def _add_log_file(self, logger: logging.Logger, log_file: Path):
        """Add log-file to existing logger"""
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s - %(message)s"))
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
        return logger

    def _close_log_file(self, logger: logging.Logger):
        """Remove log-file from existing logger"""
        for h in logger.handlers:
            h.close()
            logger.removeHandler(h)

    def _log_to_results(self, log_file: Path, result_summary: ExtendedResultSummary):
        result_summary.log = log_file.read_text().split("\n")

    def _fixer(
        self,
        directory,
        output_types: List[str] = ["geopackage"],
        log_level: Literal["INFO", "DEBUG"] = "INFO",
        coverages: dict = {},
        raise_error: bool = False,
    ):
        timer = Timer()
        # try:
        results_path = None
        dir_path = Path(directory)
        logger = self._init_logger(
            log_level=log_level,
        )

        logger.info("init validatie")
        date_check = pd.Timestamp.now().isoformat()

        ## ----------------
        ## Write some logic that if there is no fixes.gpkg, this is created.
        ## If it exists, fill FixSummary from the gpkg.
        ## Implement a version of the create_report function
        fix_summary = ExtendedLayersSummary(date_check=date_check)  # saved as fix_overview.gpkg
        result_summary = ExtendedResultSummary(date_check=date_check)  # saved as fix_result.json
        # layers_summary = LayersSummary(date_check=date_check) # saved as results.gpkg
        ## Find a way to link HyDAMO to FixSummary
        ## ----------------
        print("\n" + "=" * 60)
        print(" PAUSE: User review required ")
        print("=" * 60)
        print(f"You can inspect or edit the file in: {results_path}")
        print("Please inspect/edit the files as needed.")
        # if not self._continue("Do you want to apply your fixes to HyDAMO? (y/n): "):
        #     print("Hydamo fixer stopped at user request.")
        #     print("Fixes not applied.")
        #     return None, layers_summary, result_summary

        # check if all files are present
        # create a results_path
        results_permission_error = review_permission_error = False
        if dir_path.exists():
            review_path = dir_path.joinpath("review")
            if review_path.exists():
                try:
                    shutil.rmtree(review_path)
                except PermissionError:
                    review_permission_error = True
            review_path.mkdir(parents=True, exist_ok=True)
            results_path = dir_path.joinpath("results")
            if results_path.exists():
                try:
                    shutil.rmtree(results_path)
                except PermissionError:
                    results_permission_error = True
            results_path.mkdir(parents=True, exist_ok=True)
        else:
            raise FileNotFoundError(f"{dir_path.absolute().resolve()} does not exist")

        log_file = results_path.joinpath("fixer.log")
        logger = self._add_log_file(logger, log_file=log_file)
        logger.info("start hydamo fixer")
        if review_permission_error:
            logger.warning(f"Kan pad {review_path} niet verwijderen. Dit kan later tot problemen leiden!")
        if results_permission_error:
            logger.warning(f"Kan pad {results_path} niet verwijderen. Dit kan later tot problemen leiden!")
        dataset_path = dir_path.joinpath("datasets")
        hydamo_gpkg = dataset_path / "HyDAMO_validated.gpkg"
        validation_rules_json = dir_path.joinpath("validationrules.json")
        validation_results_gpkg = dir_path / "results.gpkg"
        missing_paths = []
        for path in [dataset_path, hydamo_gpkg, validation_rules_json, validation_results_gpkg]:
            if not path.exists():
                missing_paths += [str(path)]
        if missing_paths:
            result_summary.error += [f"missing_paths: {','.join(missing_paths)}"]
            raise FileNotFoundError(f"missing_paths: {','.join(missing_paths)}")
        else:
            validation_rules_sets = read_validation_rules(validation_rules_json, result_summary)
            validation_rules_objects = validation_rules_sets["objects"]
            # fix_rules_sets = self.read_fix_rules(
            #     fix_rules_json=hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json"),
            #     result_summary=result_summary
            # )
        print("validation_rule is valid, with fix_rule added in it")
        ### Wat als we de fix rules toevoegen aan de validation rules en een fix rule required maken voor elke attribuut / input van een validatie rule?

        # check if output-files are supported
        unsupported_output_types = [item for item in output_types if item not in ["geopackage"]]
        if unsupported_output_types:
            error_message = r"unsupported output types: " f"{','.join(unsupported_output_types)}"
            result_summary.error += [error_message]
            raise TypeError(error_message)

        # set coverages
        if coverages:
            for key, item in coverages.items():
                logical_validation.general_functions._set_coverage(key, item)

        # start validation
        # read data-model
        result_summary.status = "load data-model"
        datasets = DataSets(dataset_path)
        try:
            hydamo_version = validation_rules_sets["hydamo_version"]
            hydamo_schema_layers = HyDAMO(
                version=hydamo_version,
                schemas_path=HYDAMO_SCHEMAS_PATH,
            ).layers
            schema_layers_not_in_dataset = [i for i in hydamo_schema_layers if i not in datasets.layers]
            datamodel = ExtendedHyDAMO.from_geopackage(
                hydamo_path=hydamo_gpkg,
                results_path=validation_results_gpkg,
                rules_objects=validation_rules_objects,
                version=hydamo_version,
                ignored_layers=schema_layers_not_in_dataset,
            )
        except Exception as e:
            result_summary.error = ["datamodel cannot be defined (see exception)"]
            raise e

        # validate dataset syntax
        result_summary.status = "fix-preparation (layers)"
        result_summary.dataset_layers = datasets.layers

        ## validate syntax of datasets on layers-level and append to result
        logger.info("start fix-voorbereiding van object-lagen")
        valid_layers = datamodel_layers(datamodel.layers, datasets.layers)
        result_summary.missing_layers = missing_layers(datamodel.layers, datasets.layers)

        ## validate valid_layers on fields-level and add them to data_model
        result_summary.status = "fix-preparation (fields)"
        fix_preparation_result = []

        ## get status_object if any
        status_object = None
        if "status_object" in validation_rules_sets.keys():
            status_object = validation_rules_sets["status_object"]
            ## allows us to filter the invalid rows. Only need to add status_object to gdf based on validation result. Should be valid or invalid

        datamodel_check, fix_summary, result_summary = hydamo_fixes.run(
            datamodel,
            fix_summary,
            result_summary,
            logger,
            raise_error,
            keep_general=False,
        )
        datamodel_check.to_geopackage(results_path / "HyDAMO_fixed.gpkg")

        fix_summary, result_summary = hydamo_fixes.review(
            datamodel_check,
            fix_summary,
            result_summary,
            # valid_layers,
            # ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"],
            logger,
            raise_error,
        )
        fix_summary.export(results_path=review_path, gpkg_name="fix_overview_test.gpkg", output_types=["geopackage"])
        stop

        general_rules_lookup_table = hydamo_fixes._build_general_rules_lookup_table(datamodel.validation_rules)
        general_mapping = hydamo_fixes.map_general_rule_inputs(
            datamodel,
            valid_layers,
        )
        # print(general_rules_lookup_table["profiellijn"])
        # print(validation_mapping["duikersifonhevel"])
        # with open(Path.home() / "general_rule_lookup_table.json", "w", encoding="utf-8", newline="\n") as dst:
        #     json.dump(general_rules_lookup_table, dst, indent=4)

        with open(Path.home() / "test_general_mapping.json", "w", encoding="utf-8", newline="\n") as dst:
            json.dump(general_mapping, dst, indent=4)

        stop

        for layer in valid_layers:
            ## This step reads data from dataset, sets it to an empty or filled in datamodel and then does a syntax check. Also layerssummary is updated.
            ## If a datamodel already is in place, use this for loop to create the fix_overview in layerssummary but dont have to change the datamodel
            logger.info(f"{layer}: inlezen")

            # read layer
            gdf, schema = datamodel.read_layer(
                layer, result_summary=result_summary, status_object=status_object
            )  ## could maybe be done with _get_schema()

            if gdf.empty:  # pass if gdf is empty. Most likely due to mall-formed or ill-specifiec status_object
                logger.warning(
                    f"{layer}: geen objecten ingelezen. Zorg dat alle waarden in de kolom 'status_object' voorkomen in {status_object}"
                )
                continue

            layer = layer.lower()
            for col in INCLUDE_COLUMNS:  ## should be the validation overview fields
                if col not in gdf.columns:
                    gdf[col] = None
                    schema["properties"][col] = "str"

            # if layer == "duikersifonhevel":
            #     print(schema)
            #     stop

            logger.info(f"{layer}: fix-voorbereiding")
            fix_gdf = hydamo_fixes.prepare(
                gdf,
                layer=layer,
                schema=schema,  ## schema is most likely needed to account for ignored validation rules
                validation_schema=datamodel.validation_schemas[layer],
                validation_result=datamodel.validation_results[layer],
                validation_rules=datamodel.validation_rules[layer],
                attribute_mapping=datamodel.attribute_mapping[layer],
                keep_columns=["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"],
                logger=logger,
                raise_error=raise_error,
            )

            ## shoud this function be updated to validate gdf based on fix schema?
            ## it does do something to hydamo data and then is set to datamodel
            ## probably needed for logical_validation.execute()
            ## maybe keep and then just remove the syntax_columns
            ## ----> This should be the create_report() function.
            ## Data is then stored in layers_summary and later exported as fix_overview.gpkg

            # Add the syntax-validation result to the results_summary
            fix_summary.set_data(fix_gdf, layer, schema["geometry"])
            # Add the corrected datasets_layer data to the datamodel.
            # if gdf.empty:
            #     logger.warning(
            #         f"{layer}: geen valide objecten na syntax-validatie. Inspecteer 'syntax_oordeel' in de resultaten; deze is false voor alle objecten. De laag zal genegeerd worden in de (topo)logische validatie."
            #     )
            # else:
            #     datamodel.set_data(hydamo_gdf, layer, index_col=None)
            fix_preparation_result += [layer]

        ## -------------------
        ## Do an export here of fix_overview to and request user input to continue
        fix_layers = fix_summary.export(
            results_path=review_path, gpkg_name="fix_overview.gpkg", output_types=["geopackage"]
        )
        ## ----------------
        print("\n" + "=" * 60)
        print(" PAUSE: User review required ")
        print("=" * 60)
        print(f"You can inspect or edit the file in: {results_path}")
        print("Please inspect/edit the files as needed.")
        time.sleep(0.1)
        # if not self._continue("Do you want to apply your fixes to HyDAMO? (y/n): "):
        #     print("Hydamo fixer stopped at user request.")
        #     print("Fixes not applied.")
        fix_summary = ExtendedLayersSummary.from_geopackage(file_path=review_path / "fix_overview.gpkg")
        ## -------------------

        # do logical validation: append result to layers_summary
        result_summary.status = "apply fixes"
        logger.info("start automatische fix van object-lagen")

        ## ---------------------
        ## now we're entering logical_fix.execute() territory
        ## add a try expect method maybe to see wether fixes are applicable
        ## ---------------------
        datamodel, fix_summary, result_summary = hydamo_fixes.execute(
            datamodel,
            validation_rules_sets,
            fix_summary,
            result_summary,
            logger,
            raise_error,
        )
        datamodel.to_geopackage(results_path / "HyDAMO_fixed.gpkg")

        # finish validation and export results
        logger.info("exporteren resultaten")
        result_summary.status = "export results"
        result_summary.fix_layers = fix_layers
        result_summary.error_layers = [i for i in datasets.layers if i.lower() not in fix_layers]
        result_summary.prep_result = fix_preparation_result
        result_summary.fix_result = [
            i["object"] for i in validation_rules_sets["objects"] if i["object"] in fix_layers
        ]
        result_summary.success = True
        result_summary.status = "finished"
        result_summary.duration = timer.report()
        logger.info(f"klaar in {result_summary.duration:.2f} seconden")

        self._log_to_results(log_file, result_summary)
        result_summary.to_json(results_path, "fix_result.json")

        self._close_log_file(logger)

        ## Make sure that this function works but just returns the unfixed gdf. Then we can implement the fixes using hydamo_validation method
        ## For the sprint review, set these tasks in github as proposed targets for next sprint

        return datamodel, fix_summary, result_summary

    def fixer(
        self,
        output_types: List[str] = ["geopackage"],
        log_level: Literal["INFO", "DEBUG"] = "INFO",
        coverages: dict = {},
    ) -> Callable:
        """

        Parameters
        ----------
        output_types : List[str], optional
            The types of output files that will be written. Options are
            ["geojson", "csv", "geopackage"]. By default all will be written
        log_level : Literal['INFO', 'DEBUG'], optional
            Level for logger. The default is "INFO".
        coverages : dict, optional
        Location of coverages. E.g. {"AHN: path_to_ahn_dir} The default is {}.

        Returns
        -------
        Callable[[str], dict]
            Partial of _validator function

        """

        return partial(
            self._fixer,
            output_types=output_types,
            log_level=log_level,
            coverages=coverages,
        )

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
            ## Store the fixes for each feature and each attribute in a feature_dict and save the highest priority fix method
            ## Question: how can a report table look? Multiple attributes? Just one fix per attribute right? But multiple fixes per feature?
            for idx, row in object_gdf.iterrows():
                feature_dict = {
                    "id": idx,
                    "code": row["code"],
                    "fixes": {col.replace("fixes_", ""): row[col] for col in fix_cols},
                    "manual_inputs": {col.replace("manual_overwrite_", ""): row[col] for col in manual_cols},
                }
                results.append(feature_dict)

            execution_dict[layer_name] = [{}]
            list_features_to_remove = []  ## codes
            list_features_to_edit = []  ## codes
            highest_prios = []
            for result in results:
                fixes: dict = result["fixes"]
                fix_suggestions = fixes.values()
                fix_ids = []
                for fix in fix_suggestions:
                    if fix:
                        fix_ids.append(int(re.search(r"^[A-Za-z]{2}(\d+):", fix).group(1)))
                fix_ids.sort()
                highest_prio_fix = min(fix_ids)
                if result["code"] in [
                    "KDU-OH-5108",
                    "KDU-Q-8338",
                    "KDU-Q-2146",
                    "KDU-Q-1355",
                    "KDU-OH-4971",
                    "KDU-Q-8343",
                ]:
                    highest_prio_fix = 2
                highest_prios.append(highest_prio_fix)
                if highest_prio_fix == 1:
                    list_features_to_remove.append(result["code"])
                    execution_dict[layer_name][0]["fix_id"] = min(highest_prios)
                    execution_dict[layer_name][0]["inputs"] = list_features_to_remove
                if highest_prio_fix == 2:
                    list_features_to_edit.append(result["code"])
                    if not len(execution_dict[layer_name]) >= highest_prio_fix:
                        execution_dict[layer_name].append({})
                    execution_dict[layer_name][1]["fix_id"] = 2
                    execution_dict[layer_name][1]["inputs"] = list_features_to_edit

        ## Make an execution dict that executes fixes based on an order that prevents conflicts
        for layer_name, execution in execution_dict.items():
            hydamo_layer_gdf = gpd.read_file(self.hydamo_file_path, layer=layer_name)
            if execution[0]["fix_id"] == FIX_MAPPING.omit:
                gdf_hydamo_fixed = hydamo_fixes.omit_features(
                    gdf_HyDAMO=hydamo_layer_gdf,
                    layer=layer_name,
                    list_features=execution[0]["inputs"],
                    logger=self.logger,
                )
                # save layer report gdf to report gpkg
                gdf_hydamo_fixed.to_file(self.hydamo_fixed_file_path, layer=layer_name, driver="GPKG")
                self.logger.info(f"Finshed and saved report gdf for layer {layer_name} to {self.report_gpkg_path}")
            if len(execution) > 2 and execution[1]["fix_id"] == FIX_MAPPING.edit:
                pass
                ## do the edit logic
            if execution[0]["fix_id"] == FIX_MAPPING.multi_edit:
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
