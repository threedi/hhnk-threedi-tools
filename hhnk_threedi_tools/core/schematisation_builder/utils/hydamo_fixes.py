from pathlib import Path
from typing import Any

import fiona
import geopandas as gpd
import numpy as np
from hhnk_research_tools.logging import logging
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.datasets import normalize_fiona_schema, read_geopackage

from hhnk_threedi_tools.core.schematisation_builder.utils.summaries import ExtendedLayersSummary, ExtendedResultSummary


class ExtendedHyDAMO(HyDAMO):
    def __init__(self, hydamo_path: Path = None, results_path: Path = None, rules_objects: list = [], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hydamo_path = hydamo_path
        self.results_path = results_path

        self.post_process_datamodel(rules_objects)

    def post_process_datamodel(self, objects: list) -> None:
        """Post-process DataModel from self.validation_results."""
        self.validation_results: dict[str, gpd.GeoDataFrame] = {}
        self.validation_rules: dict[str, dict] = {}

        validation_results = ExtendedLayersSummary.from_geopackage(self.results_path)
        for hydamo_layer in self.layers:
            self.validation_results[hydamo_layer] = getattr(
                validation_results, 
                hydamo_layer
            )
            self.validation_rules[hydamo_layer] = next(
                (obj for obj in objects if obj["object"] == hydamo_layer), 
                {}
            )

        self._set_properties()

    def _set_properties(self):
        self.properties = {}
        if self.hydamo_path:
            layers = fiona.listlayers(self.hydamo_path)
            layers_dict = {}
            for layer in layers:
                with fiona.open(self.hydamo_path) as src:
                    schema = normalize_fiona_schema(src.schema)
                    layers_dict[layer] = schema

            self.properties[self.hydamo_path.name] = layers_dict
        return self.properties

    def _filter_status(self, gdf: gpd.GeoDataFrame, status_object: list[str]):
        if status_object is not None:
            if "statusobject" in gdf.columns:
                gdf = gdf.loc[np.where(gdf["statusobject"].isna() | gdf["statusobject"].isin(status_object))]
        return gdf

    def read_layer(self, layer, result_summary=ExtendedResultSummary(), status_object=None):
        """
        Read a layer from the datamodel.

        Parameters
        ----------
        layer : str
            Name of the layer (case sensitive!)
        result_summary : FixResultSummary
            A hydamo_fix FixResultSummary class where a possible exception
            will be appended to.
        status_object : List[str], optional
            A list of statusobject values used as a filter. The default is None.

        Raises
        ------
        e
            General exception while reading the layer from the geopackage.
        KeyError
            Specific exception; the layer is not part of the geopackage.

        Returns
        -------
        gdf : GeoDataFrame
            GeoDataFrame read from datasets (all columns are converted to lower case)
        schema : TYPE
            Fiona schema read from the layer
        """

        if not self.hydamo_path:
            raise ValueError(f"Path to geopackage unknown. This function only works if a geopackage is available.")

        if layer in self.layers:
            dataset = {k: v for k, v in self.properties.items() if layer in v.keys()}
            file_path = self.hydamo_path
            schema = list(dataset.values())[0][layer]
            try:
                gdf = read_geopackage(file_path, layer=layer)
                gdf = self._filter_status(gdf, status_object)
            except Exception as e:
                result_summary.append_warning(
                    (
                        f"Laag {layer} uit bestand {file_path.name} is geen "
                        "GeoPackage die wij kunnen openen. Vervang het bestand en "
                        "probeer opnieuw."
                    )
                )
                raise e

            # we will read all lower case
            schema["properties"] = {k.lower(): v for k, v in schema["properties"].items()}
            gdf.columns = [i.lower() for i in gdf.columns]
        else:
            raise KeyError(f"'{layer}' not in dataset-layers: '{self.layers}'")

        return gdf, schema

    @classmethod
    def from_geopackage(
        cls, 
        hydamo_path=None, 
        results_path=None, 
        rules_objects=None, 
        version="2.4", 
        ignored_layers=[], 
        check_columns=True,
        check_geotype=True
    ):
        """
        Initialize ExtendedHyDAMO class from GeoPackage

        Parameters
        ----------
        file_path : path-string
            Path-string to the hydamo GeoPackage
        check_columns : bool, optional
            Check if all required columns are present in the GeoDataFrame.
            The default is True.
        check_geotype : bool, optional
            Check if the geometry is of the required type. The default is True.

        Returns
        -------
        hydamo : ExtendedHyDAMO
            ExtendedHyDAMO object initialized with content of GeoPackage

        """
        if not hydamo_path:
            raise ValueError(f"No geopackage path is provided.")

        hydamo = cls(
            hydamo_path=hydamo_path, 
            results_path=results_path,
            rules_objects=rules_objects,  
            version=version, 
            ignored_layers=ignored_layers
        )
        for layer in fiona.listlayers(hydamo_path):
            if layer in hydamo.layers:
                hydamo_layer: HyDAMO = getattr(hydamo, layer)
                hydamo_layer.set_data(
                    gpd.read_file(hydamo_path, layer=layer),
                    check_columns=check_columns,
                    check_geotype=check_geotype,
                )
        return hydamo


# Example function
# TODO: list_features could also be summary validation/fix dataframe with codes to remove.
def omit_features(
    gdf_HyDAMO: gpd.GeoDataFrame, layer: str, list_features: list, logger: logging.Logger
) -> "gpd.GeoDataFrame":
    """Remove features from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer in which features need to be asingned as not usable.
        list_features (list): List of old feature IDs to remove.

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe with extra column 'is_usable' indicating features which arenot usable.
    """
    features_layer = gdf_HyDAMO
    if not features_layer.empty:
        try:
            if "is_usable" not in features_layer.columns:
                features_layer["is_usable"] = None
            # features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]

            # add column 'is_usable' to indicate features and set true for features in list_features
            features_layer_adjusted = features_layer.copy()
            features_layer_adjusted["is_usable"] = (
                True  ## FIXME: meer het idee om bij te houden welke features wel of niet gebruikt worden. is_usable is beter
            )
            features_layer_adjusted.loc[features_layer_adjusted["code"].isin(list_features), "is_usable"] = False

            features_layer.update(features_layer_adjusted)
            logger.info(f"Indicated {len(list_features)} features as not usable in layer {layer}.")
        except Exception as e:
            logger.error(f"Error indicating features as not usable in layer {layer}: {e}")
            pass
    else:
        logger.warning(f"No features found in layer {layer} which are not usable.")

    return features_layer


def edit_features(
    gdf_HyDAMO: gpd.GeoDataFrame, layer: str, attribute_name: str, value, logger: logging.Logger
) -> "gpd.GeoDataFrame":
    """Edits features from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer in which features need to be edited.
        attribute_name (str): The attribute that needs to be edited

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe with extra column 'is_usable' indicating features which arenot usable.
    """
    features_layer = gdf_HyDAMO
    if not features_layer.empty:
        try:
            if "is_usable" not in features_layer.columns:
                features_layer["is_usable"] = None
            # features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]

            # add column 'is_usable' to indicate features and set true for features in list_features
            features_layer_adjusted = features_layer.copy()
            features_layer_adjusted["is_usable"] = (
                True  ## FIXME: meer het idee om bij te houden welke features wel of niet gebruikt worden. is_usable is beter
            )
            features_layer_adjusted.loc[features_layer_adjusted["code"].isin(list_features), "is_usable"] = False

            features_layer.update(features_layer_adjusted)
            logger.info(f"Indicated {len(list_features)} features as not usable in layer {layer}.")
        except Exception as e:
            logger.error(f"Error indicating features as not usable in layer {layer}: {e}")
            pass
    else:
        logger.warning(f"No features found in layer {layer} which are not usable.")

    return features_layer


# functions to add
# change attributes based on info from other layers
# change attributes based on given assumption(s)
# change attributes based on DEM?
# %%
import numpy as np
from hydamo_validation import general_functions, logic_functions, topologic_functions
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.logical_validation import (
    _process_general_function,
    _process_logic_function,
    _process_topologic_function,
)
from shapely.geometry import LineString, Point, Polygon

GEOTYPE_MAPPING = {LineString: "LineString", Point: "Point", Polygon: "Polygon"}
SUMMARY_COLUMNS = [
    "valid",
    "invalid",
    "invalid_critical",
    "invalid_non_critical",
    "invalid_auto_fixable",
    "invalid_manual_fixable",
    "ignored",
    "summary",
    "tags_assigned",
    "tags_invalid",
]
LIST_SEPARATOR = ";"
NOTNA_COL_IGNORE = ["related_parameter"]
EXCEPTION_COL = "nen3610id"


def _process_omission():
    pass


def _process_assumption():
    pass


def _process_relation():
    pass


def _process_multi_layer_fix():
    pass


## use own categories


## minus: == general_rules.difference
def _iterator():
    pass


def _checker():
    pass

def _read_validation_rules(gdf: gpd.GeoDataFrame, validation_rules: list[dict], attribute: str):
    columns = gdf.columns
    related_attributes = []
    for rule in validation_rules:
        func = rule["function"]
        attributes = _check_atributes(func, attribute)
        related_attributes.extend(attributes)
    return related_attributes


def prepare(
        gdf: gpd.GeoDataFrame, 
        layer: str,
        schema, 
        validation_schema, 
        validation_result: gpd.GeoDataFrame,
        validation_rules: dict,
        keep_columns,   
        logger: logging.Logger,
        raise_error
    ):
    """
    Create validation and fix overview report
    Inputs:
        1. gdf: HyDAMO layer geodataframe
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
    ## Read the fix rules on attributes and make an overview of which validation rules are connected to this attribute
    ## There should be fix for every attribute that has a validation rule

    # create report gpkg with per layer a summary of validation and fix suggestions
    ## if validation_result.empty or not validation_rules: raise error
    logger.info(f"Start creating validation and fix summary for layer: {layer}")

    layer_name = layer
    rules = validation_rules
    validation_rules = rules.get("validation_rules", None)
    fix_rules = rules.get("fix_rules", None)

    if not rules:
        logger.info(f"Validation rules set not filled in for {layer_name}. Creating empty dataframe.")
        layer_report_gdf = gpd.GeoDataFrame(columns=keep_columns)
    else:
        layer_report_gdf = validation_result[
            ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
        ].copy()

    if not validation_rules or not fix_rules:
        logger.info(f"Quitting. Validation rules or fix rules for layer {layer_name} unknown.")
        return layer_report_gdf
        
    # remove rows where invalid columns are empty strings
    layer_report_gdf["invalid_critical"] = (
        layer_report_gdf["invalid_critical"].fillna("").astype(str)
        + layer_report_gdf["invalid_critical"].apply(lambda x: "; " if x else "")
        + layer_report_gdf["ignored"].fillna("").astype(str)
    )
    layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
    layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)

    if layer_report_gdf.empty:
        logger.info(
            f"No invalid features found in layer {layer_name}, fixing is not needed/finished for this layer."
        )
        return layer_report_gdf

    logger.info(f"Created base report gdf with {len(layer_report_gdf)} objects which need fixes")

    # add layer specific columns based on fix config
    add_specific_columns = []
    for fix in fix_rules:
        attribute_name = fix["attribute_name"]
        # other_attributes = _read_validation_rules(validation_rules, attribute_name)

        # attribute_name_test = read_attributes() ## for an object, read the 
        ## think about if we can read the attribute name and deduce the validation ids from it. 
        ## every validation_rule_id should be in the 
        ## so, for instance a rule is dependent on slope. slope is depenedent on delta_h and lengte
        ## lengte is an attribute, delta_h is a general_rule variable dependent on hoogtebinnenonderkantbov and hoogtebinnenonderkantbene.
        ## So: the slope fix should be based on lengte, hoogtebinnenonderkantbov and hoogtebinnenonderkantbene
        ## another rule, verval is also dependent on hoogtebinnenonderkantbov and hoogtebinnenonderkantbene
        ## so: lengte, hoogtebinnenonderkantbov and hoogtebinnenonderkantbene should be next to each other, and next to them should be the fix overview of the slope and verval fix rules
        

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
    logger.info(f"Added specific columns to report gdf for following attributes: {add_specific_columns}")

    # fill in validation and fix information
    logger.info(f"Filling in validation, fix and attribute information for layer: {layer_name}")
    list_attributes_filled = []
    for index, row in layer_report_gdf.iterrows():
        # connect IDs of invalid_critical to validation_id in fix config
        if row["invalid_critical"] is not None or row["invalid_non_critical"] is not None:
            invalid_ids = []
            if row["invalid_critical"] is not None:
                invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
            if row["invalid_non_critical"] is not None:
                invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

            for attribute_fix in fix_rules:
                validation_ids = attribute_fix["validation_ids"]
                attribute_name = attribute_fix["attribute_name"]
                fix_method = attribute_fix["fix_method"]
                fix_id = attribute_fix["fix_id"]
                fix_description = attribute_fix["fix_description"]

                if any(validation_id in invalid_ids for validation_id in validation_ids):
                    # mark attribute as filled
                    if attribute_name not in list_attributes_filled:
                        list_attributes_filled.append(attribute_name)

                    # Loop through all validation ids of attribute fix which are present in invalid ids'
                    for validation_id in validation_ids:
                        if validation_id in invalid_ids:
                            # based on validation_rules.json, check error type and message
                            for rule in validation_rules:
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
                    if attribute_name in gdf.columns and attribute_name != "geometry":
                        code = row["code"]
                        if fix_id == 2 and "equal_to" in list(fix_method.keys()):
                            hydamo_value = [fix_method["equal_to"]]
                        else:
                            hydamo_value = gdf.loc[
                                gdf["code"] == code, attribute_name
                            ].values
                        if len(hydamo_value) > 0:
                            layer_report_gdf.loc[index, attribute_name] = hydamo_value[0]
                        else:
                            logger.warning(
                                f"Could not find attribute value for code {code} and attribute {attribute_name}"
                            )

    logger.info(
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
    logger.info(f"Finshed report gdf for layer {layer_name}")

    return layer_report_gdf


def execute(
    datamodel,
    validation_rules_sets,
    layers_summary,
    result_summary,
    logger=None,
    raise_error=False,
):
    """Execute the logical validation."""

    return datamodel, layers_summary, result_summary

    object_rules_sets = [
        i
        for i in validation_rules_sets["objects"]
        if i["object"] in datamodel.data_layers
    ]
    logger.info(
        rf"lagen met valide objecten en regels: {[i['object'] for i in object_rules_sets]}"
    )
    for object_rules in object_rules_sets:
        col_translation: dict = {}

        object_layer = object_rules["object"]
        logger.info(f"{object_layer}: start")
        object_gdf = getattr(datamodel, object_layer).copy()

        # add summary columns
        object_gdf["rating"] = 10
        for col in SUMMARY_COLUMNS:
            object_gdf[col] = ""

        # general rule section
        if "fix_rules" in object_rules.keys():
            ## sort based on hierarchy key that the user can set in fix_overview.gpkg?
            ## apply omissions
            ## then do the other fixes and filter for is_usable
            ## gdf_add_summary / history function
            pass

        if "general_rules" in object_rules.keys():
            general_rules = object_rules["general_rules"]
            general_rules_sorted = sorted(general_rules, key=lambda k: k["id"])
            for rule in general_rules_sorted:
                logger.info(
                    f"{object_layer}: uitvoeren general-rule met id {rule['id']}"
                )
                try:
                    result_variable = rule["result_variable"]
                    result_variable_name = (
                        f"general_{rule['id']:03d}_{rule['result_variable']}"
                    )

                    # get function
                    function = next(iter(rule["function"]))
                    input_variables = rule["function"][function]

                    # remove all nan indices
                    indices = _notna_indices(object_gdf, input_variables)
                    dropped_indices = [
                        i
                        for i in object_gdf.index[object_gdf.index.notna()]
                        if i not in indices
                    ]

                    # add object_relation
                    if "related_object" in input_variables.keys():
                        input_variables = _add_related_gdf(
                            input_variables, datamodel, object_layer
                        )
                    elif "custom_function_name" in input_variables.keys():
                        input_variables["hydamo"] = datamodel
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, datamodel)

                    if dropped_indices:
                        result_summary.append_warning(
                            _nan_message(
                                len(dropped_indices),
                                object_layer,
                                rule["id"],
                                "general_rule",
                            )
                        )
                    if object_gdf.loc[indices].empty:
                        object_gdf[result_variable] = np.nan
                    else:
                        result = _process_general_function(
                            object_gdf.loc[indices], function, input_variables
                        )
                        object_gdf.loc[indices, result_variable] = result

                        getattr(datamodel, object_layer).loc[
                            indices, result_variable
                        ] = result

                    col_translation = {
                        **col_translation,
                        result_variable: result_variable_name,
                    }
                except Exception as e:
                    logger.error(
                        f"{object_layer}: general_rule {rule['id']} crashed width Exception {e}"
                    )
                    result_summary.append_error(
                        (
                            "general_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                            f"(object: '{object_layer}', id: '{rule['id']}', function: '{function}', "
                            f"input_variables: {input_variables}, Reason (Exception): {e})"
                        )
                    )
                    if raise_error:
                        raise e
                    else:
                        pass

        validation_rules = object_rules["validation_rules"]
        validation_rules = [
            i for i in validation_rules if ("active" not in i.keys()) | i["active"]
        ]
        validation_rules_sorted = sorted(validation_rules, key=lambda k: k["id"])
        # validation rules section
        for rule in validation_rules_sorted:
            try:
                rule_id = rule["id"]
                logger.info(
                    f"{object_layer}: uitvoeren validatieregel met id {rule_id} ({rule['name']})"
                )
                result_variable = rule["result_variable"]
                if "exceptions" in rule.keys():
                    exceptions = rule["exceptions"]
                    indices = object_gdf.loc[
                        ~object_gdf[EXCEPTION_COL].isin(exceptions)
                    ].index
                else:
                    indices = object_gdf.index
                    exceptions = []
                result_variable_name = (
                    f"validate_{rule_id:03d}_{rule['result_variable']}"
                )

                # get function
                function = next(iter(rule["function"]))
                input_variables = rule["function"][function]

                # remove all nan indices
                notna_indices = _notna_indices(object_gdf, input_variables)
                indices = [i for i in indices[indices.notna()] if i in notna_indices]

                # add object_relation
                if "join_object" in input_variables.keys():
                    input_variables = _add_join_gdf(input_variables, datamodel)

                # apply filter on indices
                if "filter" in rule.keys():
                    filter_function = next(iter(rule["filter"]))
                    filter_input_variables = rule["filter"][filter_function]
                    series = _process_logic_function(
                        object_gdf, filter_function, filter_input_variables
                    )
                    series = series[series.index.notna()]
                    filter_indices = series.loc[series].index.to_list()
                    indices = [i for i in filter_indices if i in indices]
                else:
                    filter_indices = []

                if object_gdf.loc[indices].empty:
                    object_gdf[result_variable] = None
                elif rule["type"] == "logic":
                    object_gdf.loc[indices, (result_variable)] = (
                        _process_logic_function(
                            object_gdf.loc[indices], function, input_variables
                        )
                    )
                elif (rule["type"] == "topologic") and (
                    hasattr(datamodel, "hydroobject")
                ):
                    result_series = _process_topologic_function(
                        # getattr(
                        #     datamodel, object_layer
                        # ),  # FIXME: commented as we need to apply filter in topologic functions as well. Remove after tests pass
                        object_gdf,
                        datamodel,
                        function,
                        input_variables,
                    )
                    object_gdf.loc[indices, (result_variable)] = result_series.loc[
                        indices
                    ]

                col_translation = {
                    **col_translation,
                    result_variable: result_variable_name,
                }

                # summarize
                if rule["error_type"] == "critical":
                    penalty = 5
                    critical = True
                else:
                    penalty = 1
                    critical = False
                if "penalty" in rule.keys():
                    penalty = rule["penalty"]

                error_message = rule["error_message"]

                if "tags" in rule.keys():
                    tags = LIST_SEPARATOR.join(rule["tags"])
                else:
                    tags = None

                auto_fixable = rule.get("auto_fixable", False)

                exceptions += filter_indices
                _valid_indices = object_gdf[~object_gdf.index.isna()].index
                tags_indices = [i for i in _valid_indices if i not in exceptions]
                object_gdf = gdf_add_summary(
                    gdf=object_gdf,
                    variable=result_variable,
                    rule_id=rule_id,
                    penalty=penalty,
                    error_message=error_message,
                    critical=critical,
                    tags=tags,
                    tags_indices=tags_indices,
                    auto_fixable=auto_fixable,
                )

            except Exception as e:
                logger.error(
                    f"{object_layer}: validation_rule {rule['id']} width Exception {e}"
                )
                result_summary.append_error(
                    (
                        "validation_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                        f"(object '{object_layer}', rule_id '{rule['id']}', function: '{function}', "
                        f"input_variables: {input_variables}, Reason (Exception): {e})"
                    )
                )
                if raise_error:
                    raise e
                else:
                    pass

        # drop columns
        drop_columns = [
            i
            for i in object_gdf.columns
            if i
            not in list(col_translation.keys())
            + ["nen3610id", "geometry", "rating"]
            + SUMMARY_COLUMNS
        ]
        object_gdf.drop(columns=drop_columns, inplace=True)
        # re_order columns
        column_order = ["nen3610id"]
        column_order += list(col_translation.keys())
        column_order += ["rating"] + SUMMARY_COLUMNS
        if "geometry" in object_gdf.columns:
            column_order += ["geometry"]
        object_gdf = object_gdf[column_order]

        # finish result columns
        for i in SUMMARY_COLUMNS:
            if i in object_gdf.columns:
                object_gdf.loc[:, i] = object_gdf[i].map(lambda x: str(x)[:-1])
        if "rating" in object_gdf.columns:
            object_gdf.loc[:, "rating"] = np.maximum(1, object_gdf["rating"])
        for i in ["tags_assigned", "tags_invalid"]:
            if i in object_gdf.columns:
                object_gdf.loc[:, i] = object_gdf[i].map(
                    lambda x: ";".join(list(set(str(x).split(LIST_SEPARATOR))))
                )

        # rename columns
        object_gdf.rename(columns=col_translation, inplace=True)

        # join gdf to layer_summary
        layers_summary.join_gdf(object_gdf, object_layer)

        if gdf.empty:
            logger.warning(
                f"{layer}: geen valide objecten na syntax-validatie. Inspecteer 'syntax_oordeel' in de resultaten; deze is false voor alle objecten. De laag zal genegeerd worden in de (topo)logische validatie."
            )
        else:
            datamodel.set_data(gdf, layer, index_col=None)

    return datamodel, layers_summary, result_summary


class FixOveriew:
    pass


class ResultsOverview:
    pass

# %%
