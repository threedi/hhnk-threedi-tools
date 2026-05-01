from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from core.schematisation_builder.fixer.mapping import _get_validation_ids_for_attribute
from core.schematisation_builder.fixer.summaries import ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary
from hhnk_research_tools.logging import logging
from hydamo_validation import logical_validation
from hydamo_validation.logical_validation import (
    _add_join_gdf,
    _add_related_gdf,
    _nan_message,
    _notna_indices,
    _process_general_function,
    _process_logic_function,
    _process_topologic_function,
)
from shapely.geometry import LineString, Point, Polygon

GEOTYPE_MAPPING = {LineString: "LineString", Point: "Point", Polygon: "Polygon"}
SUMMARY_COLUMNS = [
    "is_usable",
    "fix_history",
]
KEEP_COLUMNS = ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
LIST_SEPARATOR = ";"
NOTNA_COL_IGNORE = ["related_parameter"]
EXCEPTION_COL = "nen3610id"
INVALID_COLUMNS = ["invalid_critical", "invalid_non_critical", "ignored"]


@dataclass(frozen=True)
class FixColumns:
    """
    Utiity dataclass that standardizes the naming conventions for fix‑related
    columns added during the review and fix‑execution phases.

    Parameters
    ----------
    attribute_name : str
        Name of the attribute/column that a fix rule applies to.

    Notes
    -----
    Each property returns the corresponding column name used in the fix review
    GeoDataFrame. This ensures naming consistency across all fix operations.
    """

    attribute_name: str

    @property
    def validation_summary(self):
        return f"val_errors_{self.attribute_name}"

    @property
    def fix_suggestion(self):
        return f"fixes_{self.attribute_name}"

    @property
    def fix_checks(self):
        return f"fix_checks_{self.attribute_name}"

    @property
    def manual_overwrite(self):
        return f"manual_overwrite_{self.attribute_name}"


def _add_custom_kwargs(input_variables: dict, datamodel):
    input_variables["hydamo"] = datamodel
    kwargs = {k: v for k, v in input_variables.items() if k not in ["custom_function_name", "hydamo", "kwargs"]}
    for kwarg in list(kwargs.keys()):
        input_variables.pop(kwarg, None)
    input_variables["kwargs"] = kwargs

    return input_variables


def pre_run_logic(gdf: gpd.GeoDataFrame, input_variables: dict):
    logic = input_variables["kwargs"]["logic"]
    function = next(iter(logic))
    inputs = logic[function]
    result = _process_logic_function(gdf, function, inputs)
    input_variables["kwargs"]["logic"] = result
    return input_variables


def _run_true_false(gdf: gpd.GeoDataFrame, input_variables: dict):
    true = input_variables["kwargs"]["true"]
    function_true = next(iter(true))
    inputs_true = true[function_true]
    result_true = _process_general_function(gdf, function_true, inputs_true)
    input_variables["kwargs"]["true"] = result_true

    false = input_variables["kwargs"]["false"]
    function_false = next(iter(false))
    inputs_false = false[function_false]
    result_false = _process_general_function(gdf, function_false, inputs_false)
    input_variables["kwargs"]["false"] = result_false

    return input_variables


def _iterate_by_rounds(data: dict, execution_dict: dict[str, int]):
    """
    Yields (round_num, key, value) from `data` in execution round order.
    """
    rounds = sorted(set(execution_dict.values()))

    for round_num in rounds:
        keys_in_round = [k for k, r in execution_dict.items() if r == round_num]
        for key in keys_in_round:
            yield round_num, key, data[key]


def _iterate_by_steps(fix_rules: list[dict], fix_iterations: dict[str, dict[int, list[int]]]):
    """
    Yields (step, rule) from `fix_rules` ordered by fix_iterations.

    Iterates iteration keys in ascending order. Within each key, fix_ids are
    yielded in ascending order. The step label is the iteration key if the
    group contains one item, or '{iteration_num}.{pos}' (1-based) if multiple.

    Args:
        fix_rules: List of fix rule dicts, each containing at least 'fix_id'.
        fix_iterations: Full nested dict mapping layer -> {iteration_num: [fix_ids]}.
            e.g. {'duikersifonhevel': {1: [], 2: [10], 3: [11, 12]}}
    """
    fix_id_to_rule: dict[int, dict] = {rule["fix_id"]: rule for rule in fix_rules}
    fix_ids_in_rules: set[int] = set(fix_id_to_rule.keys())

    # Find the layer sub-dict whose fix_ids overlap with the provided fix_rules
    layer_iterations: dict[int, list[int]] = {}
    for layer_dict in fix_iterations.values():
        all_fids = {fid for fids in layer_dict.values() for fid in fids}
        if fix_ids_in_rules & all_fids:
            layer_iterations = layer_dict
            break

    for iteration_num in sorted(layer_iterations.keys()):
        fix_ids = layer_iterations[iteration_num]
        multiple = len(fix_ids) > 1
        for pos, fix_id in enumerate(fix_ids, start=1):
            rule = fix_id_to_rule.get(fix_id)
            if rule is None:
                continue
            step = f"{iteration_num}.{pos}" if multiple else str(iteration_num)
            yield step, rule


def _invalid_indices(
    gdf: gpd.GeoDataFrame, validation_result: gpd.GeoDataFrame, validation_ids: list[int]
) -> list[int]:
    """
    Identify all row indices where any of the given validation rule IDs appear
    in one of the invalid columns ("invalid_critical", "invalid_non_critical",
    "ignored") of the validation results.

    Parameters
    ----------
    gdf : GeoDataFrame
        The original object-layer data.
    validation_result : GeoDataFrame
        Validation output with aggregated invalid rule IDs stored as ';'
        separated strings.
    validation_ids : list[int]
        Validation rule IDs to check for.

    Returns
    -------
    list[int]
        List of indices where one or more of the given rule IDs appear in
        the invalid columns.
    """

    vids = [str(vid) for vid in validation_ids]
    mask = False
    for col in INVALID_COLUMNS:
        invalid_ids = validation_result[col].str.split(";")
        col_mask = invalid_ids.apply(lambda lst: any(iid == vid for iid in lst for vid in vids))
        mask |= col_mask
    invalid_indices = validation_result[mask].index.tolist()
    return [i for i in invalid_indices if i in gdf.index]


def _manual_indices(gdf: gpd.GeoDataFrame, review_gdf: gpd.GeoDataFrame, manual_column: str):
    """
    Identify all row indices of the hydamo gdf where a value has been filled in the manual_overwrite column of the layer.

    Parameters
    ----------
    layers_summary : ExtendedLayersSummary
        A container object holding reviewer layers, accessible as attributes.
        Must contain `layer` as an attribute storing a GeoDataFrame.
    gdf : GeoDataFrame
        The main object-layer GeoDataFrame containing a 'code' column.
    layer : str
        Name of the reviewer layer inside `layers_summary` to read from.

    Returns
    -------
    list[int]
        A list of integer indices from `gdf` corresponding to features whose
        'code' is listed in the specified manual review column.
    """

    # Validate required columns
    if manual_column not in review_gdf.columns or "code" not in review_gdf.columns:
        return []
    if "code" not in gdf.columns:
        return []

    # Extract manual codes present in the reviewer layer
    manual_gdf = review_gdf[["code", manual_column]].dropna(subset=[manual_column])
    manual_codes = manual_gdf["code"]

    # Match codes back to the original gdf
    gdf_indices = gdf[gdf["code"].isin(manual_codes)].index.to_list()
    review_indices = [i for i in manual_gdf.index.to_list() if i in gdf.index]

    # Sanity-filter to ensure indices belong to gdf
    return gdf_indices, review_indices


def _apply_manual_overwrites(
    object_gdf: gpd.GeoDataFrame,
    layers_summary: ExtendedLayersSummary,
    object_layer: str,
    attribute_name: str,
    logger: logging.Logger,
) -> gpd.GeoDataFrame:
    """
    Apply manual overwrite values from the review layer onto `object_gdf`.

    If the layer is not present in `layers_summary`, or no manual values are
    found, `object_gdf` is returned unchanged.

    Parameters
    ----------
    object_gdf : GeoDataFrame
        The layer GeoDataFrame to update in-place.
    layers_summary : ExtendedLayersSummary
        Summary container holding review GeoDataFrames.
    object_layer : str
        Name of the layer to look up in `layers_summary`.
    attribute_name : str
        Attribute column to overwrite.
    logger : logging.Logger
        Logger instance.

    Returns
    -------
    GeoDataFrame
        `object_gdf` with manual overwrite values applied.
    """
    if object_layer not in layers_summary.data_layers:
        return object_gdf

    review_gdf: gpd.GeoDataFrame = getattr(layers_summary, object_layer)
    manual_column = FixColumns(attribute_name).manual_overwrite
    object_indices, review_indices = _manual_indices(object_gdf, review_gdf, manual_column)
    if len(object_indices) != len(review_indices):
        logger.warning("Length of object_indices not equal to length of review_indices")
    if not object_indices:
        return object_gdf

    manual_gdf = review_gdf.loc[review_indices, manual_column]
    manual_dtype = object_gdf.loc[object_indices, attribute_name].dtypes
    if manual_dtype == "float64":
        manual_gdf = manual_gdf.astype(float)
    elif manual_dtype == "int64":
        manual_gdf = manual_gdf.astype(int)
    elif manual_dtype == "bool":
        manual_gdf = manual_gdf.astype(bool)
    elif manual_dtype == "object":
        manual_gdf = manual_gdf.astype(str)
    object_gdf.loc[object_indices, attribute_name] = manual_gdf
    return object_gdf


def _apply_general_rules(
    gdf: gpd.GeoDataFrame,
    layer: str,
    rules: dict,
    overwrite: bool,
    datamodel: ExtendedHyDAMO,
    logger: logging.Logger,
    result_summary: ExtendedResultSummary,
    raise_error: bool,
):
    """
    Apply general rules (derivations) from `rules` to `gdf` in-place.

    If `overwrite` is False, rules whose result_variable already exists as a
    column of `gdf` are skipped. If `overwrite` is True, the existing column
    is dropped before the rule is applied so it is fully recomputed.
    """
    if "general_rules" not in rules:
        return gdf

    general_rules_sorted = sorted(rules["general_rules"], key=lambda k: k["id"])
    for rule in general_rules_sorted:
        logger.info(f"{layer}: uitvoeren general-rule met id {rule['id']}")
        try:
            result_variable = rule["result_variable"]

            if result_variable in gdf.columns:
                if not overwrite:
                    logger.info(
                        f"{layer}: skipping general-rule {rule['id']} "
                        f"('{result_variable}' already exists, overwrite=False)"
                    )
                    continue
                # overwrite=True: drop the column so it is cleanly recomputed
                gdf.drop(columns=[result_variable], inplace=True)

            # get function
            function = next(iter(rule["function"]))
            input_variables = rule["function"][function]

            # remove all nan indices
            indices = _notna_indices(gdf, input_variables)
            dropped_indices = [i for i in gdf.index[gdf.index.notna()] if i not in indices]

            # add object_relation
            if "related_object" in input_variables:
                input_variables = _add_related_gdf(input_variables, datamodel, layer)
            elif "custom_function_name" in input_variables:
                input_variables = _add_custom_kwargs(input_variables, datamodel)
            elif "join_object" in input_variables:
                input_variables = _add_join_gdf(input_variables, datamodel)

            if dropped_indices:
                result_summary.append_warning(_nan_message(len(dropped_indices), layer, rule["id"], "general_rule"))

            if gdf.loc[indices].empty:
                gdf[result_variable] = np.nan
            else:
                result = _process_general_function(gdf.loc[indices], function, input_variables)
                gdf.loc[indices, result_variable] = result
                getattr(datamodel, layer).loc[indices, result_variable] = result

        except Exception as e:
            logger.error(f"{layer}: general_rule {rule['id']} crashed with Exception {e}")
            result_summary.append_error(
                "general_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                f"(object: '{layer}', id: '{rule['id']}', function: '{function}', "
                f"input_variables: {input_variables}, Reason (Exception): {e})"
            )
            if raise_error:
                raise e

    return gdf


def review(
    datamodel: ExtendedHyDAMO,
    layers_summary: ExtendedLayersSummary,
    result_summary: ExtendedResultSummary,
    logger: logging.Logger,
    raise_error: bool,
) -> Tuple[ExtendedLayersSummary, ExtendedResultSummary]:
    """
    Populate a review DataFrame for each layer, including:
    - validation summaries
    - fix suggestions
    - fix checks
    - manual override fields

    This function *does not* modify the underlying datamodel. Instead, it
    generates annotated review layers that guide a human reviewer in preparing
    corrections.

    Workflow
    --------
    1. Load validation rules and validation results for each layer.
    2. Construct a review GeoDataFrame containing:
       - KEEP_COLUMNS from validation results
       - fix columns generated by `FixColumns`
       - aggregated error summaries
       - fix-check results per rule
    3. Write review results into `layers_summary`.

    Parameters
    ----------
    datamodel : ExtendedHyDAMO
        Datamodel after validation.
    layers_summary : ExtendedLayersSummary
        Summary container used to collect review-layer outputs.
    result_summary : ExtendedResultSummary
        Object accumulating warnings/errors during processing.
    logger : logging.Logger
        Logger for status and diagnostic messages.
    raise_error : bool
        If True, rethrow exceptions; otherwise, record errors and continue.

    Returns
    -------
    (ExtendedLayersSummary, ExtendedResultSummary)
        Updated summary objects containing review-layer data.
    """

    logger.info(rf"Start review")
    new_datamodel = datamodel

    ## create an updated datamodel based on datamodel post processing information
    object_rules_sets = deepcopy(new_datamodel.validation_rules)
    validation_results = deepcopy(new_datamodel.validation_results)
    validation_ids = deepcopy(new_datamodel.validation_ids)
    validation_iterations = deepcopy(new_datamodel.validation_iterations)
    fix_iterations = deepcopy(new_datamodel.fix_iterations)

    logger.info(
        rf"lagen met valide objecten en regels: {[i for i in list(object_rules_sets.keys())]}"
    )  ## add check to tell which objects have fixes
    for round, object_layer, object_rules in _iterate_by_rounds(object_rules_sets, validation_iterations):
        logger.info(f"Round {round}: review fix for {object_layer}")
        object_gdf, object_schema = new_datamodel.read_layer(
            object_layer, result_summary
        )  ## maybe use is_usable to deselect rows that are not getting fixed?
        result_gdf = validation_results[object_layer]

        if not all([col in result_gdf.columns for col in KEEP_COLUMNS]):
            logger.info(
                f"Validation did not result run properly. Some the following columns not available: {KEEP_COLUMNS}"
            )
            continue
        review_gdf = result_gdf[KEEP_COLUMNS].copy()
        review_gdf["invalid_critical"] = (
            review_gdf["invalid_critical"].fillna("").astype(str)
            + review_gdf["invalid_critical"].apply(lambda x: ";" if x else "")
            + review_gdf["ignored"].fillna("").astype(str)
        )
        review_gdf["invalid_critical"] = review_gdf["invalid_critical"].replace("", None)
        review_gdf["invalid_non_critical"] = review_gdf["invalid_non_critical"].replace("", None)
        review_gdf = review_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")
        review_gdf["invalid_critical"] = review_gdf["invalid_critical"].fillna("").astype(str)
        review_gdf["invalid_non_critical"] = review_gdf["invalid_non_critical"].fillna("").astype(str)

        # add fix columns
        seen = []
        for col in [
            rule["attribute_name"] for rule in object_rules.get("fix_rules", [{}]) if "attribute_name" in rule
        ]:
            if col == "geometry" or col in seen:
                continue
            seen.append(col)
            fix_columns = FixColumns(col)
            review_gdf[col] = object_gdf.loc[review_gdf.index, col]
            review_gdf[fix_columns.validation_summary] = ""
            review_gdf[fix_columns.fix_suggestion] = ""
            review_gdf[fix_columns.fix_checks] = ""
            review_gdf[fix_columns.manual_overwrite] = None
        # add summary columns
        for col in SUMMARY_COLUMNS:
            review_gdf[col] = ""

        logger.info(f"Added specific columns to report gdf for following attributes: {seen + SUMMARY_COLUMNS}")

        # fix rule section
        if "fix_rules" in object_rules.keys():
            fix_rules = object_rules["fix_rules"]
            for step, rule in _iterate_by_steps(fix_rules, fix_iterations):
                logger.info(f"Step {step}: uitvoeren van fix-rule {rule['fix_id']} ({object_layer})")
                try:
                    attribute_name = rule["attribute_name"]
                    attribute_validation_ids = validation_ids[object_layer][attribute_name]
                    function = next(iter(rule["fix_method"]))
                    description = rule["fix_description"]
                    input_variables = rule["fix_method"][function]
                    logger.info(input_variables)

                    indices = _invalid_indices(object_gdf, review_gdf, attribute_validation_ids)
                    fix_columns = FixColumns(attribute_name)
                    validation_rules = [
                        vrule
                        for i in attribute_validation_ids
                        for vrule in object_rules["validation_rules"]
                        if vrule["id"] == i
                    ]

                    # apply filter on indices
                    if "filter" in rule.keys():
                        filter_function = next(iter(rule["filter"]))
                        filter_input_variables = rule["filter"][filter_function]
                        series = _process_logic_function(object_gdf, filter_function, filter_input_variables)
                        series = series[series.index.notna()]
                        filter_indices = series.loc[series].index.to_list()
                        indices = [i for i in filter_indices if i in indices]
                    else:
                        filter_indices = []

                    # add object_relation
                    if "related_object" in input_variables.keys():
                        input_variables = _add_related_gdf(input_variables, new_datamodel, object_layer)
                    elif "custom_function_name" in input_variables.keys():
                        input_variables = _add_custom_kwargs(input_variables, new_datamodel)
                        if input_variables["custom_function_name"] == "if_else":
                            input_variables = pre_run_logic(object_gdf, input_variables)
                            input_variables = _run_true_false(object_gdf, input_variables)
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, new_datamodel)

                    # fill fix columns
                    logger.info(
                        f"{object_layer}: invullen fix kolommen voor {attribute_name} met validatie regels {attribute_validation_ids})"
                    )
                    review_gdf.loc[indices, fix_columns.fix_suggestion] = description

                    # apply manual overwrites
                    if object_layer in layers_summary.data_layers:
                        review_gdf[fix_columns.manual_overwrite] = getattr(layers_summary, object_layer)[
                            fix_columns.manual_overwrite
                        ]

                    for _rule in validation_rules:
                        _rule_id: int = _rule["id"]
                        _error_type: str = _rule["error_type"]
                        _error_message: str = _rule["error_message"]
                        _error_prefix = "C" if _error_type == "critical" else "W"
                        _indices = _invalid_indices(object_gdf, review_gdf, [_rule_id])
                        _function = next(iter(_rule["function"]))
                        _input_variables = _rule["function"][_function]

                        # add object_relation
                        if "join_object" in _input_variables.keys():
                            _input_variables = _add_join_gdf(_input_variables, new_datamodel)

                        # apply filter on indices
                        if "filter" in _rule.keys():
                            filter_function = next(iter(_rule["filter"]))
                            filter_input_variables = _rule["filter"][filter_function]
                            series = _process_logic_function(object_gdf, filter_function, filter_input_variables)
                            series = series[series.index.notna()]
                            filter_indices = series.loc[series].index.to_list()
                            _indices = [i for i in filter_indices if i in _indices]
                        else:
                            filter_indices = []

                        # fill in validation_summary column
                        validation_summary = f"{_error_prefix}{_rule_id}:{_error_message}"
                        review_gdf.loc[_indices, fix_columns.validation_summary] = np.where(
                            review_gdf.loc[_indices, fix_columns.validation_summary] == "",
                            validation_summary,
                            review_gdf.loc[_indices, fix_columns.validation_summary] + ";" + validation_summary,
                        )

                        # fill in fix checks column
                        if object_gdf.loc[_indices].empty:
                            check = None
                        elif _rule["type"] == "logic":
                            check = _process_logic_function(object_gdf.loc[_indices], _function, _input_variables)
                        elif (_rule["type"] == "topologic") and (hasattr(new_datamodel, "hydroobject")):
                            result_series = _process_topologic_function(
                                object_gdf,
                                new_datamodel,
                                _function,
                                _input_variables,
                            )
                            check = result_series.loc[_indices]

                        if isinstance(check, pd.Series):
                            fix_check = check.map(
                                lambda _check: f"{_rule_id}:Valid" if _check else f"{_rule_id}:Invalid"
                            ).astype(str)
                        elif isinstance(check, bool):
                            fix_check = f"{_rule_id}:Valid" if check else f"{_rule_id}:Invalid"
                        elif check is None:
                            fix_check = ""
                        else:
                            fix_check = f"{_rule_id}:Unknown"

                        review_gdf.loc[_indices, fix_columns.fix_checks] = np.where(
                            review_gdf.loc[_indices, fix_columns.fix_checks] == "",
                            fix_check,
                            review_gdf.loc[_indices, fix_columns.fix_checks] + ";" + fix_check,
                        )

                except Exception as e:
                    logger.error(f"{object_layer}: validation_rule {rule['fix_id']} width Exception {e}")
                    result_summary.append_error(
                        (
                            "validation_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                            f"(object '{object_layer}', rule_id '{rule['fix_id']}', function: '{function}', "
                            f"input_variables: {input_variables}, Reason (Exception): {e})"
                        )
                    )
                    if raise_error:
                        raise e
                    else:
                        pass

        # set review_gdf to layer_summary
        layers_summary.set_data(review_gdf, object_layer, object_schema["geometry"])

    return layers_summary, result_summary


def execute(
    datamodel: ExtendedHyDAMO,
    layers_summary: ExtendedLayersSummary,
    result_summary: ExtendedResultSummary,
    logger: logging.Logger,
    raise_error: bool,
) -> Tuple[ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary]:
    """
    Apply fix-rules and optionally general-rules to the datamodel.

    This function performs the actual mutation of the HyDAMO layers by writing
    corrected attribute values back into the GeoDataFrames.

    Two main sections are executed:
    1. General rules (optional, but typically executed after fix rules to ensure all derivations are up to date)
    2. Fix rules (always executed if available)

    Fix-rule execution steps:
    -------------------------
    - Identify invalid rows based on rule.validation_ids
    - Optionally apply rule-level filters
    - Resolve related-object or join-object dependencies
    - Execute fix function and write results into the datamodel

    Parameters
    ----------
    datamodel : ExtendedHyDAMO
        Datamodel that will be modified in-place based on fix rules.
    layers_summary : ExtendedLayersSummary
        Summary of review layers (optional for overwrite functionality).
    result_summary : ExtendedResultSummary
        Accumulates warnings, errors, and final statistics.
    logger : logging.Logger
        Logger instance for tracing workflow progress.
    raise_error : bool
        If True, exceptions halt execution; otherwise, errors are logged and
        processing continues.

    Returns
    -------
    (ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary)
        Updated datamodel and summaries after fix application.
    """

    new_datamodel = datamodel
    ## create an updated datamodel based on datamodel post processing information
    object_rules_sets = deepcopy(new_datamodel.validation_rules)
    validation_results = deepcopy(new_datamodel.validation_results)
    validation_ids = deepcopy(new_datamodel.validation_ids)
    validation_iterations = deepcopy(new_datamodel.validation_iterations)
    fix_iterations = deepcopy(new_datamodel.fix_iterations)

    logger.info(
        rf"lagen met valide objecten en regels: {[i for i in list(object_rules_sets.keys())]}"
    )  ## add check to tell which objects have fixes
    for round, object_layer, object_rules in _iterate_by_rounds(object_rules_sets, validation_iterations):
        logger.info(f"Round {round}: start fix for {object_layer}")
        object_gdf: gpd.GeoDataFrame = getattr(
            new_datamodel, object_layer
        ).copy()  ## check if the copy is redundant for our purpose
        object_validation_result = validation_results[object_layer]
        # add summary columns
        for col in SUMMARY_COLUMNS:
            object_gdf[col] = ""

        # general rule section
        object_gdf = _apply_general_rules(
            gdf=object_gdf,
            layer=object_layer,
            rules=object_rules,
            overwrite=False,
            datamodel=new_datamodel,
            logger=logger,
            result_summary=result_summary,
            raise_error=raise_error,
        )

        # fix rule section
        if "fix_rules" in object_rules.keys():
            fix_rules = object_rules["fix_rules"]
            for step, rule in _iterate_by_steps(fix_rules, fix_iterations):
                logger.info(
                    f"Step {step}: uitvoeren van fix-rule {rule['fix_id']} ({object_layer}, {rule['attribute_name']})"
                )
                try:
                    attribute_name = rule["attribute_name"]
                    attribute_validation_ids = validation_ids[object_layer][attribute_name]
                    function = next(iter(rule["fix_method"]))
                    input_variables = rule["fix_method"][function]
                    # find all invalid indices
                    indices = _invalid_indices(object_gdf, object_validation_result, attribute_validation_ids)
                    # apply filter on indices
                    if "filter" in rule.keys():
                        filter_function = next(iter(rule["filter"]))
                        filter_input_variables = rule["filter"][filter_function]
                        series = _process_logic_function(object_gdf, filter_function, filter_input_variables)
                        series = series[series.index.notna()]
                        filter_indices = series.loc[series].index.to_list()
                        indices = [i for i in filter_indices if i in indices]
                    else:
                        filter_indices = []
                    # add object_relation
                    if "related_object" in input_variables.keys():
                        input_variables = _add_related_gdf(input_variables, new_datamodel, object_layer)
                    elif "custom_function_name" in input_variables.keys():
                        # inputs = input_variables
                        input_variables = _add_custom_kwargs(input_variables, new_datamodel)
                        if input_variables["custom_function_name"] == "if_else":
                            input_variables = pre_run_logic(object_gdf.loc[indices], input_variables)
                            input_variables = _run_true_false(object_gdf.loc[indices], input_variables)
                            input_variables["kwargs"]["attribute"] = attribute_name
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, new_datamodel)

                    if object_gdf.loc[indices].empty:
                        object_gdf.loc[indices, attribute_name] = np.nan
                    else:
                        result = _process_general_function(object_gdf.loc[indices], function, input_variables)
                        object_gdf.loc[indices, attribute_name] = result

                    # apply manual overwrites
                    object_gdf = _apply_manual_overwrites(
                        object_gdf, layers_summary, object_layer, attribute_name, logger
                    )

                    # recompute the general rules after fix application to make sure all derivations are up to date for the next fix rules
                    object_gdf = _apply_general_rules(
                        gdf=object_gdf,
                        layer=object_layer,
                        rules=object_rules,
                        overwrite=True,
                        datamodel=new_datamodel,
                        logger=logger,
                        result_summary=result_summary,
                        raise_error=raise_error,
                    )

                except Exception as e:
                    logger.error(f"{object_layer}: fix_rule {rule['fix_id']} crashed width Exception {e}")
                    result_summary.append_error(
                        (
                            "fix_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                            f"(object: '{object_layer}', id: '{rule['fix_id']}', function: '{function}', "
                            f"input_variables: {input_variables}, Reason (Exception): {e})"
                        )
                    )
                    if raise_error:
                        raise e
                    else:
                        pass

        if object_gdf.empty:
            logger.warning(
                f"{object_layer}: geen valide objecten na fixen. Inspecteer 'fix_oordeel' in de resultaten; deze is false voor alle objecten. De laag zal genegeerd worden in de (topo)logische validatie."
            )
        else:
            new_datamodel.set_data(object_gdf, object_layer, index_col=None)

        # re_order columns
        column_order = object_gdf.columns.to_list() + SUMMARY_COLUMNS
        if "geometry" in object_gdf.columns:
            column_order.remove("geometry")
            column_order += ["geometry"]
        object_gdf = object_gdf[column_order]

    ## return updated_datamodel, new layerssummary, new resultsummary
    return new_datamodel, layers_summary, result_summary


# %%
