from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from core.schematisation_builder.fixer.summaries import ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary
from hhnk_research_tools.logging import logging
from hydamo_validation import logical_validation
from hydamo_validation.logical_validation import (
    _add_join_gdf,
    _add_related_gdf,
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


def _build_general_rules_lookup_table(validation_rules: Dict[str, Any]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Build a lookup table for derived variables originating from 'general_rules'.

    This transforms the validation rules structure into a quick‑access mapping:
        {
            layer_name: {
                result_variable_name: <function-definition-dict>
            }
        }

    Parameters
    ----------
    validation_rules : dict
        Raw validation‑rules dictionary read from validationrules.json. Expected
        structure:
            validation_rules[layer]["general_rules"] → list of rules

    Returns
    -------
    dict
        Nested mapping enabling efficient recursive evaluation of general rule
        dependencies when resolving input variables.

        {
          layer_name: {
              result_variable_name: { <function dict> }
          }
        }
    """
    lookup: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for layer, ruleset in validation_rules.items():
        general_rules = ruleset.get("general_rules", [])
        layer_lookup: Dict[str, Dict[str, Any]] = {}
        for gr in general_rules:
            # NOTE: your JSON uses "result_variable" for name
            rv = gr.get("result_variable")
            if rv and "function" in gr:
                layer_lookup[rv] = gr["function"]
        lookup[layer] = layer_lookup
    return lookup


def _extract_inputs_from_function(
    func: Dict[str, Any],
    current_layer: str,
    layers: Set[str],
    columns_by_layer: Dict[str, Set[str]],
    general_lookup_by_layer: Dict[str, Dict[str, Dict[str, Any]]],
    seen_general: Set[Tuple[str, str]] | None = None,
) -> List[Dict[str, Any]]:
    """
    Recursively extract all concrete input references required by a function
    definition used in logical/general/topologic rules.

    This handles:
    - nested function definitions
    - derived variables (recursively expanding them)
    - object‑prefixed references pointing to other layers
    - geometry.* references
    - whole‑object references when no attribute is explicitly provided

    Parameters
    ----------
    func : dict
        Single‑function dictionary where the key is the function name and the
        value is its parameter mapping.
    current_layer : str
        Layer for which the function is being evaluated.
    layers : set[str]
        Set of all available layer names in the datamodel.
    columns_by_layer : dict[str, set[str]]
        Mapping of available columns for each layer.
    general_lookup_by_layer : dict
        Lookup table for derived variables constructed by
        `_build_general_rules_lookup_table`.
    seen_general : set of (layer, variable), optional
        Used to prevent infinite recursion when derived variables depend on
        each other.

    Returns
    -------
    list of dict
        List of resolved input references, each item of the form:
            {"object": <layer>, "attribute": <column-or-None>}
    """

    if seen_general is None:
        seen_general = set()

    fname = next(iter(func))
    params = func[fname]
    inputs: List[Dict[str, Any]] = []
    prefix = ""

    # -------------------------------------------------------------
    # STEP 1 — Detect object references via "*object*" in key
    # -------------------------------------------------------------
    referenced_objects: Dict[str, str] = {}  # {object: prefix}
    for key, val in params.items():
        if "object" in key.lower() and isinstance(val, str) and val in layers:
            prefix = key.lower().split("object")[0].rstrip("_")
            referenced_objects[val] = prefix

    # -------------------------------------------------------------
    # STEP 2 — Process each parameter
    # -------------------------------------------------------------
    for key, val in params.items():
        # ----------------------------
        # CASE A — numeric → ignore
        # ----------------------------
        if isinstance(val, (int, float, bool)) or val is None:
            continue

        # ----------------------------
        # CASE B — nested function
        # ----------------------------
        if isinstance(val, dict) and len(val) == 1:
            nested = _extract_inputs_from_function(
                val,
                current_layer,
                layers,
                columns_by_layer,
                general_lookup_by_layer,
                seen_general,
            )
            inputs.extend(nested)
            continue

        # ----------------------------
        # CASE C — string only
        # ----------------------------
        if isinstance(val, str):
            # RULE: If val is a derived variable on current layer → expand recursively
            if val in general_lookup_by_layer.get(current_layer, {}) and (current_layer, val) not in seen_general:
                seen_general.add((current_layer, val))
                derived = general_lookup_by_layer[current_layer][val]
                sub = _extract_inputs_from_function(
                    derived,
                    current_layer,
                    layers,
                    columns_by_layer,
                    general_lookup_by_layer,
                    seen_general,
                )
                inputs.extend(sub)
                continue

            # RULE: If val is a column of current object and key does not start with a prefix → bind to current layer
            if val in columns_by_layer[current_layer] or val.startswith("geometry."):
                if referenced_objects:
                    if not any([key.lower().startswith(prefix) for prefix in list(referenced_objects.values())]):
                        inputs.append({"object": current_layer, "attribute": val})
                        continue
                else:
                    inputs.append({"object": current_layer, "attribute": val})
                    continue

            # RULE: If val is a column of *another* object BUT only if key respects object-prefix rule
            for obj, prefix in referenced_objects.items():
                if key.lower().startswith(prefix):
                    # RULE: val may be a derived variable on *referenced* object
                    if val in general_lookup_by_layer.get(obj, {}) and (obj, val) not in seen_general:
                        seen_general.add((obj, val))
                        derived = general_lookup_by_layer[obj][val]
                        sub = _extract_inputs_from_function(
                            derived, obj, layers, columns_by_layer, general_lookup_by_layer, seen_general
                        )
                        inputs.extend(sub)
                        break
                    # Raw column on referenced object
                    if val in columns_by_layer.get(obj, set()) or val.startswith("geometry."):
                        inputs.append({"object": obj, "attribute": val})
                        break

    # -------------------------------------------------------------
    # STEP 3 — Whole-object inference
    # Only add {object: <obj>, attribute: None} if there is NO key
    # in 'params' that starts with the object's prefix (besides the
    # "<prefix>_object" key itself).
    # -------------------------------------------------------------
    for obj, prefix in referenced_objects.items():
        has_prefixed_key = any(k.lower().startswith(prefix) and k.lower() != f"{prefix}_object" for k in params.keys())
        # RULE: no attribute keys matching prefix → whole-object reference
        if not has_prefixed_key:
            inputs.append({"object": obj, "attribute": None})

    # -------------------------------------------------------------
    # STEP 4 — Deduplicate
    # -------------------------------------------------------------
    final = []
    seen = set()
    for inp in inputs:
        k = (inp["object"], inp["attribute"])
        if k not in seen:
            seen.add(k)
            final.append(inp)

    return final


def map_general_rule_inputs(
    datamodel,
    layers: List[str],
) -> Dict[str, Dict[int, List[Dict[str, Any]]]]:
    """
    Build a mapping of general-rule input dependencies for each layer.

    This extracts, for each general rule:
    - all referenced layers
    - all referenced attributes
    - recursively included derived variables

    Parameters
    ----------
    datamodel : ExtendedHyDAMO
        HyDAMO datamodel containing layers, validation rules, and metadata.
    layers : list[str]
        List of layer names to analyze.

    Returns
    -------
    dict
        {
          layer_name: {
            general_rule_id: [
                {"object": <layer>, "attribute": <attribute or None>},
                ...
            ]
          }
        }
    """

    # Cache columns per layer
    columns_by_layer: Dict[str, Set[str]] = {
        layer: set(getattr(getattr(datamodel, layer), "columns", [])) for layer in layers
    }

    validation_rules = datamodel.validation_rules
    general_lookup_by_layer = _build_general_rules_lookup_table(validation_rules)

    mapping: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}

    for layer in layers:
        mapping[layer] = {}

        general_rules = validation_rules[layer].get("general_rules", [])
        for gr in general_rules:
            gid = gr["id"]
            func = gr["function"]

            inputs = _extract_inputs_from_function(
                func=func,
                current_layer=layer,
                layers=set(layers),
                columns_by_layer=columns_by_layer,
                general_lookup_by_layer=general_lookup_by_layer,
            )

            mapping[layer][gid] = inputs

    return mapping


def map_validation_rule_inputs(
    datamodel,
    layers: List[str],
    include_topologic: bool = True,
    omit_topologic_as_none: bool = False,
) -> Dict[str, Dict[int, List[Dict[str, Any]]]]:
    """
    Build a mapping of validation-rule input dependencies for each layer.

    This captures the exact (object, attribute) pairs required to evaluate
    each validation rule, including:
    - logic rules
    - general rules
    - optional topologic rules (depending on settings)

    Parameters
    ----------
    datamodel : ExtendedHyDAMO
        Datamodel containing layer GeoDataFrames and validation rules.
    layers : list[str]
        List of layers to process.
    include_topologic : bool, default True
        If False, topologic rules are excluded from the mapping.
    omit_topologic_as_none : bool, default False
        If True and topologic rules are excluded, insert a dummy record of the form:
            {"object": <layer>, "attribute": None}

    Returns
    -------
    dict
        Nested mapping from layer → rule-id → resolved input references.

    {
        <layer_name>: {
            <validation_rule_id>: [
                {"object": <layer>, "attribute": <attribute>},
                ...
            ],
            ...
        },
        ...
    }
    """
    # Cache columns per layer
    columns_by_layer: Dict[str, Set[str]] = {}
    for layer in layers:
        gdf = getattr(datamodel, layer)
        columns_by_layer[layer] = set(getattr(gdf, "columns", []))

    # Prebuild derived-variable lookups per layer
    validation_rules = getattr(datamodel, "validation_rules")
    general_lookup_by_layer = _build_general_rules_lookup_table(validation_rules)

    mapping: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}

    for layer in layers:
        mapping[layer] = {}
        ruleset = validation_rules.get(layer, {})
        vrules = ruleset.get("validation_rules", [])

        for rule in vrules:
            rid = rule.get("id", None)
            func = rule.get("function", {})
            is_topologic = rule.get("type", "") == "topologic"

            if rid is None:
                # Skip invalid rule entries
                continue

            inputs = _extract_inputs_from_function(
                func=func,
                current_layer=layer,
                layers=set(layers),
                columns_by_layer=columns_by_layer,
                general_lookup_by_layer=general_lookup_by_layer,
            )

            if is_topologic and not include_topologic:
                # Omit or emit a recognizable placeholder
                mapping[layer][rid] = [{"object": layer, "attribute": None}] if omit_topologic_as_none else []
            else:
                mapping[layer][rid] = inputs

    return mapping


def _invalid_indices(gdf: gpd.GeoDataFrame, validation_result: gpd.GeoDataFrame, validation_ids: list[int]):
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


def _manual_indices(gdf: gpd.GeoDataFrame, review_gdf: gpd.GeoDataFrame, attribute: str, manual_column: str):
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
    logger.info(
        rf"lagen met valide objecten en regels: {[i for i in list(object_rules_sets.keys())]}"
    )  ## add check to tell which objects have fixes
    for object_layer, object_rules in object_rules_sets.items():
        logger.info(f"{object_layer}: start")
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
            fix_rules_sorted = sorted(
                fix_rules, key=lambda k: k["fix_id"]
            )  ## deze sorting moet eigenlijk op basis van een execution algoritme

            ## voor nu fixen we gewoon even alle fixes op basis van een oplopende fix_id volgorde
            for rule in fix_rules_sorted:
                logger.info(f"{object_layer}: uitvoeren fix-rule met id {rule['fix_id']}")
                try:
                    attribute_name = rule["attribute_name"]
                    description = rule["fix_description"]
                    function = next(iter(rule["fix_method"]))
                    input_variables = rule["fix_method"][function]
                    validation_ids = rule["validation_ids"]
                    indices = _invalid_indices(object_gdf, review_gdf, validation_ids)
                    fix_columns = FixColumns(attribute_name)
                    validation_rules = object_rules["validation_rules"]
                    validation_rules = [i for i in validation_rules if i["id"] in validation_ids]
                    validation_rules_sorted = sorted(validation_rules, key=lambda k: k["id"])

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
                        print(input_variables)
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, new_datamodel)

                    # fill fix columns
                    logger.info(
                        f"{object_layer}: invullen fix kolommen voor {attribute_name} met validatie regels {validation_ids})"
                    )
                    review_gdf.loc[indices, fix_columns.fix_suggestion] = description
                    for _rule in validation_rules_sorted:
                        _rule_id: int = _rule["id"]
                        _error_type: str = _rule["error_type"]
                        _error_message: str = _rule["error_message"]
                        _error_prefix = "C" if _error_type == "critical" else "W"
                        _indices = _invalid_indices(object_gdf, review_gdf, [_rule_id])
                        result_variable = _rule["result_variable"]

                        # get function
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
    keep_general: bool = False,
) -> Tuple[ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary]:
    """
    Apply fix-rules and optionally general-rules to the datamodel.

    This function performs the actual mutation of the HyDAMO layers by writing
    corrected attribute values back into the GeoDataFrames.

    Two main sections are executed:
    1. General rules (optional, controlled by keep_general)
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
    keep_general : bool, default False
        If True, also apply general-rules (derivations) to the datamodel.

    Returns
    -------
    (ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary)
        Updated datamodel and summaries after fix application.
    """

    new_datamodel = datamodel
    ## create an updated datamodel based on datamodel post processing information
    object_rules_sets = deepcopy(new_datamodel.validation_rules)
    validation_results = deepcopy(new_datamodel.validation_results)
    logger.info(
        rf"lagen met valide objecten en regels: {[i for i in list(object_rules_sets.keys())]}"
    )  ## add check to tell which objects have fixes
    for object_layer, object_rules in object_rules_sets.items():
        logger.info(f"{object_layer}: start")
        object_gdf: gpd.GeoDataFrame = getattr(
            new_datamodel, object_layer
        ).copy()  ## check if the copy is redundant for our purpose
        object_validation_result = validation_results[object_layer]
        # add summary columns
        for col in SUMMARY_COLUMNS:
            object_gdf[col] = ""

        # general rule section
        if keep_general and "general_rules" in object_rules.keys():
            general_rules = object_rules["general_rules"]
            general_rules_sorted = sorted(general_rules, key=lambda k: k["id"])
            for rule in general_rules_sorted:
                logger.info(f"{object_layer}: uitvoeren general-rule met id {rule['id']}")
                try:
                    result_variable = rule["result_variable"]
                    result_variable_name = f"general_{rule['id']:03d}_{rule['result_variable']}"

                    # get function
                    function = next(iter(rule["function"]))
                    input_variables = rule["function"][function]

                    # remove all nan indices
                    indices = logical_validation._notna_indices(object_gdf, input_variables)
                    dropped_indices = [i for i in object_gdf.index[object_gdf.index.notna()] if i not in indices]

                    # add object_relation
                    if "related_object" in input_variables.keys():
                        input_variables = _add_related_gdf(input_variables, datamodel, object_layer)
                    elif "custom_function_name" in input_variables.keys():
                        input_variables = _add_custom_kwargs(input_variables, datamodel)
                        print(input_variables)
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, datamodel)

                    if dropped_indices:
                        result_summary.append_warning(
                            logical_validation._nan_message(
                                len(dropped_indices),
                                object_layer,
                                rule["id"],
                                "general_rule",
                            )
                        )
                    if object_gdf.loc[indices].empty:
                        object_gdf[result_variable] = np.nan
                    else:
                        result = _process_general_function(object_gdf.loc[indices], function, input_variables)
                        object_gdf.loc[indices, result_variable] = result

                except Exception as e:
                    logger.error(f"{object_layer}: general_rule {rule['id']} crashed width Exception {e}")
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

        # fix rule section
        if "fix_rules" in object_rules.keys():
            fix_rules = object_rules["fix_rules"]
            fix_rules_sorted = sorted(
                fix_rules, key=lambda k: k["fix_id"]
            )  ## deze sorting moet eigenlijk op basis van een execution algoritme
            ## voor nu fixen we gewoon even alle fixes op basis van een oplopende fix_id volgorde
            for rule in fix_rules_sorted:
                logger.info(f"{object_layer}: uitvoeren fix-rule met id {rule['fix_id']}")
                try:
                    attribute_name = rule["attribute_name"]
                    fix_attribute_name = f"fix_{rule['fix_id']:03d}_{rule['attribute_name']}"

                    # get function
                    function = next(iter(rule["fix_method"]))
                    input_variables = rule["fix_method"][function]
                    logger.info(input_variables)
                    validation_ids = rule["validation_ids"]

                    # find all invalid indices
                    indices = _invalid_indices(object_gdf, object_validation_result, validation_ids)
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
                        print(input_variables)
                        print(rule["fix_method"][function])
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, new_datamodel)

                    if object_gdf.loc[indices].empty:
                        object_gdf.loc[indices, attribute_name] = np.nan
                    else:
                        logger.info(input_variables)
                        result = _process_general_function(object_gdf.loc[indices], function, input_variables)
                        object_gdf.loc[indices, attribute_name] = result

                    # apply manual overwrites
                    if object_layer in layers_summary.data_layers:
                        review_gdf: gpd.GeoDataFrame = getattr(layers_summary, object_layer)
                        manual_column = FixColumns(attribute_name).manual_overwrite
                        object_indices, review_indices = _manual_indices(
                            object_gdf, review_gdf, attribute_name, manual_column
                        )
                        if len(object_indices) != len(review_indices):
                            logger.warning("Length of object_indices not equal to length of review_indices")
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
