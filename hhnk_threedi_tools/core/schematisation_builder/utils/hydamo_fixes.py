from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from hhnk_research_tools.logging import logging
from hydamo_validation import logical_validation
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.datasets import normalize_fiona_schema, read_geopackage
from hydamo_validation.logical_validation import _add_join_gdf, _add_related_gdf

from hhnk_threedi_tools.core.schematisation_builder.utils.summaries import ExtendedLayersSummary, ExtendedResultSummary

INVALID_COLUMNS = ["invalid_critical", "invalid_non_critical", "ignored"]


@dataclass(frozen=True)
class FixColumns:
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


class ExtendedHyDAMO(HyDAMO):
    def __init__(self, hydamo_path: Path = None, results_path: Path = None, rules_objects: list = [], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hydamo_path = hydamo_path
        self.results_path = results_path

        self.post_process_datamodel(rules_objects)

    @property
    def empty(self) -> bool:
        """
        Return True if the instance has *no* GeoDataFrame attributes.
        """
        for value in self.__dict__.values():
            if isinstance(value, gpd.GeoDataFrame):
                return False
        return True

    def post_process_datamodel(self, objects: list) -> None:
        """Post-process DataModel from self.validation_results."""
        self.validation_results: dict[str, gpd.GeoDataFrame] = {}
        self.validation_rules: dict[str, dict] = {}

        validation_results = ExtendedLayersSummary.from_geopackage(self.results_path)
        for hydamo_layer in self.layers:
            self.validation_results[hydamo_layer] = getattr(validation_results, hydamo_layer)
            self.validation_rules[hydamo_layer] = next((obj for obj in objects if obj["object"] == hydamo_layer), {})

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
                if self.empty:
                    gdf = read_geopackage(file_path, layer=layer)
                else:
                    gdf = getattr(self, layer)
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
        check_geotype=True,
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
            ignored_layers=ignored_layers,
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
    "is_usable",
    "fix_history",
]
KEEP_COLUMNS = ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
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


def _check_attributes(func: dict, validation_rules: dict, attributes: str):
    all_attributes = []
    if func:
        func_name = list(func.keys())[0]
        func_contents: dict = func[func_name]
        for key, item in func_contents.items():
            if item in attributes:
                all_attributes.append(item)
            elif item in _general_rule_variables(validation_rules):
                pass


def _general_rule_variables(validation_rules: dict):
    general_rules = validation_rules.get("general_rules", [])
    variables = [general_rule["attribute_name"] for general_rule in general_rules]
    return variables


def _get_related_attributes(input_dict):
    pass


def _read_validation_rules(gdf: gpd.GeoDataFrame, validation_rules: list[dict], attribute: str):
    columns = gdf.columns
    related_attributes = []
    general_rule_variables = _general_rule_variables(validation_rules)
    for rule in validation_rules:
        func = rule["function"]
        func_name = list(func.keys())[0]
        func_contents: dict = func[func_name]
        func_inputs = func_contents.values()
        func_attribute_inputs = [func_input for func_input in func_inputs if func_input in columns]
        func_general_inputs = [func_input for func_input in func_inputs if func_input in general_rule_variables]

        general_rule_inputs = _check_general_rule_attributes(func_inputs)
        attributes = []
        related_attributes.extend(attributes)
    return related_attributes


from typing import Any, Dict, List, Set, Tuple

# Treat these as "topologic" and handle them specially
TOPOLOGIC_FUNCS: Set[str] = {
    "compare_longitudinal",
    "compare_crossings",
    "nearest_compare",
    "compare_topologic",
}


def _build_general_rules_lookup_table(validation_rules: Dict[str, Any]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Returns:
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
    Fully generic mapping logic based only on the user's three rules.
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

    # {
    #                 "attribute_name": "breedteopening",
    #                 "fix_id": 2,
    #                 "fix_action": "Derived assumption",
    #                 "fix_type": "automatic",
    #                 "fix_method": {
    #                     "iter_func": {


# 						"1": {
# 							"equal": {
# 								"to": "hoogteopening"
# 							}
# 						},
# 						"2": {
# 							"equal": {
# 								"to": 3
# 							}
# 						}
# 					}
#                 },
#                 "fix_description": "DA:breedteopening equal to hoogteopening"
# 			},


def map_general_rule_inputs(
    datamodel,
    layers: List[str],
) -> Dict[str, Dict[int, List[Dict[str, Any]]]]:
    """
    Build a mapping from each layer to each general rule id,
    listing the concrete inputs (object + attribute) needed.

    Returns:
        {
            <layer_name>: {
                <general_rule_id>: [
                    {"object": <object>, "attribute": <attribute>},
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
    Build a mapping from each layer to each validation rule id,
    listing the concrete inputs (object + attribute) needed.

    Args:
        datamodel: your ExtendedHyDAMO-like object that exposes:
            - datamodel.<layer>: GeoDataFrame with .columns
            - datamodel.validation_rules: Dict[layer, {general_rules:[], validation_rules:[]}]
        layers: list of layer names (strings) to process.
        include_topologic: if False, topologic functions are omitted from mapping (set to [] by default).
        omit_topologic_as_none: if True and include_topologic=False, emit [{object: layer, attribute: None}]
                                so you can track "omitted" explicitly.

    Returns:
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


# def map_validation_rule_inputs(datamodel: ExtendedHyDAMO, layers: list[str]):
#     '''
#     Expected output:
#     {
#         profiellijn: {},
#         duikersifonhevel: {
#             0: [{object: duikersifonhevel, object: hoogtebinnenonderkantbov]},
#             1: [{object: duikersifonhevel, object: hoogtebinnenonderkantbene]},
#             2: [{object: duikersifonhevel, object: hoogtebinnenonderkantbov]},
#             3: [{object: duikersifonhevel, object: hoogtebinnenonderkantbene]},
#             4: [{object: duikersifonhevel, object: lengte]},
#             5: [{object: duikersifonhevel, object: breedteopening}, {object: duikersifonhevel, object: hoogteopening}],
#             6: [maybe should be {object: duikersifonhevel, object: None}], # topological function -> set to omit
#             7: [{object: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {object: duikersifonhevel, attribute: hoogtebinnenonderkantbene}, {object: duikersifonhevel, attribute: lengte}]
#             8: [{object: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {object: duikersifonhevel, attribute: hoogtebinnenonderkantbene}]
#             9: [], # topological function -> set to omit
#             10: [{object: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {object: profiellijn, attribute: bodemhoogte}],
#             11: [{object: duikersifonhevel, attribute: hoogtebinnenonderkantbene}, {object: profiellijn, attribute: bodemhoogte}]
#             fake: [{object: profielpunt, attribute: None}]
#         },
#     )
#     Maybe add keys: topologic: True of False and features: multi, single or smth
#     '''
#     mapping = {}
#     for layer in layers:
#         mapping[layer] = {}
#         gdf: gpd.GeoDataFrame = getattr(datamodel, layer)
#         columns = gdf.columns

#         validation_rules_set = datamodel.validation_rules[layer]
#         general_rules = validation_rules_set.get("general_rules", [])
#         general_rule_variables = [general_rule["attribute_name"] for general_rule in general_rules]

#         validation_rules = validation_rules_set.get("validation_rules", [])
#         for rule in validation_rules:
#             validation_id: int = rule["id"]
#             validation_function: dict = rule["function"]
#             validation_function_name = list(validation_function.keys())[0]
#             validation_function_params: dict = validation_function[validation_function_name]
#             validation_function_param_attributes = []
#             for param, variable in validation_function_params.items():
#                 if variable in general_rule_variables:
#                     general_function = next(
#                         (
#                             general_rule["function"] for general_rule in general_rules
#                             if general_function["result_variable"] == variable
#                         ),
#                         {}
#                     )
#                     general_function_name = list(general_function.keys())[0]
#                     general_function_params: dict = general_function[general_function_name]
#                     general_function_param_objects = [gen_variable for gen_variable in list(general_function_params.values()) if gen_variable in layers]
#                     general_function_param_attributes = []
#                     for gparam, gvariable in general_function_params.items():
#                         obj = layer
#                         if gvariable in layers:
#                             obj = gvariable
#                             obj_key = gparam
#                             obj_key_start = obj_key.split("_")[0]
#                             ## ...

#                         if gvariable in general_rule_variables:
#                             ...
#                         elif gvariable in columns:
#                             mapping[validation_id].append({"layer": obj, "attribute": gvariable})


#             # param for param in validation_function_params if param in columns and not param == layer]
#             # func_general_inputs = [param for param in validation_function_params if param in _general_rule_variables(rule)]
def check(
    datamodel: ExtendedHyDAMO,
    layerssummary: ExtendedLayersSummary,
    resultsummary: ExtendedResultSummary,
    layers: list[str],
    columns: list[str],
    logger: logging.Logger,
    raise_error: bool,
):
    # input_mapping_general = ...
    mapped_inputs = map_validation_rule_inputs(datamodel, layers)
    # mapped_inputs_fix = map_fix_rule_inputs()

    ## --> set mappings as property in layerssummary

    # layerssummary.mapping["validation_rules"]

    for layer in layers:
        gdf = getattr(datamodel, layer)
        fix_gdf = prepare(
            gdf,
            layer=layer,
            schema={},  ## schema is most likely needed to account for ignored validation rules
            validation_schema=datamodel.validation_schemas[layer],
            validation_result=datamodel.validation_results[layer],
            validation_rules=datamodel.validation_rules[layer],
            keep_columns=columns,
            logger=logger,
            raise_error=raise_error,
            datamodel=datamodel,
        )
        layerssummary.set_data(fix_gdf, layer, "FIXME")

    return layerssummary


def prepare(  # maybe rename to check() !!! When redoing a fix_overview creation, keep the original manual overwrite columns, but replace the rest of the columns based on new rules.
    gdf: gpd.GeoDataFrame,
    layer: str,
    schema,
    validation_schema,
    validation_result: gpd.GeoDataFrame,
    validation_rules: dict,
    keep_columns,
    logger: logging.Logger,
    raise_error,
    datamodel: ExtendedHyDAMO = None,
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
        logger.info(f"No invalid features found in layer {layer_name}, fixing is not needed/finished for this layer.")
        return layer_report_gdf

    logger.info(f"Created base report gdf with {len(layer_report_gdf)} objects which need fixes")

    # add layer specific columns based on fix config
    add_specific_columns = []
    """
    {
        profiellijn: {},
        duikersifonhevel: {
            0: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov]},
            1: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene]},
            2: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov]},
            3: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene]},
            4: [{layer: duikersifonhevel, attribute: lengte]},
            5: [{layer: duikersifonhevel, attribute: breedteopening}, {layer: duikersifonhevel, attribute: hoogteopening}],
            6: [], # topological function -> set to omit
            7: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene}, {layer: duikersifonhevel, attribute: lengte}]
            8: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene}]
            9: [], # topological function -> set to omit
            10: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {layer: profiellijn, attribute: bodemhoogte}],
            11: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene}, {layer: profiellijn, attribute: bodemhoogte}]
        },
    }


    attribute + validation rule order:
    [hoogtebinnenonderkantbov, fix, 0_fix_description, 2_fix_description, 7_fix_description, 8_fix_description, 10_fix_description, fix_rating, hoogtebinnenonderkantbene, 1, 3, 7, 8, 11, lengte, 4, 7, vormkoker, 5, breedteopening, 5]
    [hoogtebinnenonderkantbov, val_sum_hoogtebinnenonderkantbov                                  , fix_hoogtebinnenonderkantbov, fix_check_hoogtebinnenonderkantbov, fix_history_boogtebinnenonderkantbov                                                                 ]
    [None                    , C0:bok_boven niet plausibel;C2bok_boven > maaiveld;W7:verhang niet tussen 2 tot 5 cm/m;W8:verval niet kleiner dan 50cm;C10:bok bovenstrooms beneden bodem, 
        AA:hoogtebinnenonderkantbov - 0,2, 0:Invalid;2:Invalid;7:Valid; ]
        - Styling: Als waarde hoogtebinnenonderkantbov and {string statement} = True --> groen, else --> rood
    fix_rating: indicate which fixes are fixed when applying the eventual fix
        - Use chosen/filled in value of attribute to check validity of every rule associated using code from validator
    fix_hierarchy: check for a fix whether an input is another attribute/layer. Fixes without a relation will be done last
    What to do with nr_of_profielpunten: how does this get into the fix_overview
        - If a validation_id has no column names as input, then add the general column name and show why it is not valid. You cannot fix it, so then omit the feature

    """
    attributes_in_validation = ...
    """ {
        0: [hoogtebinnenonderkantbov],
        1: [hoogtebinnenonderkantbene],
        2: [hoogtebinnenonderkantbov],
        3: [hoogtebinnenonderkantbene],
        4: [lengte],
        5: [vormkoker, breedteopening, {layer: duikersifonhevel, attribute: hoogteopening}],
        6: [], # empty, when the function is topological
        7: [hoogtebinnenonderkantbov, hoogtebinnenonderkantbene, lengte]
        8: [hoogtebinnenonderkantbov, hoogtebinnenonderkantbene]
        9: [], # topological function
        10: [hoogtebinnenonderkantbov,]


    }
    """
    ## column names: {layer}__{attribute}?
    ## use an _is_valid() to check whether parameters have been validated or are valid (?)
    ## IDEA: make layers for every attribute that need fixing:
    ##      fix_overview__duikersifonhevel__hoogtebinnenonderkantbov
    ##      fix_overview__duikersifonhevel__hoogtebinnenonderkantbene
    ##      etc
    # _check_necessary_fixes() --> check if a fix exists for all validation ids for at least one parameter

    ## prepare fixes and create fix_overview
    ## create temp hydamo and execute fixes
    ## run validation and save results.gpkg
    ## read results.gpkg and style the contents of fix_overview according to which fixes worked (?)

    for fix in fix_rules:
        attribute_name = fix["attribute_name"]
        attribute_names = []  ## attribute and attributes it is related to
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
            layer_report_gdf[f"fix_checks_{attribute_name}"] = None
            layer_report_gdf[f"manual_overwrite_{attribute_name}"] = None

    layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")
    logger.info(f"Added specific columns to report gdf for following attributes: {add_specific_columns}")

    # fill in validation and fix information
    hydamo_check = datamodel
    gdf_check = getattr(hydamo_check, layer_name)

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
                ## related_validation_rules --> rules where the input is one of the attributes
                ## related_ids --> corresponding ids

                validation_ids = attribute_fix["validation_ids"]
                attribute_name = attribute_fix["attribute_name"]
                ## validation_ids = get_ids_related_to_attribute()
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
                                    current_val_sum = layer_report_gdf.at[index, f"validation_sum_{attribute_name}"]
                                    if current_val_sum is None:
                                        layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = text_val_sum
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
                        layer_report_gdf.at[index, f"fixes_{attribute_name}"] = f"{current_fix};{text_fix_suggestion}"

                    # fill in attribute value if present in hydamo layer
                    if attribute_name in gdf_check.columns and attribute_name != "geometry":
                        ## ADD CHECK FOR ATTRIBUTE THAT HAS MULTIPLE VALIDATION IDS WITH MULTIPLE INPUTS
                        ## USE A FIX_ITER FUNCTION

                        ## Fill in the attribute data based on hydamo and based on fix_rules
                        ## For now, say that the highest fix id that is not omit is used to fill in the fixed version
                        ## --> Needs to be based on the input mapping

                        ## !!!!!!!!!!!!!!!!!! HERE WE POPULATE THE DATAMODEL COPY, AFTERWARDS WE COPY DATA TO LAYERREPORT
                        code = row["code"]
                        if fix_id == 2 and "equal" in list(fix_method.keys()):
                            hydamo_value = [fix_method["equal"]["to"]]
                        else:
                            hydamo_value = gdf_check.loc[gdf_check["code"] == code, attribute_name].values
                        if len(hydamo_value) > 0:
                            hydamo_value = hydamo_value[0]
                            if isinstance(hydamo_value, str) and hydamo_value in gdf_check.columns:
                                gdf_check.loc[index, attribute_name] = gdf_check.loc[index, hydamo_value]
                                layer_report_gdf.loc[index, attribute_name] = gdf_check.loc[index, attribute_name]
                            else:
                                gdf_check.loc[index, attribute_name] = hydamo_value
                                layer_report_gdf.loc[index, attribute_name] = gdf_check.loc[index, attribute_name]
                        else:
                            logger.warning(
                                f"Could not find attribute value for code {code} and attribute {attribute_name}"
                            )

                ## Here, use the validation ids and their inputs to run the functions again with the inputs in the validation settings
                check_overview = None
                for validation_id in validation_ids:
                    if validation_id in invalid_ids:
                        validation_rule = next((rule for rule in validation_rules if rule["id"] == validation_id), {})
                        validation_type = validation_rule["type"]
                        validation_function = validation_rule["function"]
                        validation_function_name = next(iter(validation_function))
                        validation_function_inputs = validation_function[validation_function_name]

                        if "related_object" in validation_function_inputs.keys():
                            validation_function_inputs = _add_related_gdf(
                                validation_function_inputs, hydamo_check, layer_name
                            )
                        elif "custom_function_name" in validation_function_inputs.keys():
                            validation_function_inputs["hydamo"] = hydamo_check
                        elif "join_object" in validation_function_inputs.keys():
                            validation_function_inputs = _add_join_gdf(validation_function_inputs, hydamo_check)

                        if validation_type == "logic":
                            ## the layer_report_gdf needs to have the right columns. otherwise check will fail
                            ## make a gdf that combines all the input columns maybe?
                            if gdf_check.loc[[index]].empty:
                                result = np.nan
                            else:
                                result = _process_logic_function(
                                    gdf_check.loc[[index]], validation_function_name, validation_function_inputs
                                ).values[0]  ## gdf / series that gives true or false
                        validity_check = "Valid" if result else "Invalid"
                        if check_overview is None:
                            check_overview = ""
                        if check_overview:
                            check_overview += ";"
                        check_overview += f"{validation_id}:{validity_check}"

                print(f"{layer} - {attribute_name} - {check_overview}")
                layer_report_gdf.loc[index, f"fix_checks_{attribute_name}"] = check_overview
                ## MAYBE BETTER: UPDATE VALID, INVALID_NON_CRITICAL AND INVALID_CRITICAL SUCH THAT THE STYLING CAN SHOW WHAT FEATURES ARE NOT GOOD YET
                ## GOOD IDEA BUT DO BOTH SUCH THAT YOU CAN SEE WHICH FOR WHICH INPUT THE VALIDATION RULE IS VALID/INVALID

                ### POPULATE LAYER_REPORT_GDF WITH DATA FROM GDF_CHECK / DATAMODEL COPY

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
            f"fix_checks_{attribute_name}",
            f"manual_overwrite_{attribute_name}",
        ]
    layer_report_gdf = layer_report_gdf[cols_to_save]
    logger.info(f"Finshed report gdf for layer {layer_name}")

    return layer_report_gdf


def _invalid_indices(gdf: gpd.GeoDataFrame, validation_result: gpd.GeoDataFrame, validation_ids: list[int]):
    """'
    Checks if all validation_ids are in the invalid columns of validation_result and returns the indices where this is the case.
    """
    vids = [str(vid) for vid in validation_ids]
    mask = False
    for col in INVALID_COLUMNS:
        invalid_ids = validation_result[col].str.split(";")
        col_mask = invalid_ids.apply(lambda lst: any(iid == vid for iid in lst for vid in vids))
        mask |= col_mask
    invalid_indices = validation_result[mask].index.tolist()
    return [i for i in invalid_indices if i in gdf.index]


def _get_validation_summary(
    validation_ids: list[int], validation_result: gpd.GeoDataFrame, validation_rules: list[dict], indices, column
):
    gdf = validation_result.loc[indices, :]
    summary = validation_result.loc[indices, [column]]
    for index, row in gdf.iterrows():
        invalid_ids = []
        if row["invalid_critical"] != "":
            invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
        if row["invalid_non_critical"] != "":
            invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

        if any(validation_id in invalid_ids for validation_id in validation_ids):
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
                            current_val_sum = summary.at[index, column]
                            if current_val_sum == "":
                                summary.at[index, column] = text_val_sum
                            else:
                                summary.at[index, column] = f"{current_val_sum};{text_val_sum}"

    return summary


def review(
    datamodel: ExtendedHyDAMO,
    layers_summary: ExtendedLayersSummary,
    result_summary: ExtendedResultSummary,
    logger: logging.Logger,
    raise_error: bool,
):
    # general_inputs_mapping = map_general_rule_inputs(datamodel, layers)
    # validation_inputs_mapping = map_validation_rule_inputs(datamodel, layers)
    # fix_inputs_mapping = ...
    # execution_mapping = ...
    # to execute: do a for loop on the objects and fix rules and execute every fix with a hierarchy of 1. Then do a new for loop for every higher hierarchy etc to take into account dependencies
    logger.info(rf"Start review")
    new_datamodel = datamodel

    ## create an updated datamodel based on datamodel post processing information
    object_rules_sets = new_datamodel.validation_rules
    validation_results = new_datamodel.validation_results
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
        if "fix_rules" in object_rules.keys():  # fix_inputs_mapping[object_layer]:
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
                        input_variables["hydamo"] = new_datamodel
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
                                # getattr(
                                #     datamodel, object_layer
                                # ),  # FIXME: commented as we need to apply filter in topologic functions as well. Remove after tests pass
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


def run(
    datamodel: ExtendedHyDAMO,
    layers_summary: ExtendedLayersSummary,
    result_summary: ExtendedResultSummary,
    logger: logging.Logger,
    raise_error: bool,
    keep_general: bool = False,
):
    # general_inputs_mapping = map_general_rule_inputs(datamodel, layers)
    # validation_inputs_mapping = map_validation_rule_inputs(datamodel, layers)
    # fix_inputs_mapping = ...
    # execution_mapping = ...
    # to execute: do a for loop on the objects and fix rules and execute every fix with a hierarchy of 1. Then do a new for loop for every higher hierarchy etc to take into account dependencies

    new_datamodel = datamodel

    ## create an updated datamodel based on datamodel post processing information
    object_rules_sets = new_datamodel.validation_rules
    validation_results = new_datamodel.validation_results
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
                        input_variables["hydamo"] = datamodel
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
        if "fix_rules" in object_rules.keys():  # fix_inputs_mapping[object_layer]:
            ## sort based on hierarchy key that the user can set in fix_overview.gpkg?
            ## apply omissions
            ## then do the other fixes and filter for is_usable
            ## gdf_add_summary / history function
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
                    validation_ids = rule["validation_ids"]

                    # find all invalid indices
                    indices = _invalid_indices(
                        object_gdf, object_validation_result, validation_ids
                    )  ## checks for every column name in the input variables if a row has any nulls
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
                        input_variables["hydamo"] = new_datamodel
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, new_datamodel)

                    if object_gdf.loc[indices].empty:
                        object_gdf[attribute_name] = np.nan
                    else:
                        result = _process_general_function(object_gdf.loc[indices], function, input_variables)
                        object_gdf.loc[indices, attribute_name] = result

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

    if layers_summary.empty:
        ## populate layerssummary using check() and updated datamodel
        # layers_summary.join_gdf(object_gdf, object_layer)
        pass
    else:
        ## read layerssummary for manual inputs that are used to overwrite config and adjust updated datamodel
        ## overwrite() or update()
        pass

    ## return updated_datamodel, new layerssummary, new resultsummary
    return new_datamodel, layers_summary, result_summary


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

    object_rules_sets = [i for i in validation_rules_sets["objects"] if i["object"] in datamodel.data_layers]
    logger.info(rf"lagen met valide objecten en regels: {[i['object'] for i in object_rules_sets]}")
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
                logger.info(f"{object_layer}: uitvoeren general-rule met id {rule['id']}")
                try:
                    result_variable = rule["result_variable"]
                    result_variable_name = f"general_{rule['id']:03d}_{rule['result_variable']}"

                    # get function
                    function = next(iter(rule["function"]))
                    input_variables = rule["function"][function]

                    # remove all nan indices
                    indices = _notna_indices(object_gdf, input_variables)
                    dropped_indices = [i for i in object_gdf.index[object_gdf.index.notna()] if i not in indices]

                    # add object_relation
                    if "related_object" in input_variables.keys():
                        input_variables = _add_related_gdf(input_variables, datamodel, object_layer)
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
                        result = _process_general_function(object_gdf.loc[indices], function, input_variables)
                        object_gdf.loc[indices, result_variable] = result

                        getattr(datamodel, object_layer).loc[indices, result_variable] = result

                    col_translation = {
                        **col_translation,
                        result_variable: result_variable_name,
                    }
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

        validation_rules = object_rules["validation_rules"]
        validation_rules = [i for i in validation_rules if ("active" not in i.keys()) | i["active"]]
        validation_rules_sorted = sorted(validation_rules, key=lambda k: k["id"])
        # validation rules section
        for rule in validation_rules_sorted:
            try:
                rule_id = rule["id"]
                logger.info(f"{object_layer}: uitvoeren validatieregel met id {rule_id} ({rule['name']})")
                result_variable = rule["result_variable"]
                if "exceptions" in rule.keys():
                    exceptions = rule["exceptions"]
                    indices = object_gdf.loc[~object_gdf[EXCEPTION_COL].isin(exceptions)].index
                else:
                    indices = object_gdf.index
                    exceptions = []
                result_variable_name = f"validate_{rule_id:03d}_{rule['result_variable']}"

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
                    series = _process_logic_function(object_gdf, filter_function, filter_input_variables)
                    series = series[series.index.notna()]
                    filter_indices = series.loc[series].index.to_list()
                    indices = [i for i in filter_indices if i in indices]
                else:
                    filter_indices = []

                if object_gdf.loc[indices].empty:
                    object_gdf[result_variable] = None
                elif rule["type"] == "logic":
                    object_gdf.loc[indices, (result_variable)] = _process_logic_function(
                        object_gdf.loc[indices], function, input_variables
                    )
                elif (rule["type"] == "topologic") and (hasattr(datamodel, "hydroobject")):
                    result_series = _process_topologic_function(
                        # getattr(
                        #     datamodel, object_layer
                        # ),  # FIXME: commented as we need to apply filter in topologic functions as well. Remove after tests pass
                        object_gdf,
                        datamodel,
                        function,
                        input_variables,
                    )
                    object_gdf.loc[indices, (result_variable)] = result_series.loc[indices]

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
                logger.error(f"{object_layer}: validation_rule {rule['id']} width Exception {e}")
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
            if i not in list(col_translation.keys()) + ["nen3610id", "geometry", "rating"] + SUMMARY_COLUMNS
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
                object_gdf.loc[:, i] = object_gdf[i].map(lambda x: ";".join(list(set(str(x).split(LIST_SEPARATOR)))))

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
