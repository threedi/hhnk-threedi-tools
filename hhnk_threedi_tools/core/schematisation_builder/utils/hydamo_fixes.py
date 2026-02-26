import geopandas as gpd
import hhnk_research_tools as hrt
from hhnk_research_tools.logging import logging
from typing import Tuple
from hhnk_threedi_tools.core.schematisation_builder.utils.summaries import FixLayersSummary, FixResultSummary
from hydamo_validation.datamodel import HyDAMO

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
from hydamo_validation.logical_validation import (
    _process_general_function,
    _process_logic_function,
    _process_topologic_function,
)
from hydamo_validation.datamodel import HyDAMO
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

def execute(
        datamodel: HyDAMO,
        fix_rules_sets, ## think about whether validation_rules_sets is needed as input as well -> no
        layers_summary: FixLayersSummary, ## needs to filled such that all data can be read for logical fix
        result_summary: FixResultSummary, ## is basically a form of logging at this point
        logger,
        raise_error,
    ):

    return datamodel, layers_summary, result_summary


class FixOveriew:
    pass

class ResultsOverview:
    pass

