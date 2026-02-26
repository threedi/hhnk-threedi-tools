from pathlib import Path

import fiona
import geopandas as gpd
import numpy as np
from hhnk_research_tools.logging import logging
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.datasets import normalize_fiona_schema, read_geopackage

from hhnk_threedi_tools.core.schematisation_builder.utils.summaries import FixLayersSummary, FixResultSummary


class ExtendedHyDAMO(HyDAMO):
    def __init__(self, file_path: Path = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = file_path
        self.properties = self._set_properties()

    def _set_properties(self):
        properties = {}
        if self.path:
            layers = fiona.listlayers(self.path)
            layers_dict = {}
            for layer in layers:
                with fiona.open(self.path) as src:
                    schema = normalize_fiona_schema(src.schema)
                    layers_dict[layer] = schema

            properties[self.path.name] = layers_dict
        return properties

    def _filter_status(self, gdf: gpd.GeoDataFrame, status_object: list[str]):
        if status_object is not None:
            if "statusobject" in gdf.columns:
                gdf = gdf.loc[np.where(gdf["statusobject"].isna() | gdf["statusobject"].isin(status_object))]
        return gdf

    def read_layer(self, layer, result_summary=FixResultSummary(), status_object=None):
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

        if not self.path:
            raise ValueError(f"Path to geopackage unknown. This function only works if a geopackage is available.")

        if layer in self.layers:
            dataset = {k: v for k, v in self.properties.items() if layer in v.keys()}
            file_path = self.path
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
    def from_geopackage(cls, file_path=None, version="2.4", ignored_layers=[], check_columns=True, check_geotype=True):
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
        if not file_path:
            raise ValueError(f"No geopackage path is provided.")

        hydamo = cls(file_path=file_path, version=version, ignored_layers=ignored_layers)
        for layer in fiona.listlayers(file_path):
            if layer in hydamo.layers:
                hydamo_layer: HyDAMO = getattr(hydamo, layer)
                hydamo_layer.set_data(
                    gpd.read_file(file_path, layer=layer),
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


def execute(
    datamodel: HyDAMO,
    fix_rules_sets,  ## think about whether validation_rules_sets is needed as input as well -> no
    layers_summary: FixLayersSummary,  ## needs to filled such that all data can be read for logical fix
    result_summary: FixResultSummary,  ## is basically a form of logging at this point
    logger,
    raise_error,
):
    return datamodel, layers_summary, result_summary


class FixOveriew:
    pass


class ResultsOverview:
    pass
