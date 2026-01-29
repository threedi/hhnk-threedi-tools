import logging
import re
from pathlib import Path
from typing import Dict, Optional, Union

import geopandas as gpd

from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# styling.py

STYLING_BASIC_TABLE_COLUMNS = [
    "id",
    "f_table_catalog",
    "f_table_schema",
    "f_table_name",
    "f_geometry_column",
    "styleName",
    "styleQML",
    "styleSLD",
    "useAsDefault",
    "description",
    "owner",
    "ui",
    "update_time",
]


def prepare_layers_for_export(
    table_C: Dict[str, gpd.GeoDataFrame], filename: Union[str, Path], overwrite: bool = False
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Prepare layers for export by exploding geometries and handling representative points.

    :param table_C: Dictionary of GeoDataFrames
    :param filename: Output file path
    :param overwrite: Whether to overwrite existing file
    :return: Modified table_C
    """

    # Ensure output file handling and normalize geometries for export.
    if Path(filename).exists():
        if overwrite:
            Path.unlink(filename)
        else:
            raise FileExistsError(
                f'The file "{filename}" already exists. If you want to overwrite the existing file, add overwrite=True to the function.'
            )
    # exploded geometries to ensure proper per-feature rows
    for layer_name in list(table_C):
        table_C[layer_name] = table_C[layer_name].explode(index_parts=True)

    # convert mixed-geometry layers to representative points to avoid mixed-type write errors
    for layer_name in list(table_C):
        if len(table_C[layer_name].geometry.type.unique()) > 1:
            logger.debug(
                f"Layer {layer_name} has multiple geometry types: {table_C[layer_name].geometry.type.unique()}. Using representative point."
            )
            table_C[layer_name].geometry = table_C[layer_name].geometry.representative_point()

    return table_C


def export_comparison_DAMO(
    table_C: Dict[str, gpd.GeoDataFrame],
    statistics,
    filename: Union[str, Path],
    model_info: ModelInfo,
    overwrite: bool = False,
    styling_path: Optional[Path] = None,
) -> gpd.GeoDataFrame:
    """
    Export all compared layers and statistics to a GeoPackage.

    :param table_C: Dictionary containing a GeoDataframe per layer
    :param statistics: Dataframe containing the statistics
    :param filename: Filename of the GeoPackage to export to
    :param overwrite: If true it will delete the old GeoPackage
    :param styling_path: Path to folder containing .qml files. For each layer in table_C it will lookup a .qml file
    with the exact same name as the layer
    :return:
    """
    # Write DAMO-styled comparison layers and return a layer_styles table.
    table = []
    table_C = prepare_layers_for_export(table_C, filename, overwrite)
    for i, layer_name in enumerate(table_C):
        # Check if the layer name has a style in the styling folder
        if styling_path is not None:
            qml_name = layer_name + ".qml"
            qml_file = (styling_path) / qml_name
        if qml_file.exists():
            logger.debug(f"Style layer for layer {layer_name} found, adding it to the GeoPackage")
            with open(qml_file, "r") as file:
                style = file.read()

            # keep original QML content as-is, do not perform replacements or write back
            style_name = layer_name + "_style"
            table.append(
                [
                    i,
                    None,
                    None,
                    layer_name,
                    table_C[layer_name]._geometry_column_name,
                    style_name,
                    style,
                    None,
                    "false",
                    None,
                    None,
                    None,
                    None,
                ]
            )

        logger.info(f"Export results of comparing DAMO/3Di layer {layer_name} to {filename}")

        table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG")
    # construct GeoDataFrame describing layer styles
    layer_styles = gpd.GeoDataFrame(columns=STYLING_BASIC_TABLE_COLUMNS, data=table)
    layer_styles.fillna("", inplace=True)
    return layer_styles


def export_comparison_3di(
    table_C: Dict[str, gpd.GeoDataFrame],
    statistics,
    filename: Union[str, Path],
    model_info: ModelInfo,
    overwrite: bool = False,
    styling_path: Optional[Path] = None,
    crs=None,
) -> gpd.GeoDataFrame:
    """
    Export all compared layers and statistics to a GeoPackage.

    :param table_C: Dictionary containing a GeoDataframe per layer
    :param statistics: Dataframe containing the statistics
    :param filename: Filename of the GeoPackage to export to
    :param overwrite: If true it will delete the old GeoPackage
    :param styling_path: Path to folder containing .qml files. For each layer in table_C it will lookup a .qml file
    with the exact same name as the layer
    :param crs: Coordinate reference system to set for the layers
    :return:
    """
    # Write 3Di-styled comparison layers and return layer_styles table.
    table = []
    table_C = prepare_layers_for_export(table_C, filename, overwrite)

    for i, layer_name in enumerate(table_C):
        # locate QML for this layer if styling_path provided
        if styling_path is not None:
            qml_name = layer_name + ".qml"
            qml_file = (styling_path) / qml_name

        if qml_file.exists():
            logger.debug(f"Style layer for layer {layer_name} found, adding it to the GeoPackage")
            with open(qml_file, "r") as file:
                style = file.read()

            style_name = layer_name + "_style"
            table.append(
                [
                    i,
                    None,
                    None,
                    layer_name,
                    table_C[layer_name]._geometry_column_name,
                    style_name,
                    style,
                    None,
                    "false",
                    None,
                    None,
                    None,
                    None,
                ]
            )

        logger.info(f"Export results of comparing DAMO/3Di layer {layer_name} to {filename}")

        # ensure layer uses requested CRS before export
        table_C[layer_name] = table_C[layer_name].set_crs(crs, allow_override=True)
        table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG")
    # add styling to layers
    layer_styles = gpd.GeoDataFrame(columns=STYLING_BASIC_TABLE_COLUMNS, data=table)
    layer_styles.fillna("", inplace=True)
    return layer_styles
