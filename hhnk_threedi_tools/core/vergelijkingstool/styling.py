import logging
import re
from pathlib import Path

import geopandas as gpd

from hhnk_threedi_tools.core.vergelijkingstool import Dataset, utils
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo, symbology_both

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# styling.py

update_symbology = symbology_both(False)

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


def geom_kind(gdf):
    """
    Determine the kind of geometry in a GeoDataFrame.
    :param gdf: GeoDataFrame to check
    :return: 'point', 'line', or 'polygon' based on the geometry type
    """
    # get the type of geometry
    geometry_type = set(gdf.geometry.type.dropna().unique())
    if geometry_type <= {"Point", "MultiPoint"}:
        return "point"
    if geometry_type <= {"LineString", "MultiLineString"}:
        return "line"
    if geometry_type <= {"Polygon", "MultiPolygon"}:
        return "polygon"


# Function to replace labels based on their current value
def replace_label_DAMO(match, model_info: ModelInfo):
    """
    Replace labels in the DAMO dataset that are going to be use later in symbology.
    """
    label_value = match.group(1)
    value_value = match.group(2)
    symbol_value = match.group(3)
    if symbol_value == "0":
        return f'label="{model_info.model_name} new {model_info.date_new_damo}" value="{model_info.model_name} new" symbol="0"'
    elif symbol_value == "1":
        return f'label="{model_info.model_name} old {model_info.date_old_damo}" value="{model_info.model_name} old" symbol="1"'
    elif symbol_value == "2":
        return f'label="{model_info.model_name} both" value="{model_info.model_name} both" symbol="2"'
    else:
        if label_value.startswith(model_info.model_name) and value_value.startswith(model_info.model_name):
            print("The labels are corrected")


# replace values
def replace_label_3di(match, model_info: ModelInfo):
    """
    Replace labels in the 3Di dataset that are going to be use later in symbology.
    param match: regex match object
    param model_info: ModelInfo object containing model details
    return: formatted string with updated label, value, and symbol
    """
    label_value = match.group(3)
    value_value = match.group(1)
    symbol_value = match.group(2)
    if value_value.__contains__("both"):
        return f'value="{model_info.model_name} both" symbol="2" label="{model_info.model_name} both"'

    elif value_value.__contains__("damo"):
        return f'value="{model_info.model_name} damo" symbol="0" label="Damo {model_info.model_name} {model_info.date_new_damo}"'

    elif value_value.__contains__("sqlite"):
        return f'value="{model_info.model_name} sqlite" symbol="1" label="Model {model_info.model_name} {model_info.date_sqlite}"'


def prepare_layers_for_export(table_C, filename, overwrite=False):
    """
    Prepares layers for export by exploding geometries and handling representative points.

    :param table_C: Dictionary of GeoDataFrames
    :param filename: Output file path
    :param overwrite: Whether to overwrite existing file
    :return: Modified table_C
    """
    if Path(filename).exists():
        if overwrite:
            Path.unlink(filename)
        else:
            raise FileExistsError(
                f'The file "{filename}" already exists. If you want to overwrite the existing file, add overwrite=True to the function.'
            )

    for layer_name in list(table_C):
        table_C[layer_name] = table_C[layer_name].explode(index_parts=True)

    for layer_name in list(table_C):
        if len(table_C[layer_name].geometry.type.unique()) > 1:
            logger.debug(
                f"Layer {layer_name} has multiple geometry types: {table_C[layer_name].geometry.type.unique()}. Using representative point."
            )
            table_C[layer_name].geometry = table_C[layer_name].geometry.representative_point()

    return table_C


def export_comparison_DAMO(table_C, statistics, filename, model_info: ModelInfo, overwrite=False, styling_path=None):
    """
    Exports all compared layers and statistics to a GeoPackage.

    :param table_C: Dictionary containing a GeoDataframe per layer
    :param statistics: Dataframe containing the statistics
    :param filename: Filename of the GeoPackage to export to
    :param overwrite: If true it will delete the old GeoPackage
    :param styling_path: Path to folder containing .qml files. For each layer in table_C it will lookup a .qml file
    with the exact same name as the layer
    :return:
    """

    model_name = model_info.model_name
    table = []
    table_C = prepare_layers_for_export(table_C, filename, overwrite)
    for i, layer_name in enumerate(table_C):
        gdf = table_C[layer_name]

        kind = geom_kind(gdf)

        # Check if the layer name has a style in the styling folder
        if styling_path is not None:
            qml_name = kind + ".qml"
            qml_file = (styling_path) / "DAMO" / qml_name
            print(qml_file)
        if qml_file.exists():
            logger.debug(f"Style layer for layer {layer_name} found, adding it to the GeoPackage")
            with open(qml_file, "r") as file:
                style = file.read()

            style = re.sub(
                r'label="([^"]*)" value="([^"]*)" symbol="([^"]*)"',
                lambda match: replace_label_DAMO(match, model_name=model_name),
                style,
            )

            # Write the modified content back to the QML file
            with open(qml_file, "w") as file:
                file.write(style)

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

        table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG")
    # add styling to layers
    layer_styles = gpd.GeoDataFrame(columns=STYLING_BASIC_TABLE_COLUMNS, data=table)
    layer_styles.fillna("", inplace=True)
    return layer_styles


def export_comparison_3di(
    table_C, statistics, filename, model_info: ModelInfo, overwrite=False, styling_path=None, crs=None
):
    """
    Exports all compared layers and statistics to a GeoPackage.

    :param table_C: Dictionary containing a GeoDataframe per layer
    :param statistics: Dataframe containing the statistics
    :param filename: Filename of the GeoPackage to export to
    :param overwrite: If true it will delete the old GeoPackage
    :param styling_path: Path to folder containing .qml files. For each layer in table_C it will lookup a .qml file
    with the exact same name as the layer
    :param crs: Coordinate reference system to set for the layers
    :return:
    """

    model_name = model_info.model_name
    table = []
    table_C = prepare_layers_for_export(table_C, filename, overwrite)

    for i, layer_name in enumerate(table_C):
        gdf = table_C[layer_name]
        # if not hasattr(gdf, "geometry") or gdf.geometry is None:
        #     continue
        # else:
        kind = geom_kind(gdf)

        # Check if the layer name has a style in the styling folder
        if styling_path is not None:
            qml_name = kind + ".qml"
            qml_file = (styling_path) / "Threedi" / qml_name

        if qml_file.exists():
            logger.debug(f"Style layer for layer {layer_name} found, adding it to the GeoPackage")
            with open(qml_file, "r") as file:
                style = file.read()

            # Use a regular expression to find and replace label values
            style = re.sub(
                r'label="([^"]*)" value="([^"]*)" symbol="([^"]*)"',
                lambda match: replace_label_3di(match, model_name=model_name),
                style,
            )

            # Write the modified content back to the QML file
            with open(qml_file, "w") as file:
                file.write(style)

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

        table_C[layer_name] = table_C[layer_name].set_crs(crs, allow_override=True)
        table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG")
    # add styling to layers
    layer_styles = gpd.GeoDataFrame(columns=STYLING_BASIC_TABLE_COLUMNS, data=table)
    layer_styles.fillna("", inplace=True)
    return layer_styles
