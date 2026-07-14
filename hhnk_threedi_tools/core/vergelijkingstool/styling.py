import logging
import re
from pathlib import Path
from typing import Dict, Optional, Union

import geopandas as gpd

from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo

logger = logging.getLogger("Styling")
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
    table_C: Dict[str, gpd.GeoDataFrame],
    filename: Union[str, Path],
    overwrite: bool = False,
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Prepare layers for export by exploding geometries and handling mixed geometry types.

    Special rule for KDU:
    - If KDU has only line geometries, keep it as KDU.
    - If KDU has point geometries or mixed point+line geometries, convert to Point
      and export it as KDU_point.

    Other layers keep the previous behavior:
    - Mixed point+other geometries are normalized to Point.
    - Only lines, only polygons, or mixed lines/polygons are left as-is.

    :param table_C: Dictionary of GeoDataFrames
    :param filename: Output file path
    :param overwrite: Whether to overwrite existing file
    :return: Modified table_C ready for export
    """

    filename = Path(filename)

    if filename.exists():
        if overwrite:
            filename.unlink()
        else:
            raise FileExistsError(
                f'The file "{filename}" already exists. '
                "If you want to overwrite the existing file, add overwrite=True to the function."
            )

    def to_point(geom):
        if geom is None or geom.is_empty:
            return None

        geometry_type = geom.geom_type

        if geometry_type == "Point":
            return geom

        if geometry_type == "MultiPoint":
            return geom.centroid

        if "Line" in geometry_type:
            return geom.interpolate(0.5, normalized=True)

        return geom.centroid

    prepared_layers: Dict[str, gpd.GeoDataFrame] = {}

    for layer_name, gdf in table_C.items():
        gdf = gdf.copy()

        # Explode geometries to ensure proper per-feature rows
        gdf = gdf.explode(index_parts=True)

        # Remove null or empty geometries before checking types
        gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty].copy()

        if gdf.empty:
            continue

        unique_types = set(gdf.geometry.geom_type.unique())

        point_types = {"Point", "MultiPoint"}
        line_types = {"LineString", "MultiLineString", "LinearRing"}

        has_points = bool(unique_types & point_types)
        has_lines = bool(unique_types & line_types)

        only_points = unique_types.issubset(point_types)
        only_lines = unique_types.issubset(line_types)

        # Special handling for KDU
        if layer_name == "KDU":
            # KDU only line geometries: keep as KDU
            if only_lines:
                prepared_layers["KDU"] = gdf
                continue

            # KDU only points or mixed point+line: export as KDU_point
            if only_points or (has_points and has_lines):
                gdf["geometry"] = gdf.geometry.apply(to_point)
                gdf = gdf.set_geometry("geometry")
                gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty].copy()

                prepared_layers["KDU_point"] = gdf
                continue

            # Fallback for unexpected KDU geometry combinations
            if has_points:
                gdf["geometry"] = gdf.geometry.apply(to_point)
                gdf = gdf.set_geometry("geometry")
                gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty].copy()

                prepared_layers["KDU_point"] = gdf
                continue

            prepared_layers["KDU"] = gdf
            continue

        # Skip if no points involved, or if already all points
        if not has_points or only_points:
            prepared_layers[layer_name] = gdf
            continue

        # Mixed point + other geometry types: normalize all to Point
        logger.debug(
            f"Layer {layer_name} has mixed point+other geometry types: {unique_types}. Normalizing all to Point."
        )

        gdf["geometry"] = gdf.geometry.apply(to_point)
        gdf = gdf.set_geometry("geometry")
        gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty].copy()

        prepared_layers[layer_name] = gdf

    return prepared_layers


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
        else:
            logger.warning(f"Style layer for layer {layer_name} not found")
        table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG")
    # construct GeoDataFrame describing layer styles
    logger.info(f"Export results of comparing DAMO/3Di layer to {filename}")
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
        else:
            logger.error(f"Style layer for layer {layer_name} not found, adding it to the GeoPackage")

        # ensure layer uses requested CRS before export
        table_C[layer_name] = table_C[layer_name].set_crs(crs, allow_override=True)
        table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG")

    # add styling to layers
    layer_styles = gpd.GeoDataFrame(columns=STYLING_BASIC_TABLE_COLUMNS, data=table)
    logger.info(f"Export results of comparing DAMO/3Di layer {layer_name} to {filename}")
    layer_styles.fillna("", inplace=True)
    return layer_styles
