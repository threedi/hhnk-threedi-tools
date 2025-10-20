import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import fiona
import geopandas as gpd
from shapely.geometry import Point

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.vergelijkingstool import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class ModelInfo:
    """Dataclass to hold model information."""

    model_name: str
    source_data: Path
    source_data_old: Path
    fn_damo_old: Path
    fn_hdb_old: Path
    fn_damo_new: Path
    fn_hdb_new: Path
    damo_selection: Path
    fn_threedimodel: Path
    json_folder: Path
    output_folder: Path
    date_damo_old: str
    date_damo_new: str
    date_hdb_old: str
    date_hdb_new: str
    date_sqlite: str


def get_model_info(path: str) -> ModelInfo:
    """
    Get model information from the given path.
    :param path: Path to the model folder
    :return: ModelInfo object containing model details
    """

    folder = Folders(path)
    source_data = Path(folder.source_data.path)
    model_name = folder.name
    fn_threedimodel = folder.model.schema_base.content[0]

    source_data_old = source_data / "vergelijkingsTool" / "old"
    json_folder = source_data / "vergelijkingsTool" / "json_files"
    fn_damo_old = source_data_old / "DAMO.gdb"
    fn_hdb_old = source_data_old / "HDB.gdb"
    fn_damo_new = source_data / "DAMO.gpkg"
    fn_hdb_new = source_data / "HDB.gpkg"
    damo_selection = source_data / "polder_polygon.gpkg"
    output_folder = source_data / "vergelijkingsTool" / "output"

    return ModelInfo(
        model_name=model_name,
        source_data=source_data,
        source_data_old=source_data_old,
        fn_damo_old=fn_damo_old,
        fn_hdb_old=fn_hdb_old,
        fn_damo_new=fn_damo_new,
        fn_hdb_new=fn_hdb_new,
        damo_selection=damo_selection,
        fn_threedimodel=fn_threedimodel,
        json_folder=json_folder,
        output_folder=output_folder,
        date_damo_old=time.ctime(os.path.getmtime(fn_damo_old)),
        date_damo_new=time.ctime(os.path.getmtime(fn_damo_new)),
        date_hdb_old=time.ctime(os.path.getmtime(fn_hdb_old)),
        date_hdb_new=time.ctime(os.path.getmtime(fn_hdb_new)),
        date_sqlite=time.ctime(os.path.getmtime(fn_threedimodel)),
    )


def translate(data, translation_file):
    """
    Load a translation file and translates the data datastructure.
    Renames tables and columns as indicated in the translation_file

    :param data: Data to be translated
    :param translation_file: Path to translation file
    :return: Translated data
    """

    # load file
    f = open(translation_file)

    try:
        mapping = json.loads(json.dumps(json.load(f)).lower())
    except json.decoder.JSONDecodeError:
        logger.error("Structure of DAMO-translation file is incorrect, check brackets and commas")
        raise

    translate_layers = {}
    # Iterate over the gdf within the data that has inside of it the names of the table from the sqlite or gpkg
    for layer in data.keys():
        # Check if the layer name is mapped in the translation file
        for layer_name in mapping.keys():
            if layer == layer_name:
                # Map column names
                logger.debug(f"Mapping column names of layer {layer}")

                # Rename de columns within the data dictionary following the damo_translation.json file
                data[layer].rename(columns=mapping[layer]["columns"], inplace=True)

                # Store layer mapping in dict to be mapped later
                translate_layers[layer_name] = mapping[layer]["name"]

    # Map layer names
    for old, new in translate_layers.items():
        data[new] = data.pop(old)

    return data


def load_file_and_translate(
    damo_filename=None,
    hdb_filename=None,
    threedi_filename=None,
    translation_DAMO=None,
    translation_HDB=None,
    translation_3Di=None,
    layer_selection=False,
    layers_input_damo_selection=None,
    layers_input_hdb_selection=None,
    layers_input_threedi_selection=None,
    mode="damo",  # options: "damo", "threedi", "both"
):
    """
    Load for DAMO, HDB, and 3Di datasets.
    Depending on `mode`

    mode = "damo" loads DAMO + HDB
    mode = "threedi" loads 3Di layers
    mode = "both" loads all (if paths are provided)
    """

    data = {}
    logger.debug("Find layer names within geopackages")

    # Determine layers to load depending on mode
    if layer_selection:
        layers_damo = layers_input_damo_selection
        layers_hdb = layers_input_hdb_selection
        layers_3di = layers_input_threedi_selection
        logger.debug("Layer selection enabled")
    else:
        layers_damo = fiona.listlayers(damo_filename) if damo_filename else []
        layers_hdb = fiona.listlayers(hdb_filename) if hdb_filename else []
        layers_3di = fiona.listlayers(threedi_filename) if threedi_filename else []

    # Load DAMO + HDB datasets

    if mode in ["damo", "both"]:
        for layer in layers_damo:
            if layer in config.DAMO_LAYERS:
                logger.debug(f"Reading DAMO layer {layer}")
                try:
                    gdf = gpd.read_file(damo_filename, layer=layer)
                    gdf.columns = gdf.columns.str.lower()
                    data[layer.lower()] = gdf
                except Exception as e:
                    logger.error(f"Error loading DAMO layer {layer}: {e}")

        for layer in layers_hdb:
            if layer in config.HDB_LAYERS:
                logger.debug(f"Reading HDB layer {layer}")
                try:
                    gdf = gpd.read_file(hdb_filename, layer=layer)
                    gdf.columns = gdf.columns.str.lower()
                    data[layer.lower()] = gdf
                except Exception as e:
                    logger.error(f"Error loading HDB layer {layer}: {e}")
        # Start translation DAMO
        if translation_DAMO is not None:
            logger.debug("Start mapping layer and column names of DAMO layers")
            data = translate(data, translation_DAMO)

        # Start translation DAMO
        if translation_HDB is not None:
            logger.debug("Start mapping layer and column names of HDB layers")
            data = translate(data, translation_HDB)

    # Load 3Di dataset

    if mode in ["threedi", "both"]:
        for layer in layers_3di:
            logger.debug(f"Loading 3Di layer {layer}")
            try:
                gdf = gpd.read_file(threedi_filename, layer=layer)
                gdf.columns = gdf.columns.str.lower()
                data[layer.lower()] = gdf
            except Exception as e:
                logger.error(f"Error loading 3Di layer {layer}: {e}")

        if translation_HDB is not None:
            logger.debug("Start mapping layer and column names of 3di layers")
            data = translate(data, translation_3Di)

    return data


def update_channel_codes(channel:gpd.GeoDataFrame, cross_section_location:gpd.GeoDataFrame, damo, model_path)->gpd.GeoDataFrame:
    """
    Update channel `code` values by nearest-matching DAMO hydroobject codes and persist the result.

    This function:
    - If all existing channel codes start with "OAF", returns the input `channel` unchanged.
    - Reads feature ids from the "channel" layer in `model_path` (expects a GeoPackage/other Fiona-supported datasource)
      and assigns them to the working `channel` GeoDataFrame as an "id" column.
    - Reads DAMO hydroobjects from the `damo` datasource (layer "hydroobject"), normalizes the code column
      (accepts "CODE" or "code"), and creates a mapping from channel_id -> nearest hydroobject.code using a
      spatial nearest-join between `cross_section_location` (expects a "channel_id" column) and the DAMO data.
      The join uses max_distance=5 (units of the CRS).
    - Maps the found codes onto the channels by matching the channel "id" to the derived mapping and writes the
      updated channel layer back to `model_path` (layer "channel", driver "GPKG").
    - Removes the temporary "id" column from the original `channel` if it existed.

    Parameters
    ----------
    channel : geopandas.GeoDataFrame
        GeoDataFrame representing channel features to update. Column names will be treated case-insensitively
        (lowercased earlier in the pipeline).
    cross_section_location : geopandas.GeoDataFrame
        GeoDataFrame with cross-section locations. Must contain a "channel_id" column and geometries used to
        find the nearest DAMO hydroobject for each channel.
    damo : str or pathlib.Path
        Path to the DAMO datasource (e.g. GPKG) containing a "hydroobject" layer with a code column.
    model_path : str or pathlib.Path
        Path to the model datasource (e.g. GeoPackage) containing a "channel" layer. This file will be written.

    Returns
    -------
    geopandas.GeoDataFrame
        A new GeoDataFrame with updated "code" values for channels (and same geometry/attributes as input `channel`).

    Notes
    -----
    - The spatial nearest join uses a max_distance of 5 (CRS units). Increase this value if your data CRS uses degrees
      or if features are further apart.
    - If the DAMO hydroobject code column is named "CODE" it will be renamed to "code" for consistency.
    - Side effects: writes the updated "channel" layer into `model_path` using the GPKG driver.
    - Exceptions from Fiona/GeoPandas (file access, missing layers, CRS mismatches) are propagated.
    """
    if "code" in channel.columns:
        codes = channel["code"].astype(str).str.strip()  # delete spaces
        starts_with_oaf = codes.str.startswith("OAF", na=False)
    if starts_with_oaf.all():
        print("all the codes are updated")
        return channel

    with fiona.open(model_path, layer="channel") as src:
        ids = [feat["id"] for feat in src]

    channel["id"] = ids
    gdf_cross_section = cross_section_location

    gdf_channel = channel.copy()
    damo_gdf = gpd.read_file(damo, layer="hydroobject")

    cross = gdf_cross_section[["channel_id", "geometry"]].copy()
    if "CODE" in damo_gdf.columns:
        damo_gdf.rename(columns={"CODE": "code"}, inplace=True)

    damo_gdf = damo_gdf[["code", "geometry"]].copy()

    joined = gpd.sjoin_nearest(cross, damo_gdf, how="left", max_distance=5)

    code_map = joined.groupby("channel_id")["code"].first()

    gdf_channel["id"] = gdf_channel["id"].astype(str)
    code_map.index = code_map.index.astype(str)

    gdf_channel["code"] = gdf_channel["id"].map(code_map)

    gdf_channel = gdf_channel.copy()

    if "id" in channel.columns:
        channel.drop(columns=["id"], inplace=True)
    gdf_channel.to_file(model_path, layer="channel", driver="GPKG")

    return gdf_channel
