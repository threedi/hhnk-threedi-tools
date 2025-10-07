import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import fiona
import geopandas as gpd

from hhnk_threedi_tools.core.folders import Folders

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class ModelInfo:
    """ "Dataclass to hold model information."""

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
    Loads a translation file and translates the data datastructure.
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


# def from_file(
#     self,
#     damo_filename,
#     hdb_filename,
#     translation_DAMO=None,
#     translation_HDB=None,
#     layer_selection=None,
#     layers_input_hdb_selection=None,
#     layers_input_damo_selection=None,
#     layer_threedi_filename = None,
# ):
#     """
#     Function that loads the data in GeoDataFrames and applies layer and column mapping

#     :param damo_filename: Path to the .gbd folder with DAMO data
#     :param hdb_filename: Path to the .gbd folder with HDB data
#     :param translation_DAMO: Path to the DAMO translation dictionary
#     :param translation_HDB: Path to the HDB translation dictionary
#     :return: Dictionary containing layer names (keys) and GeoDataFrames (values)
#     """

#     # Define empty data dictionary, to be filled with layer data from file
#     data = {}

#     # Load layers within .gdb datasets
#     self.logger.debug("Find layer names within geopackages")
#     # Get layer in the geopackge

#     if layer_selection == True:
#         layers_gdb_damo = layers_input_damo_selection
#         layers_gdb_hdb = layers_input_hdb_selection

#     else:
#         layers_gdb_damo = fiona.listlayers(damo_filename)
#         layers_gdb_hdb = fiona.listlayers(hdb_filename)
#         layer_threedi = fiona.listlayers(layer_threedi_filename)

#     # Start reading DAMO
#     for layer in layers_gdb_damo:
#         # Check if the layer name is mapped in the translation file
#         if layer in DAMO_LAYERS:
#             # Map column names
#             self.logger.debug(f"Reading DAMO layer {layer}")
#             gdf_damo = gpd.read_file(damo_filename, layer=layer)
#             gdf_damo.columns = gdf_damo.columns.str.lower()

#             # Make the name of the DAMO layer lowecase and the save it in the dictionary
#             data[layer.lower()] = gdf_damo

#     # Start reading HDB
#     for layer in layers_gdb_hdb:
#         # Check if the layer name is mapped in the translation file
#         if layer in HDB_LAYERS:
#             # Map column names
#             self.logger.debug(f"Reading HDB layer {layer}")
#             gdf_hdb = gpd.read_file(hdb_filename, layer=layer)
#             gdf_hdb.columns = gdf_hdb.columns.str.lower()

#             # Make the name of the HDB layer lowecase and the save it in the dictionary
#             data[layer.lower()] = gdf_hdb

#         # loop over all layers and save them in a dictionary
#         for layer in layer_threedi:
#             self.logger.debug(f"Loading layer {layer}")
#             try:
#                 gdf_layer_data = gpd.read_file(filename, layer=layer)
#             except Exception as e:
#                 self.logger.error(f"Error loading layer {layer}: {e}")
#                 continue

#             # Save to dictionary
#             data[layer] = gdf_layer_data

#     # Start translation DAMO
#     if translation_DAMO is not None:
#         self.logger.debug("Start mapping layer and column names of DAMO layers")
#         data = utils.translate(data, translation_DAMO)

#     # Start translation HDB
#     if translation_HDB is not None:
#         self.logger.debug("Start mapping layer and column names of HDB layers")
#         data = utils.translate(data, translation_HDB)

#     return data


# def from_geopackage(self, filename, translation=None):
#     """
#     Load data from GeoPackage (.gpkg) file

#     :param filename: Path of the .gpkg file
#     :param translation: Path of the translation file (optional)
#     :return: Dictionary containing layer names (keys) and GeoDataFrames (values)
#     """

#     data = {}
#     logger.debug("called from_geopackage")

#     # Get layer in the geopackge
#     layers = fiona.listlayers(filename)
#     if not layers:
#         self.logger.error("No layers found in .gpkg, or file does not exist")
#         raise Exception("Error reading GeoPackage")

#     self.logger.debug(f"Layers results: {layers}")

#     # loop over all layers and save them in a dictionary
#     for layer in layers:
#         self.logger.debug(f"Loading layer {layer}")
#         try:
#             gdf_layer_data = gpd.read_file(filename, layer=layer)
#         except Exception as e:
#             self.logger.error(f"Error loading layer {layer}: {e}")
#             continue

#         # Save to dictionary
#         data[layer] = gdf_layer_data

#     self.logger.debug("Done loading layers")

#     # Start translation
#     if translation is not None:
#         self.logger.debug("Start mapping layer and column names")
#         # load file
#         data = utils.translate(data, translation)
#     return data


# import fiona
# import geopandas as gpd
# import logging

# logger = logging.getLogger(__name__)

# def load_layers_from_file(
#     filename,
#     allowed_layers=None,
#     translation=None,
#     layer_selection=False,
#     layers_input_selection=None,
#     lowercase=True,
# ):
#     """
#     Load layers from a GeoPackage (.gpkg) or FileGDB (.gdb) into a dictionary of GeoDataFrames.

#     :param filename: Path to the file to load (.gpkg or .gdb)
#     :param allowed_layers: List of allowed layer names (optional)
#     :param translation: Path to translation mapping file (optional)
#     :param layer_selection: Boolean. If True, only layers_input_selection will be read.
#     :param layers_input_selection: List of layers to load (optional, required if layer_selection=True)
#     :param lowercase: Convert column names to lowercase (default=True)
#     :return: Dictionary of {layer_name: GeoDataFrame}
#     """
#     data = {}

#     # Get available layers
#     try:
#         layers_available = fiona.listlayers(filename)
#     except Exception as e:
#         logger.error(f"Error listing layers from {filename}: {e}")
#         raise

#     if not layers_available:
#         raise Exception(f"No layers found in {filename}")

#     # Determine which layers to load
#     if layer_selection and layers_input_selection:
#         layers_to_load = layers_input_selection
#         logger.debug(f"User-defined layer selection: {layers_to_load}")
#     else:
#         layers_to_load = layers_available
#         logger.debug(f"Loading all layers from {filename}")

#     # Loop through and load
#     for layer in layers_to_load:
#         if allowed_layers and layer not in allowed_layers:
#             logger.debug(f"Skipping non-allowed layer: {layer}")
#             continue

#         logger.debug(f"Loading layer {layer}")
#         try:
#             gdf = gpd.read_file(filename, layer=layer)
#         except Exception as e:
#             logger.error(f"Error loading layer {layer} from {filename}: {e}")
#             continue

#         if lowercase:
#             gdf.columns = gdf.columns.str.lower()

#         data[layer.lower()] = gdf

#     return data


# # En DAMO:
# data.update(load_layers_from_file(damo_filename, DAMO_LAYERS, translation_DAMO))
# data.update(load_layers_from_file(hdb_filename, HDB_LAYERS, translation_HDB))

# # En 3Di:
# data.update(load_layers_from_file(filename, translation=translation))


# def from_file(
#         self,
#         damo_filename,
#         hdb_filename,
#         layer_threedi_filename,
#         translation_DAMO=None,
#         translation_HDB=None,
#         layer_selection=None,
#         layers_input_hdb_selection=None,
#         layers_input_damo_selection=None,
#         layers_input_threedi_selection=None
#         DAMO_HDB = None,
#         three_di = None,
#     ):
#     data = {}

#     logger.debug("Find layer names within geopackages")

#     if layer_selection == True:
#         layers_gdb_damo = layers_input_damo_selection
#         layers_gdb_hdb = layers_input_hdb_selection
#         layers_gdb_threedi = layers_input_threedi_selection
#     else:
#         layers_gdb_damo = fiona.listlayers(damo_filename)
#         layers_gdb_hdb = fiona.listlayers(hdb_filename)
#         layers_gdb_threedi = fiona.listlayers(layer_threedi_filename)

#     if DAMO_HDB == True:
#         # Start reading DAMO
#         for layer in layers_gdb_damo:

#             # Check if the layer name is mapped in the translation file
#             if layer in DAMO_LAYERS:
#                 # Map column names
#                 self.logger.debug(f"Reading DAMO layer {layer}")
#                 gdf_damo = gpd.read_file(damo_filename, layer=layer)
#                 gdf_damo.columns = gdf_damo.columns.str.lower()

#                 # Make the name of the DAMO layer lowecase and the save it in the dictionary
#                 data[layer.lower()] = gdf_damo

#         # Start reading HDB
#         for layer in layers_gdb_hdb:
#             # Check if the layer name is mapped in the translation file
#             if layer in HDB_LAYERS:
#                 # Map column names
#                 self.logger.debug(f"Reading HDB layer {layer}")
#                 gdf_hdb = gpd.read_file(hdb_filename, layer=layer)
#                 gdf_hdb.columns = gdf_hdb.columns.str.lower()

#                 # Make the name of the HDB layer lowecase and the save it in the dictionary
#                 data[layer.lower()] = gdf_hdb

#     if three_di == True:
#         # loop over all layers and save them in a dictionary
#         for layer in layers_gdb_threedi:
#             self.logger.debug(f"Loading layer {layer}")
#             try:
#                 gdf_layer_data = gpd.read_file(layer_threedi_filename, layer=layer)
#             except Exception as e:
#                 self.logger.error(f"Error loading layer {layer}: {e}")
#                 continue

#             # Save to dictionary
#             data[layer] = gdf_layer_data
