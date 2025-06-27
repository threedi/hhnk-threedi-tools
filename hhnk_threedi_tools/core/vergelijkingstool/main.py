#!/usr/bin/env python
"""Module for the comparison of DAMO/HDB data and 3Di model data.
With this module the actuality of 3Di models can be assessed.
There are two main usages within this module:
    1. Compare current DAMO/HDB data with the DAMO/HDB data that was used to build the model.
    This gives an indication in how much the water system has changed
    2. Compare the 3Di model with (current) DAMO/HDB data.
    This gives an indication in how much the model differs from the (current) situation.
    Possibly the model was updated in the years together with the DAMO/HDB or the datachecker/modelbuilder induced
    differences

Installation:
The sqlite3 module needs some .dll's in order to unlock the spatial functionality.
These .dll's need to be downloaded from http://www.gaia-gis.it/gaia-sins/.
Under MS Windows binaries -> current stable version -> choose x86 or amd64 -> mod_spatialite-x.x.x-win-xxx.7z.
Module was tested with http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/mod_spatialite-5.0.1-win-amd64.7z
Unpack content of .7z file and place them in the C:\\Windows\\System32 folder
"""

# %%
__authors__ = ["Thijs van den Pol (Royal HaskoningDHV)", "Emiel Verstegen (Royal HaskoningDHV)"]
__contact__ = "emiel.verstegen@rhdhv.com"
__credits__ = ["Thijs van den Pol", "Emiel Verstegen"]
__date__ = "2023/03/13"
__deprecated__ = False
__email__ = "emiel.verstegen@rhdhv.com"
__maintainer__ = "developer"
__status__ = "Production"
__version__ = "1.1.0"

import logging
import os
import warnings
from pathlib import Path

import geopandas as gpd

from hhnk_threedi_tools.core.vergelijkingstool import config, name_date, styling
from hhnk_threedi_tools.core.vergelijkingstool.DAMO import DAMO
from hhnk_threedi_tools.core.vergelijkingstool.styling import *
from hhnk_threedi_tools.core.vergelijkingstool.Threedimodel import Threedimodel

# from folder_names import name


def main(
    fn_DAMO_selection,
    fn_damo_old_translation,
    fn_damo_new,
    fn_hdb_new,
    fn_damo_old,
    fn_hdb_old,
    fn_threedimodel,
    fn_threedimodel_translation,
    fn_damo_attribute_comparison,
    fn_model_attribute_comparison,
    styling_path,
    fn_damo_new_translation,
    fn_DAMO_comparison_export,
    fn_threedi_comparison_export,
    compare_with: str = "Compare with 3Di",
    layer_selection=True,
    layers_input_hdb_selection=None,
    layers_input_damo_selection=None,
    threedi_layer_selector=False,
    threedi_structure_selection=None,
    damo_structure_selection=None,
    structure_codes=None,
):
    # fn_damo_new_translation
    # Set logging level
    logging.basicConfig(level=logging.INFO)

    # Supress fiona logging to keep logging readable
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if log_name.__contains__("fiona"):
            log_obj.disabled = True

    # Supress GeoSeries.notna warning, as it warns about a changed operator. Currently using the new operator.
    warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)

    gdf_selection = gpd.read_file(fn_DAMO_selection, engine="pyogrio")
    selection_shape = gdf_selection.unary_union

    # Create two damo_objects, supply with DAMO-file, HDB-file and optionally a translation_DAMO, translation_HDB or a
    # clip_shape

    selection_compare = compare_with

    if selection_compare == "Compare with Damo":
        damo_old = DAMO(
            fn_damo_old,
            fn_hdb_old,
            translation_DAMO=fn_damo_old_translation,
            clip_shape=selection_shape,
            layer_selection=layer_selection,
            layers_input_hdb_selection=layers_input_hdb_selection,
            layers_input_damo_selection=layers_input_damo_selection,
        )
        damo_new = DAMO(
            fn_damo_new,
            fn_hdb_new,
            translation_DAMO=fn_damo_new_translation,
            clip_shape=selection_shape,
            layer_selection=layer_selection,
            layers_input_hdb_selection=layers_input_hdb_selection,
            layers_input_damo_selection=layers_input_damo_selection,
        )
        print("doing only damo")
        # Compare damo objects with eachother and export result to geopackage
        DAMO_comparison, DAMO_statistics = damo_new.compare_with_damo(
            damo_old,
            attribute_comparison=fn_damo_attribute_comparison,
            filename=fn_DAMO_comparison_export,
            overwrite=True,
            styling_path=styling_path,
        )

        return fn_DAMO_comparison_export

    elif selection_compare == "Compare with 3Di":
        # Create Threedimodel object
        threedi_model = Threedimodel(fn_threedimodel, translation=fn_threedimodel_translation)
        damo_new = DAMO(
            fn_damo_new,
            fn_hdb_new,
            translation_DAMO=fn_damo_new_translation,
            clip_shape=selection_shape,
            layer_selection=False,
            layers_input_hdb_selection=layers_input_hdb_selection,
            layers_input_damo_selection=layers_input_damo_selection,
        )

        threedi_comparison, threedi_statistics = threedi_model.compare_with_DAMO(
            damo_new,
            attribute_comparison=fn_model_attribute_comparison,
            filename=fn_threedi_comparison_export,
            overwrite=True,
            styling_path=styling_path,
            threedi_layer_selector=threedi_layer_selector,
            threedi_structure_selection=threedi_structure_selection,
            damo_structure_selection=damo_structure_selection,
            structure_codes=structure_codes,
        )

        # return(fn_DAMO_comparison_export)
        return fn_threedi_comparison_export

    elif selection_compare == "Both":
        threedi_model = Threedimodel(fn_threedimodel, translation=fn_threedimodel_translation)
        damo_old = DAMO(
            fn_damo_old,
            fn_hdb_old,
            translation_DAMO=fn_damo_old_translation,
            clip_shape=selection_shape,
            layer_selection=layer_selection,
            layers_input_hdb_selection=layers_input_hdb_selection,
            layers_input_damo_selection=layers_input_damo_selection,
        )
        damo_new = DAMO(
            fn_damo_new,
            fn_hdb_new,
            translation_DAMO=fn_damo_new_translation,
            clip_shape=selection_shape,
            layer_selection=layer_selection,
            layers_input_hdb_selection=layers_input_hdb_selection,
            layers_input_damo_selection=layers_input_damo_selection,
        )
        DAMO_comparison, DAMO_statistics = damo_new.compare_with_damo(
            damo_old,
            attribute_comparison=fn_damo_attribute_comparison,
            filename=fn_DAMO_comparison_export,
            overwrite=True,
            styling_path=styling_path,
        )
        threedi_comparison, threedi_statistics = threedi_model.compare_with_DAMO(
            damo_new,
            attribute_comparison=fn_model_attribute_comparison,
            filename=fn_threedi_comparison_export,
            overwrite=True,
            styling_path=styling_path,
            threedi_layer_selector=threedi_layer_selector,
            threedi_structure_selection=threedi_structure_selection,
            damo_structure_selection=damo_structure_selection,
            structure_codes=structure_codes,
        )
        return (fn_DAMO_comparison_export, fn_threedi_comparison_export)

    else:
        print("You must select and option")


# %%

if __name__ == "__main__":
    # model folder

    # folder = Folders(castricum)
    # source_data = folder.source_data.path

    # name
    path = name_date.path
    model_name, source_data, folder = name_date.name(path)

    # polder polygon. It should be a geopackge file
    fn_DAMO_selection = name_date.damo_selection

    # Base folder initial files.
    source_data_old = name_date.source_data_old
    json_file = os.path.join(source_data, "vergelijkingsTool", "json_files")

    # output location.
    out_put_files = os.path.join(source_data, "vergelijkingsTool", "output")

    # Old DAMO (DCMB/FME export) location .
    fn_damo_old = name_date.fn_damo_old
    fn_damo_old_translation = Path(os.path.join(source_data_old, "damo_translation.json"))

    # the last version
    fn_damo_new = name_date.fn_damo_new
    fn_damo_new_translation = fn_damo_old_translation
    # fn_damo_new_translation = Path(os.path.join(source_data_old, 'damo_translation.json'))

    # Old HDB (DCMB/FME export) location .
    fn_hdb_old = name_date.fn_hdb_old
    fn_hdb_old_translation = Path(os.path.join(source_data_old, "hdb_translation.json"))

    # the last version
    fn_hdb_new = name_date.fn_hdb_new
    fn_hdb_new_translation = fn_hdb_old_translation

    fn_threedimodel = name_date.fn_threedimodel
    fn_threedimodel_translation = Path(os.path.join(json_file, "threedi_translation.json"))
    fn_damo_attribute_comparison = Path(os.path.join(json_file, "damo_attribute_comparison.json"))
    fn_model_attribute_comparison = Path(os.path.join(json_file, "model_attribute_comparison.json"))

    # Define path where layer stylings can be found (for each layer it will search for <<LAYER_NAME>>.qml
    styling_path = Path(os.path.join(source_data, "styling"))

    # Define outputs
    fn_DAMO_comparison_export = Path(os.path.join(out_put_files, "DAMO_comparison_Test_110.gpkg"))

    # Layers To Compare DAMO_DAMO
    layer_selection = True
    layers_input_damo_selection = ["Bergingsgebied"]
    layers_input_hdb_selection = []
    fn_threedi_comparison_export = Path(os.path.join(out_put_files, "Threedi_comparison_Test_20.gpkg"))

    # compare_with = "Compare with Damo"
    compare_with = "Compare with 3Di"
    # config.UPDATE_SYMBOLOGY = False

    # Layers to Compare DAMO_3di
    threedi_layer_selector = False
    threedi_structure_selection = [
        "culvert",
        "pump",
    ]
    damo_structure_selection = ["duikersifonhevel", "gemaal"]
    structure_codes = ["KDU", "KGM"]

    main(
        fn_DAMO_selection,
        fn_damo_old_translation,
        fn_damo_new,
        fn_hdb_new,
        fn_damo_old,
        fn_hdb_old,
        fn_threedimodel,
        fn_threedimodel_translation,
        fn_damo_attribute_comparison,
        fn_model_attribute_comparison,
        styling_path,
        fn_damo_new_translation,
        fn_DAMO_comparison_export,
        fn_threedi_comparison_export,
        compare_with,
        layer_selection,
        layers_input_hdb_selection=layers_input_hdb_selection,
        layers_input_damo_selection=layers_input_damo_selection,
        threedi_layer_selector=threedi_layer_selector,
        threedi_structure_selection=threedi_structure_selection,
        damo_structure_selection=damo_structure_selection,
        structure_codes=structure_codes,
    )
# %%
# AfvoergebiedAanvoergebied, Bergingsgebied, DuikerSifonHevel
# hydro_deelgebieden, stuwen_op_peilgrens, Levee_overstromingsmodel

# AfvoergebiedAanvoergebied,  Bergingsgebied
