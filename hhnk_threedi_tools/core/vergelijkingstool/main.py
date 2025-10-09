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
import warnings
from pathlib import Path

import geopandas as gpd

from hhnk_threedi_tools.core.vergelijkingstool.DAMO import DAMO
from hhnk_threedi_tools.core.vergelijkingstool.Threedimodel import Threedimodel

if __name__ == "__main__":
    # Set logging level
    logging.basicConfig(level=logging.INFO)

    # Supress fiona logging to keep logging readable
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if log_name.__contains__("fiona"):
            log_obj.disabled = True

    # Supress GeoSeries.notna warning, as it warns about a changed operator. Currently using the new operator.
    warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)

    # Define all the paths of the input data
    fn_damo_old = Path(r"data\input\Zijpe\old\DAMO.gdb")
    fn_damo_old_translation = Path(r"data\input\Zijpe\old\damo_translation.json")

    fn_damo_new = Path(r"data\input\Zijpe\new\DAMO.gdb")
    fn_damo_new_translation = Path(r"data\input\Zijpe\new\damo_translation.json")

    fn_hdb_old = Path(r"data\input\Zijpe\old\HDB.gdb")
    fn_hdb_old_translation = Path(r"data\input\Zijpe\old\hdb_translation.json")

    fn_hdb_new = Path(r"data\input\Zijpe\new\HDB.gdb")
    fn_hdb_new_translation = Path(r"data\input\Zijpe\new\hdb_translation.json")

    fn_threedimodel = Path(r"data\input\Zijpe\model\Zijpe.sqlite")
    fn_threedimodel_translation = Path(r"data\input\Zijpe\model\threedi_translation.json")

    fn_damo_attribute_comparison = Path(r"data\input\Zijpe\damo_attribute_comparison.json")
    fn_model_attribute_comparison = Path(r"data\input\Zijpe\model_attribute_comparison.json")

    # Define path where layer stylings can be found (for each layer it will search for <<LAYER_NAME>>.qml
    styling_path = Path(r"data\input\Zijpe\styling")

    # Define output paths
    fn_DAMO_comparison_export = Path(r"data\output\Zijpe\DAMO_comparison.gpkg")
    fn_threedi_comparison_export = Path(r"data\output\Zijpe\Threedi_comparison.gpkg")

    # Read selection geopackage (done here and not in a function because it might be that functionality is implemented
    # in a QGIS environment and the shape is passed in a different way
    fn_DAMO_selection = r"data\input\Zijpe\damo_selection.gpkg"
    gdf_selection = gpd.read_file(fn_DAMO_selection)
    selection_shape = gdf_selection.unary_union

    # Create two damo_objects, supply with DAMO-file, HDB-file and optionally a translation_DAMO, translation_HDB or a
    # clip_shape
    damo_old = DAMO(fn_damo_old, fn_hdb_old, translation_DAMO=fn_damo_old_translation, clip_shape=selection_shape)
    damo_new = DAMO(fn_damo_new, fn_hdb_new, translation_DAMO=fn_damo_new_translation, clip_shape=selection_shape)

    # Compare damo objects with eachother and export result to geopackage
    DAMO_comparison, DAMO_statistics = damo_new.compare_with_damo(
        damo_old,
        attribute_comparison=fn_damo_attribute_comparison,
        filename=fn_DAMO_comparison_export,
        overwrite=True,
        styling_path=styling_path,
    )

    # Create Threedimodel object
    threedi_model = Threedimodel(fn_threedimodel, translation=fn_threedimodel_translation)
    threedi_comparison, threedi_statistics = threedi_model.compare_with_DAMO(
        damo_new,
        attribute_comparison=fn_model_attribute_comparison,
        filename=fn_threedi_comparison_export,
        overwrite=True,
        styling_path=styling_path,
    )
