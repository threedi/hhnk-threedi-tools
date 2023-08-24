#%%
#!/usr/bin/env python
""" Module for the comparison of DAMO/HDB data and 3Di model data.
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
import geopandas as gpd
from pathlib import Path
import warnings
import os
import sys
import shutil
vergelijkings_tool = Path(r'E:\github\jacostabarragan\hhnk-threedi-tools\hhnk_threedi_tools\core\vergelijkingstool')
if str(vergelijkings_tool) not in sys.path:
        sys.path.append(str(vergelijkings_tool))
from DAMO import DAMO 
from Threedimodel import Threedimodel
from hhnk_threedi_tools import Folders
#%%
def main(cmp_damo_old, cmp_damo_last, cmp_hdb_old, cmp_hdb_last, damo_last_translation, fn_DAMO_selection):
    # Set logging level
    logging.basicConfig(level=logging.INFO)

    # Supress fiona logging to keep logging readable
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if log_name.__contains__('fiona'):
            log_obj.disabled = True

    # Supress GeoSeries.notna warning, as it warns about a changed operator. Currently using the new operator.
    warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
    
   
    # Read selection geopackage (done here and not in a function because it might be that functionality is implemented
    # in a QGIS environment and the shape is passed in a different way
    # fn_DAMO_selection = r"data\input\Zijpe\damo_selection.gpkg"
    
    gdf_selection = gpd.read_file(fn_DAMO_selection)
    selection_shape = gdf_selection.unary_union

    # Create two damo_objects, supply with DAMO-file, HDB-file and optionally a translation_DAMO, 
    # tion_HDB or a
    # clip_shape
    
    damo_old = DAMO(cmp_damo_old, cmp_hdb_old, damo_old_translation, clip_shape=selection_shape)
    damo_new = DAMO(cmp_damo_last, cmp_hdb_last, damo_last_translation, clip_shape=selection_shape)


    # Compare damo objects with eachother and export result to geopackage
    DAMO_comparison, DAMO_statistics = damo_new.compare_with_damo(damo_old,
                                                                  attribute_comparison=fn_damo_attribute_comparison,
                                                                  filename=cmp_export_damo,
                                                                  overwrite=True,
                                                                  styling_path=styling_path)

    # Create Threedimodel object
    threedi_model = Threedimodel(cmp_sqlite_last, translation=threedimodel_translation)
    threedi_comparison, threedi_statistics = \
        threedi_model.compare_with_DAMO(damo_new,
                                        attribute_comparison=fn_model_attribute_comparison,
                                        filename=cmp_export_sqlite,
                                        overwrite=True,
                                        styling_path=styling_path)
    
    return(cmp_export_damo, cmp_export_sqlite)
#%%
if __name__ == '__main__':

    #model location
    folder = Folders(r'\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\castricum')

    # Define output paths
    vergelijking_result = Path(os.path.join(folder.output.path, 'vergelijking_result'))
    if not os.path.exists(vergelijking_result):
        os.makedirs(vergelijking_result)   
        
    cmp_export_damo = vergelijking_result.joinpath('DAMO_comparison.gpkg')
    cmp_export_sqlite= vergelijking_result.joinpath('Threedi_comparison.gpkg')
    cmp_export_hdb= vergelijking_result.joinpath('HDB_comparison.gpkg')

    # Define location from the peilgebeiden to clip information
    fn_DAMO_selection = folder.source_data.peilgebieden.files['peilgebieden'].path

    # Define path where layer stylings can be found (for each layer it will search for <<LAYER_NAME>>.qml
    styling_path = Path(os.path.join(vergelijkings_tool,'styling'))

    # json files locations
    json_folder = (os.path.join(vergelijkings_tool,'json_files'))
    
    #define paths where to copy the json files.
    source_data = folder.source_data.path
    vergelijkings_json_folder = os.path.join(source_data, 'vergelijkingsTool', 'json_files')
    if not os.path.exists(vergelijkings_json_folder):
        shutil.copytree(json_folder, vergelijkings_json_folder)    

    # Define all the paths of the input data
     #damo_last_version
    cmp_damo_last = Path(folder.source_data.damo.path)
    damo_last_translation=Path(os.path.join(vergelijkings_json_folder,'damo_translation.json'))

    #hbd_last_version
    cmp_hdb_last  = Path(folder.source_data.hdb.path)
    hdb_last_translation =Path(os.path.join(vergelijkings_json_folder,'hdb_translation.json'))
    
    #sqlite_last_version
    cmp_sqlite_last = Path(folder.model.schema_base.database_path)
    threedimodel_translation= Path(os.path.join(vergelijkings_json_folder,'threedi_translation.json'))

    #damo_old_version
    cmp_damo_old = Path(r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\castricum\01_source_data\Castricum_v2\DAMO.gdb")
    damo_old_translation = damo_last_translation

    #hdb_old_version
    cmp_hdb_old = Path(r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\castricum\01_source_data\Castricum_v2\HDB.gdb")
    hdb_old_translation = hdb_last_translation
    

    fn_damo_attribute_comparison = Path(os.path.join(vergelijkings_json_folder,'damo_attribute_comparison.json'))
    fn_model_attribute_comparison = Path(os.path.join(vergelijkings_json_folder,'model_attribute_comparison.json'))
    
    main(cmp_damo_old, cmp_damo_last, cmp_hdb_old, cmp_hdb_last, damo_last_translation, fn_DAMO_selection)
# %%
