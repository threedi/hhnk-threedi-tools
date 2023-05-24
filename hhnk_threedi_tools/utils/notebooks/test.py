# %%
import xarray 
import threedi_raster_edits as tre
from hhnk_threedi_tools import Folders
import geopandas as gpd

from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import ThreediGrid
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG


#User input
folder_path = r"E:\02.modellen\23_Katvoed"
scenario = "ghg_blok_t1000" #mapnaam


folder = Folders(folder_path)

dem_path = folder.model.schema_base.rasters.dem.path
threedi_result = folder.threedi_results.one_d_two_d[scenario]


# Schadeschatter heeft wat extra voorbereiding nodig.
from pathlib import Path
schadeschatter_path = Path(r"E:\01.basisgegevens\hhnk_schadeschatter")

import sys
if str(schadeschatter_path) not in sys.path:
    sys.path.append(str(schadeschatter_path))
import hhnk_schadeschatter as hhnk_wss



#Variables
cfg_file = schadeschatter_path/'01_data/schadetabel_hhnk_2020.cfg'
landuse_file = schadeschatter_path/'01_data/landuse2019_tiles/waterland_landuse2019.vrt'

depth_file = threedi_result.pl/"wdepth_corr.tif"
output_file = threedi_result.pl/"damage.tif"



# %%
wss_settings = {'duur_uur': 48, #uren
                'herstelperiode':'10 dagen',
                'maand':'sep',
                'cfg_file':cfg_file}

#Calculation
wss_local = hhnk_wss.wss_main.Waterschadeschatter(depth_file=depth_file, 
                        landuse_file=landuse_file, 
                        output_file=output_file,
                        wss_settings=wss_settings)

# # #Berkenen schaderaster
wss_local.run()