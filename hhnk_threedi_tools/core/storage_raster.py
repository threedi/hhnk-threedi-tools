"""
#FIXME 
In ontwikkeling. Kopie van
"\\srv57d1\geo_info\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens\03.scripts\bodemberging_obv_gxg_w.py"
daarna aangepast om werkend te maken in htt als functie.

Dit script rekent grondwaterstand rasters om in een beschikbare bodemberging.

"""

# %%
import sys
sys.path.insert(0, r"E:\github\wvangerwen\hhnk-research-tools")

import hhnk_research_tools as hrt


import importlib
import folders
importlib.reload(folders)

import functions_bodemberging
importlib.reload(functions_bodemberging)

import csv
import os
import sys
from math import ceil, floor

import geopandas as gpd
import numpy as np
import pandas as pd

#from threedi_scenario_downloader import downloader as dl
import hhnk_threedi_tools.core.api.downloader as dl #FIXME deze vervangen voor threedi_scenario_downloader als het niet meer werkt.
dl.set_api_key('')


import hhnk_research_tools as hrt
# import functions.select_polder_revision_globals as gl_var
#from functions.create_folders_dict_poldermodellen import create_folders_dict
#import functions.wsa_tools as wsa

# gxg_source_name = 'wdm'
# gxg_source = '01.WDM'
# ghg_input_filename = 'ghg-mediaan.tif'
# glg_input_filename = 'glg-mediaan.tif'

gxg_name = f'GHG_ALT_Cut.tif'

unit_factor_to_m = 100
rootzone_thickness = 20 #cm
building_dh = 0.1 #m. Soil starts 0.1m under building footprint. TODO Wordt dit wel gebruikt? En zou dat moeten?
update_storage_lookup = False

#TODO how to process, per polder, of heel hhnk? te grote bestanden?
#TODO afstemmen rasers met verschillende extent etc
#%%
##########################################################

#Init folderstruct
basepath = r'G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens'
folder = folders.Folders(basepath)

# if not folder.input.exists():
#     os.mkdir(r'input\\input222')


folder.input.add_file("storage_lookup_csv", f'storage_lookup_rz{rootzone_thickness}.csv', ftype="file")
folder.input.add_file("glg_lowres",gxg_name,ftype='raster')
folder.input.add_file("area",r'Beheergebied.gpkg',ftype='gpkg')
folder.input.add_file("downloader_log", f'downloader_log.csv', ftype="file")
folder.input.add_file("glg",gxg_name[:-4]+"50cm.tif",ftype='raster')
folder.input.add_file("soil","grondsoort.tif",ftype='raster')

#GXG omzetten in juiste resolutie.
if not folder.input.glg.pl.exists():
    hrt.reproject(folder.input.glg_lowres, 
                target_res=0.5,
                output_path=folder.input.glg.path)



#Get metadata from area
area_gdf = folder.input.area.load()
meta = hrt.create_meta_from_gdf(area_gdf, res=0.5)

#%%
#Download rasters for area
uuids= {}
uuids['soil'] = '9e3534b7-b5d4-46ab-be35-4a0990379f76'
uuids['building'] = '98b5155d-dbc4-4a0c-a407-a9620741d308'

# Download rasters that are not on system yet.

dl.download_raster(scenario=uuids['soil'],
                            raster_code = "",
                            target_srs  = "EPSG:28992",
                            resolution  = meta.pixel_width,
                            bounds      = meta.bounds_dl,
                            bounds_srs  = "EPSG:28992",
                            pathname    = folder.input.soil.path,
                            is_threedi_scenario = False,
                            export_task_csv=folder.input.downloader_log.path)

# functions_bodemberging.download_lizard_rasters(uuid=uuids["soil"],
#                         output_path=folder.input.soil.path, 
#                         resolution=meta.pixel_width, 
#                         bounds=meta.bounds_dl)
# %%
# open cvs unsa sim
storage_df = pd.read_csv(folder.input.unsa_sim.path, sep=';')
storage_df.rename({'soil type':'soil_type',
                'rootzone thickness (cm)': 'rootzone_thickness',
                'groundwater level (meter below surface)': 'dewatering_depth',
                'storage coefficient (m/m)': 'storage_coefficient',
                }, axis=1, inplace=True)

#Create/load Storage lookup df
if update_storage_lookup == True:
    functions_bodemberging.create_storage_lookup(storage_df=storage_df, 
                                        rootzone_thickness=rootzone_thickness, 
                                        output_file=folder.input.storage_lookup_csv.path)
storage_lookup_df = pd.read_csv(folder.input.storage_lookup_csv.path)


# %%



# compute storage 
def compute_storage_block(storage_lookup_df, 
                    roothzone, 
                    block_gxg,
                    block_soil, 
                    nodatamask, 
                    nodatavalue, 
                    zeromask):
    
    block_coeff_storage = np.zeros(block_gxg.shape)
    #Iterate over all soil types
    for soil_type in np.unique(storage_lookup_df['Soil Type']):
        soil_mask = block_soil == soil_type

        #Create list of dewateringdepths, corresponding total storage from capsim table.
        xlist = np.round(storage_lookup_df.loc[(storage_lookup_df['Soil Type']==soil_type) & 
                                        (storage_lookup_df['Rootzone Thickness (cm)']==roothzone), 
                                        'Dewathering Depth (m)'].tolist(),5) # x = ontwateringsdiepte 
        ylist = np.round(storage_lookup_df.loc[(storage_lookup_df['Soil Type']==soil_type) & 
                                        (storage_lookup_df['Rootzone Thickness (cm)']==roothzone), 
                                        'Total Available Storage (m)'].tolist(),5) # y = bergingscoefficient
        #Determine the storage coefficient per pixel using the actual dewatering depth (dewadepth_arr[soil_mask])
        #and the corresponding storage coefficient (ylist). Find values by interpolation. 
        block_coeff_storage[soil_mask] = np.interp(x=block_gxg[soil_mask], xp=xlist, fp=ylist)
    
    #Calculate storage
#     storage_arr = sto_coeff_arr * dewadepth_arr
    block_storage=block_coeff_storage #TODO is dit nog nodig?
    
    #Apply nodata and zero values
    block_storage[nodatamask] = nodatavalue
    block_storage[zeromask] = 0
    return block_storage

# %%
def calculate_storage_raster(output_raster, 
                             meta_raster, #FIXME moet anders,
                             gxg_raster,
                             unit_factor_to_m=1,
                             nodata = -9999,
                             overwrite=False):

    #Rasters kunnen verschillende extent hebben, om dezelfde block in te laden
    #de verschillen berekenen.
    dx_min={}
    dy_min={}
    for rtype, rpath in zip(["soil", "dem", "gxg"], [folder.input.soil, folder.dem, gxg_raster]):
        dx_min[rtype], dy_min[rtype], _, _ = hrt.dx_dy_between_rasters(meta_big=rpath.metadata, 
                                                                meta_small=meta_raster.metadata)

    cont = True

    #Controle of we door moeten gaan met berekening.
    if output_raster.pl.exists():
        if overwrite is False:
            cont=False
        else:
            try:
                output_raster.pl.unlink()
            except:
                #Filelocks tijdens testen...
                i=0
                stem = output_raster.pl.stem
                while i<10:
                    output_raster = hrt.Raster(output_raster.pl.with_stem(f"{stem}_{i}"))
                    if not output_raster.pl.exists():
                        break
                    i+=1

    if cont:
        #Create output raster
        output_raster.create(metadata=meta_raster.metadata,
                                nodata=nodata, 
                                verbose=False, 
                                overwrite=overwrite)


        #Load raster so we can edit it.
        target_ds=output_raster.open_gdal_source_write()
        out_band = target_ds.GetRasterBand(1)

        blocks_df = meta_raster.generate_blocks()

        len_total = len(blocks_df)
        for idx, block_row in blocks_df.iterrows():
            
            #Create windows
            window=block_row['window_readarray']

            windows = {}
            for rtype in ["soil", "dem", "gxg"]:
                windows[rtype] = window.copy()
                windows[rtype][0] += dx_min[rtype]
                windows[rtype][1] += dy_min[rtype]
            

            #Load blocks
            block_soil = folder.input.soil._read_array(window=windows["soil"])
            block_dem = folder.dem._read_array(window=windows["dem"])
            block_gxg = gxg_raster._read_array(window=windows["gxg"])


            # create global no data masker
            nodatamask_dem = block_dem == folder.dem.nodata
            nodatamask_soil =  block_soil == folder.input.soil.nodata
            nodatamask_gxg =  block_gxg == gxg_raster.nodata

            nodatamask = nodatamask_dem | nodatamask_soil | nodatamask_gxg

            zeromask = (block_dem == 10)

            #Convert to m
            block_gxg = np.divide(block_gxg,unit_factor_to_m)

            #Calculate storage
            block_storage = compute_storage_block(storage_lookup_df=storage_lookup_df, 
                                                roothzone=rootzone_thickness, 
                                                block_gxg=block_gxg, 
                                                block_soil=block_soil, 
                                                nodatamask=nodatamask, 
                                                nodatavalue=nodata, 
                                                zeromask=zeromask)

            #Write to file
            out_band.WriteArray(block_storage, xoff=window[0], yoff=window[1])
            
            print(f"{idx} / {len_total}", end= '\r')

        out_band.FlushCache()  # close file after writing
        out_band = None
        target_ds = None

# %%
overwrite=True
output_raster = folder.output.glg

# gxg_raster = folder.input.glg
# meta_raster = folder.input.glg #Use extent of this raster for calc
gxg_raster = hrt.Raster(r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens\input\\"+gxg_name[:-4]+"50cm.tif")
meta_raster = folder.input.soil

nodata = -9999
# %%
gxg_names = ["GLG_WDM_Cut.tif","GLG_ALT_Cut.tif","GLG_ACA_Cut.tif","GHG_WDM_Cut.tif","GHG_ALT_Cut.tif","GHG_ACA_Cut.tif"]

for i in range(len(gxg_names)):
    folder.input.add_file("glg_lowres",gxg_names[i],ftype='raster')
    folder.input.add_file("glg",gxg_names[i][:-4]+"50cm.tif",ftype='raster')
    folder.output.add_file("glg",gxg_names[i][:-4]+"_storage.tif",ftype='raster')
    # if not folder.input.glg.pl.exists():
    #     hrt.reproject(folder.input.glg_lowres, 
    #                 target_res=0.5,
    #                 output_path=folder.input.glg.path)
    
    overwrite = True
    output_raster = folder.output.glg
    gxg_raster = hrt.Raster(r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens\input" + "\\" + gxg_names[i][:-4] + "50cm.tif")

    print(f"Raster {i} out of {len(gxg_names)}:")
    
    calculate_storage_raster(output_raster=output_raster, 
                                meta_raster=meta_raster, #FIXME moet anders,
                                gxg_raster=gxg_raster,
                                unit_factor_to_m=unit_factor_to_m,
                                nodata = nodata,
                                overwrite=overwrite)

# %%

df = meta_raster.generate_blocks_geometry()

df[["geometry"]].to_file(folder.output.pl/"blocks.gpkg", driver="GPKG")
    
# create additional zero's mask
 #| (dem_list == 10)
# %%
