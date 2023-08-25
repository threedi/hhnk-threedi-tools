# %%
"""
#FIXME 
In ontwikkeling. Kopie van
"\\srv57d1\geo_info\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens\03.scripts\bodemberging_obv_gxg_w.py"
daarna aangepast om werkend te maken in htt als functie.

Dit script rekent grondwaterstand rasters om in een beschikbare bodemberging.

"""

import hhnk_research_tools as hrt
import hhnk_threedi_tools as htt
import importlib

import hhnk_threedi_tools.core.raster_creation.storage_lookup as storage_lookup
importlib.reload(storage_lookup)

import os

import geopandas as gpd
import numpy as np
import pandas as pd


from tests.config import FOLDER_TEST, TEMP_DIR

rootzone_thickness = 20 #cm
building_dh = 0.1 #m. Soil starts 0.1m under building footprint. TODO Wordt dit wel gebruikt? En zou dat moeten?


class Folders(hrt.Folder):
    """base; r'G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens'"""
    def __init__(self, base, create=False):
        super().__init__(base, create=create)

        #input
        self.dem = hrt.Raster(r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC16015 Wateropgave 2.0\11. DCMB\hhnk-modelbuilder-master\data\fixed_data\DEM\DEM_AHN4_int.vrt")

        #Output
        self.add_file("storage", "storage.tif")

# compute storage 
def compute_storage_block(storage_lookup_df, 
                    block_dewa,
                    block_soil, 
                    nodatamask, 
                    nodatavalue, 
                    zeromask):
    """
    Calculate storage for a single raster block.

    storage_lookup_df (pd.DataFrame: lookup table to find available storage using dewa depth 
    block_dewa (np.array): gwlvl - dem
    block_soil (np.array): soil
    nodatamask (np.array): mask where output will be nodata
    nodatavalue (float): output nodata
    zeromask (np.array): mask where output will be zero
    """
    block_storage = np.zeros(block_dewa.shape)
    #Iterate over all soil types
    for soil_type in np.unique(storage_lookup_df['Soil Type']):
        soil_mask = block_soil == soil_type

        #Create list of dewateringdepths, corresponding total storage from capsim table.
        #ontwateringsdiepte
        idx_soil = storage_lookup_df['Soil Type']==soil_type
        xlist = np.round(storage_lookup_df.loc[idx_soil, 'Dewathering Depth (m)'].tolist()
                         ,5) # x = ontwateringsdiepte 
        ylist = np.round(storage_lookup_df.loc[idx_soil, 'Total Available Storage (m)'].tolist()
                         ,5) # y = available storage
        #Determine the storage coefficient per pixel using the actual dewatering depth (dewadepth_arr[soil_mask])
        #and the corresponding storage coefficient (ylist). Find values by interpolation. 
        block_storage[soil_mask] = np.interp(x=block_dewa[soil_mask], xp=xlist, fp=ylist)
        
    #Apply nodata and zero values
    block_storage[nodatamask] = nodatavalue
    block_storage[zeromask] = 0
    return block_storage


def calculate_storage_raster(output_raster, 
                             meta_raster, #FIXME moet anders,
                             groundwlvl_raster,
                             dem_raster,
                             soil_raster,
                             nodata = -9999,
                             overwrite=False):

    #Rasters kunnen verschillende extent hebben, om dezelfde block in te laden
    #de verschillen berekenen.
    dx_min={}
    dy_min={}
    for rtype, rpath in zip(["soil", "dem", "gwlvl"], [soil_raster, dem_raster, groundwlvl_raster]):
        dx_min[rtype], dy_min[rtype], _, _ = hrt.dx_dy_between_rasters(meta_big=rpath.metadata, 
                                                                meta_small=meta_raster.metadata)


    #Controle of we door moeten gaan met berekening.
    cont = hrt.check_create_new_file(output_file=output_raster, overwrite=overwrite)

    if cont:
        #Create output raster
        output_raster.create(metadata=meta_raster.metadata,
                                nodata=nodata, 
                                verbose=False, 
                                overwrite=overwrite)


        #Load output raster so we can edit it.
        target_ds=output_raster.open_gdal_source_write()
        out_band = target_ds.GetRasterBand(1)

        blocks_df = meta_raster.generate_blocks()

        len_total = len(blocks_df)
        for idx, block_row in blocks_df.iterrows():
            
            #Create windows
            window=block_row['window_readarray']

            windows = {}
            for rtype in ["soil", "dem", "gwlvl"]:
                windows[rtype] = window.copy()
                windows[rtype][0] += dx_min[rtype]
                windows[rtype][1] += dy_min[rtype]
            

            #Load blocks
            block_soil = soil_raster._read_array(window=windows["soil"])
            block_dem = dem_raster._read_array(window=windows["dem"])
            block_gwlvl = groundwlvl_raster._read_array(window=windows["gwlvl"])

            #Calc dewatering depth
            block_dewa = block_dem-block_gwlvl

            # create global no data masker
            masks = {}
            masks["dem"] = block_dem == dem_raster.nodata
            masks["soil"] =  block_soil == soil_raster.nodata
            masks["gwlvl"] =  block_gwlvl == groundwlvl_raster.nodata
            mask = np.any([masks[i] for i in masks],0)

            # mask where out storage should be zero
            zeromasks = {}
            zeromasks["dem_water"] = block_dem == 10
            zeromasks["negative_dewa"] = block_dewa < 0
            zeromask = np.any([masks[i] for i in masks],0)

            #Calculate storage
            block_storage = compute_storage_block(storage_lookup_df=storage_lookup_df, 
                                                block_dewa=block_dewa, 
                                                block_soil=block_soil, 
                                                nodatamask=mask, 
                                                nodatavalue=nodata, 
                                                zeromask=zeromask)

            #Write to file
            out_band.WriteArray(block_storage, xoff=window[0], yoff=window[1])
            
            print(f"{idx} / {len_total}", end= '\r')

        out_band.FlushCache()  # close file after writing
        out_band = None
        target_ds = None

# %%
if __name__ == "__main__":

    folder = Folders(TEMP_DIR/f"storage_{hrt.get_uuid()}", create=True)

    unsa_sim = hrt.get_pkg_resource_path(package_resource=htt.resources, name="unsa_sim.csv")

    #Create/load Storage lookup df
    storage_lookup_df = storage_lookup.create_storage_lookup(storage_unsa_sim_path=unsa_sim, 
                                            rootzone_thickness=rootzone_thickness)

    folder_schema =FOLDER_TEST

    output_raster = folder.storage
    overwrite=True
    nodata = -9999

    groundwlvl_raster = folder_schema.model.schema_base.rasters.gwlvl_glg
    dem_raster = folder_schema.model.schema_base.rasters.dem
    soil_raster = folder_schema.model.schema_base.rasters.soil
    meta_raster = dem_raster

    # %%

    calculate_storage_raster(output_raster = output_raster, 
                                meta_raster = meta_raster,
                                groundwlvl_raster = groundwlvl_raster,
                                dem_raster = dem_raster,
                                soil_raster = soil_raster,
                                nodata = nodata,
                                overwrite = overwrite)


    # %%

    assert output_raster.statistics(approve_ok=False) == {'min': 0.0, 'max': 0.14029, 'mean': 0.024702, 'std': 0.031965}
