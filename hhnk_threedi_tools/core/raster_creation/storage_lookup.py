# %%
import numpy as np
import pandas as pd
import os

# from threedi_scenario_downloader import downloader as dl
import hhnk_threedi_tools.core.api.downloader as dl
import hhnk_research_tools as hrt
from hhnk_research_tools import Folder





# %%
def create_storage_lookup(storage_df, rootzone_thickness, output_file):
    """function creates a list of available storage for given rootzone thckness"""

    # Compute total available storage at all depths
    dstep = 0.01
    depths = np.arange(0,10+dstep,dstep)

    ### tijdelijk roothzone op 40 cm zetten omdat dit in voorbeel Jeroen wordt gebruikt
    ## rootzone_thickness = 40 (is maar een voorbeeld)

    storage_lookup = []
    local_storage_sum = 0
    for soil_type in np.unique(storage_df['soil_type']):

        #Create list of dewateringdepths and corresponding storage coefficient and roothzone storage from capsim table.
        xlist = np.round(storage_df.loc[(storage_df['soil_type']==soil_type) & (storage_df['rootzone_thickness']==rootzone_thickness), 
                                        'dewatering_depth'].tolist(),5) # x = ontwateringsdiepte 
        sclist = np.round(storage_df.loc[(storage_df['soil_type']==soil_type) & (storage_df['rootzone_thickness']==rootzone_thickness), 
        'storage_coefficient'].tolist(),5) # y = bergingscoefficient
        rzlist = np.round(storage_df.loc[(storage_df['soil_type']==soil_type) & (storage_df['rootzone_thickness']==rootzone_thickness), 
        'root zone soil moisture storage in equilibrium conditions (mm)'].tolist(),5) # y = bergingscoefficient

        max_soil_rooth_storage = rzlist.max()

        for d in depths:
            # calculate and summate storage in unsaturated zone

            if d == 0:
                local_coef=0
                local_storage = 0
                local_storage_sum = 0
            else:
                local_coef = np.interp(x=d, xp=xlist, fp=sclist)
                local_storage = local_coef * dstep * 1000
                local_storage_sum = local_storage_sum + local_storage

            # calculate storage in rooth zone
            rooth_storage = np.interp(x=d, xp=xlist, fp=rzlist)
            avail_rooth_storage = max_soil_rooth_storage - rooth_storage

            # Total availeble storage
            total_storage = (avail_rooth_storage + local_storage_sum) / 1000

            storage_lookup.append(
                {
                    'Soil Type': soil_type,
                    'Rootzone Thickness (cm)': rootzone_thickness,
                    'Dewathering Depth (m)': d,
                    'Storage Coefficient (m/m)': local_coef,
                    'Local Storage (mm)': local_storage,
                    'Unsa Zone Storage (mm)': local_storage_sum,
                    'Local Total Rooth Zone Storage (mm)': rooth_storage,
                    'Available Rooth Zone Storage (mm)': avail_rooth_storage,
                    'Total Available Storage (m)': total_storage
                }
            )

    storage_lookup_df = pd.DataFrame(storage_lookup)
    storage_lookup_df.to_csv(output_file)

def download_lizard_rasters(uuid,output_path, resolution, bounds):
    if not os.path.exists(output_path):
        # Download rasters that are not on system yet.

        #API v4
        # dl.download_raster(scenario=uuid,
        #                     raster_code = "",
        #                     projection  = "EPSG:28992",
        #                     resolution  = resolution,
        #                     bbox      = bounds,
        #                     bounds_srs  = "EPSG:28992",
        #                     pathname    = output_path,
        #                     is_threedi_scenario = False)
        #API v3
        dl.download_raster(scenario=uuid,
                            raster_code = "",
                            target_srs  = "EPSG:28992",
                            resolution  = resolution,
                            bounds      = bounds,
                            bounds_srs  = "EPSG:28992",
                            pathname    = output_path,
                            is_threedi_scenario = False)
    # else:
    #     print('File already on system')

def create_dewa_raster(dem_path, gxg_path, output_path, dem_nodata, dem_meta,overwrite=False):

    # Load input rasters to array
    dem_list = wsa.gdal_load_raster(dem_path)
    gxg_list = wsa.gdal_load_raster(gxg_path)

    #TODO raster extent op elkaar afstemmen

    # Create dewatering depth
    if not os.path.exists(output_path) and overwrite==False:
        overwrite=True 
    if overwrite:

        # compute dewathering depth from dem
        dewa_mask = (dem_list != dem_nodata) & (gxg_list != iwl_nodata)
        dewadepthsp_arr = dem_list * 0
        dewadepthsp_arr[dewa_mask] = np.asarray(dem_list[dewa_mask] - gxg_list[dewa_mask])

        wsa.save_raster_array_to_tiff(output_path, dewadepthsp_arr, dem_nodata, dem_meta)
    
    return dewadepthsp_arr

# compute storage 
def compute_storage(storage_df, roothzone, dewadepth_arr, soil_list, nodatamask, nodatavalue, zeromask):

    sto_coeff_arr = np.zeros(dewadepth_arr.shape)
    
    #Iterate over all soil types
    for soil_type in np.unique(storage_df['Soil Type']):
        soil_mask = soil_list == soil_type

        #Create list of dewateringdepths and corresponding storage coefficient from capsim table.
        xlist = np.round(storage_df.loc[(storage_df['Soil Type']==soil_type) & 
                                        (storage_df['Rootzone Thickness (cm)']==roothzone), 
                                        'Dewathering Depth (m)'].tolist(),5) # x = ontwateringsdiepte 
        ylist = np.round(storage_df.loc[(storage_df['Soil Type']==soil_type) & 
                                        (storage_df['Rootzone Thickness (cm)']==roothzone), 
                                        'Total Available Storage (m)'].tolist(),5) # y = bergingscoefficient

        #Determine the storage coefficient per pixel using the actual dewatering depth (dewadepth_arr[soil_mask])
        #and the corresponding storage coefficient (ylist). Find values by interpolation. 
        sto_coeff_arr[soil_mask] = np.interp(x=dewadepth_arr[soil_mask], xp=xlist, fp=ylist)
    
    #Calculate storage
    #     storage_arr = sto_coeff_arr * dewadepth_arr
    storage_arr=sto_coeff_arr
    
    #Apply nodata and zero values
    storage_arr[nodatamask] = nodatavalue
    storage_arr[zeromask] = 0
            
    return storage_arr

def compute_storage_raster2(storagesp_arr):
    """raster maken gelijk aan dem maar waarde nul voor alle niet nodata pixels"""
    storagesp_file = fd_local['output']['bgstif']

    # save raster met data
    wsa.save_raster_array_to_tiff(storagesp_file, 
                                  storagesp_arr, 
                                  dem_nodata, 
                                  dem_meta)