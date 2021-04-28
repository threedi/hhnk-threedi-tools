import os
import numpy as np
import geopandas as gpd
from ...variables.definitions import OPEN_FILE_GDB_DRIVER
from ...wsa.loading_functions import load_gdal_raster
from ...wsa.conversion_functions import gdf_to_raster
from ...wsa.saving_functions import save_raster_array_to_tiff
from ...folder_structure_and_paths.paths_functions import create_tif_path

def calc_dewatering_depth(test_env):
    """
    Compares initial water level from fixed drainage level areas with
    surface level in DEM of model. Initial water level should be below
    surface level.
    """
    # This add .tif extension to output file name, is needed for save_raster_array_to_tif function
    datachecker_path = test_env.src_paths['datachecker']
    datachecker_fixeddrainage_layer = test_env.src_paths['datachecker_fixed_drainage']
    init_water_level_out = create_tif_path(folder=test_env.output_vars['layer_path'],
                                           filename=test_env.output_vars['init_water_level_filename'])
    init_waterlevel_value_field = test_env.src_paths['init_waterlevel_val_field']
    dewatering_out = create_tif_path(folder=test_env.output_vars['layer_path'],
                                     filename=test_env.output_vars['dewatering_filename'])
    dem_path = test_env.src_paths['dem']
    try:
        # Load layers
        fixeddrainage = gpd.read_file(datachecker_path,
                                      driver=OPEN_FILE_GDB_DRIVER,
                                      layer=datachecker_fixeddrainage_layer)
        dem_array, dem_nodata, dem_metadata = load_gdal_raster(dem_path)
        # Rasterize fixeddrainage
        initial_water_level_arr = gdf_to_raster(fixeddrainage,
                                                value_field=init_waterlevel_value_field,
                                                raster_out=init_water_level_out,
                                                nodata=dem_nodata,
                                                metadata=dem_metadata)
        dewatering_array = np.subtract(dem_array, initial_water_level_arr)
        # restore nodata pixels using mask
        nodata_mask = (dem_array == dem_nodata)
        os.remove(init_water_level_out)
        dewatering_array[nodata_mask] = dem_nodata
        # Save array to raster
        save_raster_array_to_tiff(output_file=dewatering_out,
                                  raster_array=dewatering_array,
                                  nodata=dem_nodata,
                                  metadata=dem_metadata)
        return dewatering_out
    except Exception as e:
        raise e from None
