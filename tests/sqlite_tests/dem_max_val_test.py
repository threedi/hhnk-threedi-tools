import numpy as np
from ...wsa.loading_functions import load_gdal_raster
from ...variables.definitions import DEM_MAX_VALUE

def check_dem_max_val(test_env):
    dem = test_env.src_paths['dem']
    try:
        dem_array, dem_nodata, dem_metadata = load_gdal_raster(dem)
        if np.max(dem_array) > DEM_MAX_VALUE:
            result = f"Maximale waarde DEM: {np.max(dem_array)} is te hoog"
        else:
            result = f"Maximale waarde DEM: {np.max(dem_array)} voldoet aan de norm"
        return result
    except Exception as e:
        raise e from None
