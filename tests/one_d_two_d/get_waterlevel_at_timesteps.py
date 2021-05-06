import numpy as np
import geopandas as gpd
from shapely.geometry import box
from ...threedi.variables.gridadmin import all_2d
from ...threedi.variables.rain_dataframe import t_start_rain_col, t_end_rain_col, t_end_sum_col
from ...variables.database_aliases import df_geo_col
from ...wsa.saving_functions import save_raster_array_to_tiff
from ...wsa.loading_functions import load_gdal_raster
from ...wsa.conversion_functions import gdf_to_raster
from .variables.dataframe_mapping import wtrlvl_col
from ...folder_structure_and_paths.paths_functions import create_tif_path

def read_2node_wlvl_at_timestep(results, timestep):
    """timesteps is the index of the time in the timeseries you want to use to calculate the wlvl and depth raster"""
    nodes_2d = gpd.GeoDataFrame()
    # * inputs every element from row as a new function argument.
    nodes_2d[df_geo_col] = [box(*row) for row in results.cells.subset(all_2d).cell_coords.T]
    # waterstand
    nodes_2d[wtrlvl_col] = results.nodes.subset(all_2d).timeseries(indexes=[timestep]).s1[0]
    return nodes_2d

def create_depth_raster(wlvl_list, dem_list, dem_nodata, dem_meta, raster_output_path):
    """Calculate the depth raster by subtracting the dem from the wlvl raster."""
    # difference between surface and initial water level
    try:
        depth_list = np.subtract(wlvl_list, dem_list)

        # restore nodata pixels using a mask, also filter waterways (height=10) and negative depths
        nodatamask = (dem_list == dem_nodata) | (dem_list == 10) | (depth_list < 0)
        depth_list[nodatamask] = dem_nodata

        # write array to tiff
        save_raster_array_to_tiff(output_file=raster_output_path,
                                  raster_array=depth_list,
                                  nodata=dem_nodata,
                                  metadata=dem_meta)
        return depth_list
    except Exception as e:
        raise e from None

def calc_waterlevel_depth_at_timesteps(test_env):
    """
    Deze functie bepaalt de waterstanden op de gegeven tijdstappen op basis van het 3di resultaat. Vervolgens wordt op
    basis van de DEM en de waterstand per tijdstap de waterdiepte bepaald.
    """
    results = test_env.threedi_vars.result
    timesteps = test_env.threedi_vars.scenario_df
    dem_path = test_env.src_paths['dem']
    wtrlvl_path_template = test_env.output_vars['water_level_filename_template']
    wtrdepth_path_template = test_env.output_vars['water_depth_filename_template']
    try:
        timesteps_arr = [timesteps[t_start_rain_col].value,
                         timesteps[t_end_rain_col].value,
                         timesteps[t_end_sum_col].value]
        # hours since start of calculation
        timestrings = [int(round(results.nodes.timestamps[t] / 60 / 60, 0)) for t in
                       timesteps_arr]
        dem_list, dem_nodata, dem_meta = load_gdal_raster(dem_path)
        for timestep, timestr in zip(timesteps_arr, timestrings):
            # output files
            wlvl_tif = wtrlvl_path_template.format(timestr)
            wlvl_output_path = create_tif_path(folder=test_env.output_vars['layer_path'],
                                               filename=wlvl_tif)
            depth_tif = wtrdepth_path_template.format(timestr)
            depth_output_path = create_tif_path(folder=test_env.output_vars['layer_path'],
                                                filename=depth_tif)
            # calculate waterlevel at selected timestep in nodes gdf
            nodes_2d_wlvl = read_2node_wlvl_at_timestep(results, timestep)
            wlvl_list = gdf_to_raster(gdf=nodes_2d_wlvl,
                                      value_field=wtrlvl_col,
                                      raster_out=wlvl_output_path,
                                      nodata=dem_nodata,
                                      metadata=dem_meta)
            # calculate water depth at time steps at nodes
            create_depth_raster(wlvl_list, dem_list, dem_nodata, dem_meta, depth_output_path)
        return timestrings
    except Exception as e:
        raise e from None
