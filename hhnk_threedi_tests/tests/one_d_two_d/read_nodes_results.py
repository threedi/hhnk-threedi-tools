import geopandas as gpd
import numpy as np
from shapely.geometry import box
from hhnk_research_tools.threedi.geometry_functions import coordinates_to_points
from ...variables.default_variables import DEF_TRGT_CRS
from ...variables.database_aliases import df_geo_col
from hhnk_research_tools.threedi.variables.gridadmin import all_2d
from hhnk_research_tools.threedi.variables.rain_dataframe import t_start_rain_col, t_end_sum_col, t_end_rain_col
from .variables.definitions import start_rain_sfx, end_rain_sfx, twelve_hr_after_rain_sfx
from .variables.dataframe_mapping import id_col, spatialite_id_col, node_type_col, one_d, two_d, \
    one_d_boundary_col, max_area_col, wtrlvl_m_col, wet_area_m2_col, minimal_dem_col, volume_m3_col, \
    storage_mm_col

def get_nodes_as_gdf(results):
    try:
        nodes_wlvl_crds = coordinates_to_points(results.nodes)
        nodes_wlvl = gpd.GeoDataFrame(geometry=nodes_wlvl_crds, crs=f'EPSG:{DEF_TRGT_CRS}')
        return nodes_wlvl
    except Exception as e:
        raise e from None

def nodes_to_grid(nodes_wlvl, results):
    """Transfer the nodes into polygons of the grid."""
    try:
        nodes_2d = nodes_wlvl[nodes_wlvl[node_type_col] == two_d].copy()
        # replace geometry with polygons of the cells
        nodes_2d.loc[:, df_geo_col] = [box(*row) for row in
                                       results.cells.subset(all_2d).cell_coords.T]
        nodes_2d.loc[:, minimal_dem_col] = results.cells.subset(all_2d).z_coordinate
        return nodes_2d
    except Exception as e:
        raise e from None

def read_node_results(test_env):
    """
    Deze functie leest alle 2d nodes uit het 3di resultaat en berekent de volgende waarden:
        * de minimale DEM waarde binnen het gebied van de betreffende node (geometrie is omgezet naar een vierkant)
        * het totale oppervlak dat de node beslaat
    Vervolgens wordt op drie tijdstappen (het begin van de regen het einde van de regen en het einde van de som
    de volgende informatie berekend:
        * de waterstand op de genoemde tijdstappen
        * de hoeveelheid water (volume in m3) per tijdstap
        * het natte oppervlak per tijdstap (in m2)
        * opslag van regen in het gebied van de node (hoeveelheid water / totale oppervlak gebied)
    """
    try:
        results = test_env.threedi_vars.result
        timesteps = test_env.threedi_vars.scenario_df
        nodes_wlvl = get_nodes_as_gdf(results)
        # We need to keep a reference to the crs to restore it later
        crs_orig = nodes_wlvl.crs
        nodes_wlvl[id_col] = results.nodes.id
        nodes_wlvl[spatialite_id_col] = results.nodes.content_pk
        nodes_wlvl[node_type_col] = results.nodes.node_type
        # Replace numbers with human readable values
        nodes_wlvl[node_type_col].replace([1, 3, 7], [two_d, one_d, one_d_boundary_col], inplace=True)

        # totaal oppervlak
        nodes_wlvl[max_area_col] = results.nodes.sumax

        #Load results
        # waterstand
        wlvl = results.nodes.timeseries(indexes=[timesteps[t_start_rain_col].value,
                                                 timesteps[t_end_rain_col].value,
                                                 timesteps[t_end_sum_col].value]).s1
        volume = results.nodes.timeseries(indexes=[timesteps[t_start_rain_col].value,
                                                   timesteps[t_end_rain_col].value,
                                                   timesteps[t_end_sum_col].value]).vol
        # actueel nat oppervlak
        wet_area = results.nodes.timeseries(indexes=[timesteps[t_start_rain_col].value,
                                                     timesteps[t_end_rain_col].value,
                                                     timesteps[t_end_sum_col].value]).su

        #Add results to dataframe
        args_lst = [start_rain_sfx, end_rain_sfx, twelve_hr_after_rain_sfx]
        for index, time_str in enumerate(args_lst):
            nodes_wlvl[wtrlvl_m_col + time_str] = np.round(wlvl[index], 2)
        for index, time_str in enumerate(args_lst):
            nodes_wlvl[wet_area_m2_col + time_str] = np.round(wet_area[index], 2)
        for index, time_str in enumerate(args_lst):
            nodes_wlvl[volume_m3_col + time_str] = np.round(volume[index], 2)
        for index, time_str in enumerate(args_lst):
            nodes_wlvl[storage_mm_col + time_str] = np.round(
                nodes_wlvl[volume_m3_col + time_str] / nodes_wlvl[max_area_col], 2)

        # select 2d nodes and create polygons for plotting.
        nodes_2d = nodes_to_grid(nodes_wlvl=nodes_wlvl, results=results)
        orig_geom = nodes_2d[df_geo_col]
        nodes_2d_gdf = gpd.GeoDataFrame(nodes_2d, geometry=orig_geom, crs=crs_orig)
        return nodes_2d_gdf
    except Exception as e:
        raise e from None
