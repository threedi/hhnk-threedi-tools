import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import LineString
import hhnk_research_tools as hrt
from ...variables.default_variables import DEF_TRGT_CRS
from .variables.definitions import one_d_two_d, two_d, max_sfx, suffixes_list, pump_line
from .variables.dataframe_mapping import id_col, spatialite_id_col, content_type_col, kcu_col, q_m3_s_col, \
    vel_m_s_col, pump_capacity_m3_s_col
from hhnk_research_tools.threedi.variables.rain_dataframe import t_start_rain_col, t_end_sum_col, t_end_rain_col

def read_flowline_results(threedi_result, timesteps_df):
    try:
        coords = hrt.threedi.line_geometries_to_coords(threedi_result.lines.line_geometries)  # create gdf from node coords

        flowlines_gdf = gpd.GeoDataFrame(geometry=coords, crs=f'EPSG:{DEF_TRGT_CRS}')
        flowlines_gdf[id_col] = threedi_result.lines.id
        flowlines_gdf[spatialite_id_col] = threedi_result.lines.content_pk

        content_type_list = threedi_result.lines.content_type.astype('U13')
        flowlines_gdf[content_type_col] = content_type_list

        flowlines_gdf[kcu_col] = threedi_result.lines.kcu
        flowlines_gdf.loc[flowlines_gdf[kcu_col].isin([51, 52]), content_type_col] = one_d_two_d
        flowlines_gdf.loc[flowlines_gdf[kcu_col].isin([100, 101]), content_type_col] = two_d

        q = threedi_result.lines.timeseries(indexes=[timesteps_df[t_start_rain_col].value,
                                                     timesteps_df[t_end_rain_col].value,
                                                     timesteps_df[t_end_sum_col].value]).q  # waterstand
        vel = threedi_result.lines.timeseries(indexes=[timesteps_df[t_start_rain_col].value,
                                                       timesteps_df[t_end_rain_col].value,
                                                       timesteps_df[t_end_sum_col].value]).u1
        q_all = threedi_result.lines.timeseries(indexes=slice(0, -1)).q
        vel_all = threedi_result.lines.timeseries(indexes=slice(0, -1)).u1

        # Write discharge and velocity to columns in dataframe
        for index, time_str in enumerate(suffixes_list):
            if time_str == max_sfx:
                q_max_ind = abs(q_all).argmax(axis=0)
                flowlines_gdf[q_m3_s_col + time_str] = np.round(
                    [row[q_max_ind[enum]] for enum, row in enumerate(q_all.T)], 5)
            else:
                flowlines_gdf[q_m3_s_col + time_str] = np.round(q[index], 5)

        for index, time_str in enumerate(suffixes_list):
            if time_str == max_sfx:
                vel_max_ind = abs(vel_all).argmax(axis=0)
                flowlines_gdf[vel_m_s_col + time_str] = np.round(
                    [row[vel_max_ind[enum]] for enum, row in enumerate(vel_all.T)], 5)
            else:
                flowlines_gdf[vel_m_s_col + time_str] = np.round(vel[index], 3)

        # Flowlines of 1d2d lines weirdly have flow in different direction.
        # Therefore we invert this here so arrows are plotted correctly
        for index, time_str in enumerate(suffixes_list):
            flowlines_gdf.loc[flowlines_gdf[content_type_col] == one_d_two_d, q_m3_s_col + time_str] = \
                flowlines_gdf.loc[flowlines_gdf[content_type_col] == one_d_two_d, q_m3_s_col + time_str].apply(lambda x: x * -1)

        for index, time_str in enumerate(suffixes_list):
            flowlines_gdf.loc[flowlines_gdf[content_type_col] == one_d_two_d, vel_m_s_col + time_str] = \
                flowlines_gdf.loc[flowlines_gdf[content_type_col] == one_d_two_d, vel_m_s_col + time_str].apply(lambda x: x * -1)

        return flowlines_gdf
    except Exception as e:
        raise e from None

def read_pumpline_results(threedi_result, timesteps_df):
    try:
        coords = [LineString([x[[0, 1]], x[[2, 3]]]) for x in threedi_result.pumps.node_coordinates.T]
        pump_gdf = gpd.GeoDataFrame(geometry=coords, crs=f"EPSG:{DEF_TRGT_CRS}")

        pump_gdf[id_col] = threedi_result.pumps.id
        pump_gdf[content_type_col] = pump_line
        pump_gdf[pump_capacity_m3_s_col] = threedi_result.pumps.capacity

        q_m3 = threedi_result.pumps.timeseries(indexes=[timesteps_df[t_start_rain_col].value,
                                                        timesteps_df[t_end_rain_col].value,
                                                        timesteps_df[t_end_sum_col].value]).q_pump  # waterstand
        q_all_pump = threedi_result.pumps.timeseries(indexes=slice(0, -1)).q_pump

        for index, time_str in enumerate(suffixes_list):
            if time_str == max_sfx:
                q_max_ind = abs(q_all_pump).argmax(axis=0)
                pump_gdf[q_m3_s_col + time_str] = np.round(
                    [row[q_max_ind[enum]] for enum, row in enumerate(q_all_pump.T)], 5)
            else:
                pump_gdf[q_m3_s_col + time_str] = np.round(q_m3[index], 5)
        return pump_gdf
    except Exception as e:
        raise e from None

def create_flowlines_results(test_env):
    """
    Deze functie leest alle stroom lijnen in uit het 3di resultaat. Vervolgens wordt gekeken naar het type van de lijn
    (1D2D of 2D). Vervolgens wordt op drie tijdstappen (het begin van de regen het einde van de regen en het einde van de
    som) het volgende bepaald:
        * De waterstand per tijdstap
        * Het debiet (q) in m3/s per tijdstap
        * De stroomsnelheid in m/s per tijdstap
        * De stroomrichting per tijdstap
    """
    # Define output location
    results = test_env.threedi_vars.result
    timesteps_df = test_env.threedi_vars.scenario_df

    # Load individual line results
    flowlines_gdf = read_flowline_results(threedi_result=results, timesteps_df=timesteps_df)
    pumplines_gdf = read_pumpline_results(threedi_result=results, timesteps_df=timesteps_df)

    # combine to one table
    lines_gdf = pd.concat([flowlines_gdf, pumplines_gdf], ignore_index=True, sort=False)
    lines_gdf = lines_gdf[lines_gdf.geometry.length != 0]  # Drop weird values with -9999 geometries
    return lines_gdf
