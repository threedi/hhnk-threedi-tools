import geopandas as gpd
import numpy as np
import pandas as pd
from .variables.dataframe_variables import added_calc_val, node_type_col, node_geometry_col, \
    levee_height_col, init_wlevel_col, one_d_two_d_crosses_fixed, levee_height_val, ref_plus_10_val, init_plus_10_val
from ...variables.database_variables import initial_waterlevel_col, type_col, reference_level_col, bank_level_col
from ...variables.database_aliases import df_geo_col, a_chan_id, a_man_id, a_cross_loc_id
from .variables.dataframe_variables import new_bank_level_col, new_bank_level_source_col, bank_level_diff_col
from .gather_information import gather_information

def cross_sec_loc_that_need_new_bank_levels(intersect_1d2d_all, channel_line_geo, cross_loc):
    """2. if the 1d2d line originates from an added calculation node -> find the channel this node is on and keep the
    bank levels for this channel equal to the maximum levee height. """
    # filter nodes that need to have channels with bank levels equal to levee height
    nodes_on_channel = intersect_1d2d_all[intersect_1d2d_all[node_type_col] == added_calc_val].copy()
    nodes_on_channel.drop([initial_waterlevel_col, df_geo_col], axis=1, inplace=True)
    nodes_on_channel = nodes_on_channel.rename(columns={node_geometry_col: df_geo_col})
    # Buffer point to find intersections with the channels (buffering returns point within given distance of geometry)
    nodes_on_channel[df_geo_col] = nodes_on_channel.buffer(0.1)
    # join channels on these nodes (meaning added calculation nodes) to get the channels that need higher bank levels.
    channels_bank_level = gpd.sjoin(nodes_on_channel, channel_line_geo).drop(['index_right'], axis=1)

    # sort so duplicate channel id's are removed, crossings with levees take priority over crossings
    # with peilgrenzen (fixeddrainage)
    channels_bank_level.sort_values(by=[a_chan_id, type_col], ascending=[True, False], inplace=True)
    channels_bank_level.drop_duplicates(a_chan_id, inplace=True)
    channels_bank_level.set_index([a_chan_id], inplace=True, drop=True)

    # join cross_section_location on these channels
    # get cross section locations where corresponding channel id matches channel id's that need
    # higher bank levels (aka channels that intersect with added calculation nodes)
    cross_loc_levee = cross_loc[cross_loc[a_chan_id].isin(channels_bank_level.index.tolist())]
    # Add initial water levels and levee heights to the previously obtained info about channels
    # that need higher bank levels
    cross_loc_levee = cross_loc_levee.join(channels_bank_level[[levee_height_col, init_wlevel_col]],
                                           on=a_chan_id)
    # If a row doesn't have a levee height, the 1d2d line crosses with a fixeddrainagelevelarea (peilgrens).
    cross_loc_fixeddrainage = cross_loc_levee[
        cross_loc_levee[levee_height_col].isna()]
    # If there is a levee height, the 1d2d line crosses with a levee
    cross_loc_levees = cross_loc_levee[
        cross_loc_levee[levee_height_col].notna()]
    return cross_loc_fixeddrainage, cross_loc_levees, cross_loc_levee

def get_new_bank_levels_cross_loc(cross_loc, channel_line_geo, cross_loc_levee, cross_loc_fixeddrainage):
    """
    Function calculates new bank levels. Initially, we set them to either initial water level or
    reference level, depending on which is higher. For cross section locations that cross over levees,
    we set the bank level to levee height.
    """
    try:
        # Find initial waterlevels for cross section locations by matching them to corresponding id of channels
        cross_loc_new_all = cross_loc.join(channel_line_geo[[init_wlevel_col]], on=a_chan_id)
        # All bank levels are set to initial waterlevel +10cm
        cross_loc_new_all[new_bank_level_col] = np.round(cross_loc_new_all[init_wlevel_col] + 0.1, 3).astype(float)
        cross_loc_new_all[new_bank_level_source_col] = init_plus_10_val
        # We start by setting the bank level of all cross location to either initial waterlevel or reference level
        # If the reference level is higher than the initial waterlevel,
        # use this for the banks. (dry bedding in e.g. wieringermeer)
        ref_higher_than_init = cross_loc_new_all[reference_level_col] > cross_loc_new_all[init_wlevel_col]
        cross_loc_new_all.loc[ref_higher_than_init,
                              new_bank_level_col] = np.round(
            cross_loc_new_all[reference_level_col] + 0.1, 3).astype(float)
        cross_loc_new_all.loc[ref_higher_than_init,
                              new_bank_level_source_col] = ref_plus_10_val
        # The cross locations that need levee height are set here
        cross_loc_new_all.loc[cross_loc_levee.index, new_bank_level_col] = cross_loc_levee[
            levee_height_col].astype(float)
        cross_loc_new_all.loc[cross_loc_levee.index, new_bank_level_source_col] = levee_height_val

        # Cross locations that are associated with peilgrenzen get a special label for recognition (values are already set)
        cross_loc_new_all.loc[(cross_loc_new_all.index.isin(cross_loc_fixeddrainage.index)),
                              new_bank_level_source_col] = cross_loc_new_all[
                                                                       new_bank_level_source_col] + '_fixeddrainage'
        cross_loc_new_all[bank_level_diff_col] = np.round(
            cross_loc_new_all[new_bank_level_col] - cross_loc_new_all[bank_level_col], 2)
        # reorder columns
        cross_loc_new_all_filtered = cross_loc_new_all[[
            a_cross_loc_id, a_chan_id, reference_level_col, init_wlevel_col, bank_level_col, new_bank_level_col,
            bank_level_diff_col, new_bank_level_source_col, df_geo_col]]
        cross_loc_new_all_filtered.reset_index(drop=True, inplace=True)
        # Filter the results only on cross section locations where a new bank level is proposed.
        # If the new banklevel is a NaN value, remove it from the list as this implicates that the cross section
        # is on a channel with connection nodes that do not have an initial water level
        cross_loc_new = cross_loc_new_all_filtered.loc[
            (cross_loc_new_all_filtered[bank_level_diff_col] != 0) &
            (cross_loc_new_all_filtered[bank_level_col].notna())]
        return cross_loc_new_all_filtered, cross_loc_new
    except Exception as e:
        raise e from None

def get_updated_channels(channel_line_geo, cross_loc_new_all):
    """With the new (and old) bank levels at cross_section_locations we make a new overview of the channels here.
    In qgis this can be plotted to show how the channels interact with 1d2d (considering bank heights)"""
    cross_locs = cross_loc_new_all.drop_duplicates(a_chan_id)[
        [a_chan_id, new_bank_level_col, bank_level_diff_col, new_bank_level_source_col]].reset_index(drop=True)
    # join cross locations on channels so we have a bank level per channel
    all_channels = pd.merge(channel_line_geo.reset_index(drop=True), cross_locs, left_on=a_chan_id,
                            right_on=a_chan_id)
    return all_channels

def recalculate_bank_levels(test_env):
    try:
        all_manholes_gdf, new_manholes_gdf, calc_node_intersections, channels_gdf, cross_loc_gdf, \
            all_1d2d_flowlines = gather_information(test_env)
        # Finds all channels that intersect with added calculation nodes, matches those that cross
        # levees or peilgrenzen (fixeddrainage) and returns cross section locations matching those channels id's
        cross_loc_fixeddrainage, cross_loc_levee, cross_loc_overview = \
            cross_sec_loc_that_need_new_bank_levels(calc_node_intersections, channels_gdf, cross_loc_gdf)
        cross_loc_new_all, cross_loc_new = get_new_bank_levels_cross_loc(cross_loc_gdf,
                                                                         channels_gdf,
                                                                         cross_loc_levee,
                                                                         cross_loc_fixeddrainage)
        all_channels = get_updated_channels(channels_gdf, cross_loc_new_all)
        return cross_loc_new, new_manholes_gdf, all_1d2d_flowlines, all_channels, cross_loc_new_all, all_manholes_gdf
    except Exception as e:
        raise e from None
