import geopandas as gpd
import pandas as pd
import numpy as np
from .import_information_functions import import_information
from ...variables.default_variables import DEF_TRGT_CRS
from ...variables.datachecker_variables import peil_id_col, streefpeil_bwn_col
from ...variables.database_aliases import a_man_id, a_conn_node_id, df_geo_col, a_man_conn_id
from ...variables.database_variables import initial_waterlevel_col, storage_area_col, code_col
from .variables.dataframe_variables import one_d_node_id_col, node_id_col, \
    node_geometry_col, node_type_col, connection_val, storage_area_col, levee_id_col, \
    levee_height_col, type_col, one_d_two_d_crosses_levee_val, one_d_two_d_crosses_fixed, \
    drain_level_col, already_manhole_col, unknown_val, node_in_wrong_fixed_area
from .new_manholes_prepare_model_insertion import dataframe_from_new_manholes

def get_intersections(fixeddrainage_line, lines_1d2d, levee_line_gdf):
    try:
        # Find intersection of 1d2d calculation grid lines with levees.
        calc_node_intersects_levee = gpd.sjoin(lines_1d2d, levee_line_gdf)
        calc_node_intersects_levee[type_col] = one_d_two_d_crosses_levee_val
        calc_node_intersects_levee.drop(['index_right'], axis=1, inplace=True)

        # remove duplicate 1d2d lines, created because multiple levees are crossed. Only the highest levee height is taken.
        calc_node_intersects_levee = calc_node_intersects_levee.sort_values(levee_height_col, ascending=False).drop_duplicates(
            one_d_node_id_col).sort_index()

        # Do the same for intersections with fixeddrainagelevelareas
        calc_node_intersects_fixeddrainage = gpd.sjoin(lines_1d2d, fixeddrainage_line)
        calc_node_intersects_fixeddrainage[type_col] = one_d_two_d_crosses_fixed
        calc_node_intersects_fixeddrainage = calc_node_intersects_fixeddrainage.drop_duplicates(
            one_d_node_id_col).sort_index()

        # Combine intersections with levees and fixeddrainagelevelarea
        calc_node_intersections = pd.concat([calc_node_intersects_levee, calc_node_intersects_fixeddrainage],
                                            ignore_index=False, sort=False)
        # Drop duplicate node id's, keeping highest levee. Levee intersection takes precedence over fixeddrainage intersection
        calc_node_intersections = calc_node_intersections.sort_values([levee_height_col, type_col],
                                                                      ascending=False).drop_duplicates(
            one_d_node_id_col).sort_index()
        return calc_node_intersections
    except Exception as e:
        raise e from None

def get_divergent_nodes(conn_nodes_gdf, fixeddrainage_gdf):
    """Create list of connection nodes that do not have the same initial water level as most nodes in that area.
    These connection nodes are made isolated to avoid leaking over boundaries."""
    try:
        # For all connection nodes, see in which area they are
        # nodes_in_drainage_area
        nodes_with_fixeddrainage_id_gdf = gpd.sjoin(conn_nodes_gdf, fixeddrainage_gdf[
            [peil_id_col, streefpeil_bwn_col, df_geo_col]])

        # initialize new dataframe
        nodes_with_divergent_initial_wtrlvl = gpd.GeoDataFrame()

        # Loop over all nodes, per unique drainage area. Find the mode, and add all connection nodes that do not have an
        # initial waterlevel equal to the mode.
        for p_id in nodes_with_fixeddrainage_id_gdf[peil_id_col].unique():
            nodes_in_same_area = nodes_with_fixeddrainage_id_gdf[nodes_with_fixeddrainage_id_gdf[peil_id_col] == p_id]

            # Find the most occuring value of initial waterlevel, this is considered the initial
            # waterlevel in the area specified by p_id
            init_waterlevel_mode = nodes_in_same_area[initial_waterlevel_col].mode().values[0]

            # Find which nodes have a different waterlevel than the initial waterlevel
            nodes_with_divergent_initial_wtrlvl = nodes_with_divergent_initial_wtrlvl.append(
                nodes_in_same_area[nodes_in_same_area[initial_waterlevel_col] != init_waterlevel_mode], ignore_index=True)

        # Clean up dataframe and add columns so it can be used in the sql creation for manholes on these nodes.
        nodes_with_divergent_initial_wtrlvl = nodes_with_divergent_initial_wtrlvl[
            [a_conn_node_id, initial_waterlevel_col, storage_area_col, df_geo_col]]
        nodes_with_divergent_initial_wtrlvl[drain_level_col] = np.nan
        nodes_with_divergent_initial_wtrlvl[code_col] = nodes_with_divergent_initial_wtrlvl[
            a_conn_node_id].apply(lambda x: f'{a_conn_node_id}_' + str(x))
        nodes_with_divergent_initial_wtrlvl[type_col] = node_in_wrong_fixed_area
        return nodes_with_divergent_initial_wtrlvl
    except Exception as e:
        raise e from None

def get_node_ids_without_manholes(calc_node_intersections, manholes_gdf):
    try:
        conn_nodes_1d2d_flowline = calc_node_intersections[calc_node_intersections[node_type_col] == connection_val]

        # Make a list of all connection node id's that are not yet a manhole, then select columns to keep
        conn_nodes_1d2d_flowline = conn_nodes_1d2d_flowline[~conn_nodes_1d2d_flowline[node_id_col].isin(
            manholes_gdf[a_man_conn_id])][[node_id_col, initial_waterlevel_col, storage_area_col,
                                           levee_height_col, type_col, node_geometry_col]]
        conn_nodes_1d2d_flowline.rename(columns={levee_height_col: drain_level_col, node_geometry_col: df_geo_col},
                                        inplace=True)
        conn_nodes_1d2d_flowline[already_manhole_col] = 0
        # The manholes that have a levee height joined will have to get the levee height as drain level.
        # If this is not known, the nodes will be made isolated. This is done in another script.
        conn_nodes_1d2d_flowline[code_col] = conn_nodes_1d2d_flowline[node_id_col].apply(
            lambda x: f'{node_id_col}_' + str(x))
        return conn_nodes_1d2d_flowline
    except Exception as e:
        raise e from None

def get_manhole_information(calc_node_intersections, nodes_with_divergent_initial_wtrlvl, manholes_gdf):
    """Uses the manhole table from the sqlite and the 1d2d flowlines that originate from a connection node.
    If the connection node is not already a manhole, they are added to the list. This function generates the
    dataframe from which the sql code can be made"""
    try:
        node_ids_without_manholes = get_node_ids_without_manholes(calc_node_intersections, manholes_gdf)
        # Combine the three lists: manholes, connection nodes with 1d2d flowline and connection nodes in wrong area
        # Manholes from model
        all_manholes = manholes_gdf.copy()
        all_manholes[already_manhole_col] = True
        # default manhole type, if manhole is added through other procedure,
        # this script doesnt know why it was added
        all_manholes[type_col] = unknown_val

        # Update current manholes with the type of manhole from intersections.
        all_manholes.set_index(a_man_conn_id, drop=False, inplace=True)
        all_manholes.update(calc_node_intersections.set_index(node_id_col)[type_col])
        # Add new manholes that are not yet in sqlite (rename is needed because of difference in column names)
        all_manholes = all_manholes.append(node_ids_without_manholes.rename(columns={node_id_col: a_man_conn_id}))
        # check if nodes in wrong area (different initial waterlevel than rest in area) don't have manhole yet
        nodes_with_divergent_initial_wtrlvl_no_manhole = \
            nodes_with_divergent_initial_wtrlvl[~nodes_with_divergent_initial_wtrlvl[
                a_conn_node_id].isin(all_manholes[a_man_conn_id])]
        nodes_with_divergent_initial_wtrlvl_no_manhole[already_manhole_col] = False
        # also add these to the list
        all_manholes = all_manholes.append(nodes_with_divergent_initial_wtrlvl_no_manhole, ignore_index=True)
        # Drop duplicates that are introduced by nodes_with_divergent_initial_wtrlvl_no_manhole
        all_manholes = all_manholes.sort_values(
            drain_level_col, ascending=False).drop_duplicates(a_conn_node_id).sort_index()
        all_manholes.reset_index(drop=True, inplace=True)
        all_manholes_gdf = gpd.GeoDataFrame(all_manholes, crs=f'EPSG:{DEF_TRGT_CRS}')
        return all_manholes_gdf
    except Exception as e:
        raise e from None

def add_info_intersecting_1d2d_flowlines(intersect_1d2d_all, lines_1d2d):
    """Create overview of all 1d2d flowlines and add information if these lines cross a levee"""
    try:
        all_intersecting_1d2d_flowlines = intersect_1d2d_all.drop(
            [initial_waterlevel_col, node_geometry_col, storage_area_col], axis=1)
        all_intersecting_1d2d_flowlines = all_intersecting_1d2d_flowlines[
            [one_d_node_id_col, node_id_col, node_type_col, levee_id_col, levee_height_col, type_col, df_geo_col]]

        # Start building overview of all 1d2d lines, then add information of the 1d2d lines that cross certain lines
        lines_1d2d_extra = lines_1d2d.copy()[[df_geo_col, one_d_node_id_col, node_id_col, node_type_col]]

        # combine all 1d2d lines and intersecting lines with levees
        flowlines_suffix = '_drop'
        all_1d2d_flowlines = pd.merge(lines_1d2d_extra.reset_index(drop=True),
                                      all_intersecting_1d2d_flowlines.reset_index(drop=True), on=one_d_node_id_col,
                                      how='left',
                                      suffixes=['', flowlines_suffix])
        all_1d2d_flowlines.drop([f'{node_id_col}{flowlines_suffix}',
                                 f'{node_type_col}{flowlines_suffix}',
                                 f'{df_geo_col}{flowlines_suffix}'],
                                axis=1, inplace=True)
        return all_1d2d_flowlines
    except Exception as e:
        raise e from None

def get_manholes_to_add_to_model(all_manholes):
    """
    Creates dataframe of new manholes to be added to model, formatted for insertion into the model
    """
    try:
        # Nodes with no manhole id need new manholes. Do not create manhole at fixeddrainage.
        new_manholes_df = all_manholes[(all_manholes[a_man_id].isna()) &
                                       (all_manholes[type_col] !=
                                        one_d_two_d_crosses_fixed)].drop(df_geo_col, axis=1)
        new_manholes_for_model = dataframe_from_new_manholes(new_manholes_df)
        return new_manholes_for_model
    except Exception as e:
        raise e from None

def gather_information(test_env):
    """
    Accumulates all needed information for recalculating banklevels
    """
    try:
        manholes_gdf, fixeddrainage_gdf, fixeddrainage_lines, one_d_two_d_lines_gdf, \
        conn_nodes_gdf, channels_gdf, cross_loc_gdf, levee_line_gdf = import_information(test_env)
        calc_node_intersections = get_intersections(fixeddrainage_lines, one_d_two_d_lines_gdf, levee_line_gdf)
        nodes_with_divergent_initial_wtrlvl = get_divergent_nodes(conn_nodes_gdf, fixeddrainage_gdf)
        all_manholes_gdf = get_manhole_information(calc_node_intersections,
                                                   nodes_with_divergent_initial_wtrlvl,
                                                   manholes_gdf)
        all_1d2d_flowlines = add_info_intersecting_1d2d_flowlines(calc_node_intersections, one_d_two_d_lines_gdf)
        new_manholes_df = get_manholes_to_add_to_model(all_manholes_gdf)
        return all_manholes_gdf, new_manholes_df, calc_node_intersections, channels_gdf,\
            cross_loc_gdf, all_1d2d_flowlines
    except Exception as e:
        raise e from None
