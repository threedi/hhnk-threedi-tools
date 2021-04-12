import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.wkt as wkt
from ...variables.definitions import GPKG_DRIVER
from ...variables.default_variables import DEF_TRGT_CRS
from ...variables.types import UTF8
from ...variables.datachecker_variables import peil_id_col, streefpeil_bwn_col
from ...variables.database_aliases import a_man_id, a_chan_id, a_cross_loc_id, a_conn_node_id, \
    df_geo_col, a_man_conn_id
from ...variables.database_variables import initial_waterlevel_col, storage_area_col, code_col
from ...sql_interaction.sql_functions import create_sqlite_connection
from ...dataframe_functions.conversion import gdf_from_sql
from ...wsa.conversion_functions import line_geometries_to_coords
from ...threedi.variables.results_mapping import one_d_two_d, one_d_node_id_col, node_id_col, \
    node_geometry_col, node_type_col, connection_val, init_wlevel_col, added_calc_val, \
    storage_area_col, levee_id_col, levee_height_col, type_col, one_d_two_d_crosses_levee_val, \
    one_d_two_d_crosses_fixed, drain_level_col, already_manhole_col, unknown_val, \
    node_in_wrong_fixed_area
from .geometry_conversions import point_geometries_to_wkt, extract_boundary_from_polygon
from ...queries.tests.bank_levels_test.gather_information_queries import manholes_query, \
    channels_query, cross_section_location_query, conn_nodes_query

def read_1d2d_lines(results):
    """Uitlezen 1d2d lijnelementen
    Alle 1d2d lijnelementen in het model.
    """
    try:
        # Creates geodataframe with geometries of 1d2d subset of nodes in 3di results
        coords = line_geometries_to_coords(results.lines.subset(one_d_two_d).line_geometries)
        onetwo_line_geo = gpd.GeoDataFrame(geometry=coords, crs=f'EPSG:{DEF_TRGT_CRS}')

        # 1d nodes om te bepalen bij welk kunstwerk het hoort
        onetwo_line_geo[one_d_node_id_col] = [a[1] for a in results.lines.subset(one_d_two_d).line_nodes]
        onetwo_line_geo[node_id_col] = results.nodes.filter(
            id__in=onetwo_line_geo[one_d_node_id_col].tolist()).content_pk
        onetwo_line_geo.index = onetwo_line_geo[one_d_node_id_col]

        # Filter results
        oned_nodes_list = results.nodes.filter(id__in=onetwo_line_geo[one_d_node_id_col].tolist())
        oned_conn_nodes_id_list = oned_nodes_list.connectionnodes.id.tolist()
        oned_conn_nodes_init_wlvl_list = oned_nodes_list.connectionnodes.initial_waterlevel.tolist()
        oned_conn_nodes_storage_area_list = oned_nodes_list.connectionnodes.storage_area
        oned_added_calculation_nodes_list = oned_nodes_list.added_calculationnodes.id.tolist()

        # Add node geometries
        onetwo_line_geo[node_geometry_col] = point_geometries_to_wkt(oned_nodes_list.coordinates)

        # Add information about node type
        onetwo_line_geo.loc[
            onetwo_line_geo[one_d_node_id_col].isin(oned_conn_nodes_id_list), node_type_col] = connection_val
        onetwo_line_geo.loc[
            onetwo_line_geo[one_d_node_id_col].isin(oned_added_calculation_nodes_list), node_type_col] = added_calc_val

        # Add initial waterlevel to nodes
        onetwo_line_geo.loc[
            onetwo_line_geo[one_d_node_id_col].isin(oned_conn_nodes_id_list), init_wlevel_col] = oned_conn_nodes_init_wlvl_list

        # Add storage area from connection nodes to the table
        storage_area_lst = [a.decode(UTF8) for a in oned_conn_nodes_storage_area_list]
        onetwo_line_geo.loc[
            onetwo_line_geo[one_d_node_id_col].isin(oned_conn_nodes_id_list), storage_area_col] = storage_area_lst
        onetwo_line_geo[storage_area_col] = pd.to_numeric(onetwo_line_geo[storage_area_col])
        return onetwo_line_geo
    except Exception as e:
        raise e from None

def import_levees(results):
    def levees_to_linestring(levee_geom):
        try:
            levee_linestr = []
            for line in levee_geom:
                line.FlattenTo2D()  # Er staat nog een hoogte opgeslagen in de levee van 0. Deze wordt verwijderd.
                levee_linestr.append(wkt.loads(line.ExportToWkt()))
            return levee_linestr
        except Exception as e:
            raise e from None

    try:
        levee_line = levees_to_linestring(results.levees.geoms)
        levee_line_geo = gpd.GeoDataFrame(geometry=levee_line, crs=f'EPSG:{DEF_TRGT_CRS}')
        levee_line_geo[levee_id_col] = results.levees.id
        levee_line_geo[levee_height_col] = results.levees.crest_level
        levee_line_geo.index = levee_line_geo[levee_id_col]
        return levee_line_geo
    except Exception as e:
        raise e from None

# def import_manhole(conn, conn_nodes_geo):
#     try:
#         manhole = gdf_from_sql(conn=conn,
#                                query=manholes_query,
#                                id_col=a_man_id,
#                                to_gdf=False)
#         manhole = pd.merge(manhole, conn_nodes_geo[[initial_waterlevel_col, storage_area_col, df_geo_col]],
#                            left_on=a_man_conn_id,
#                            right_on=a_conn_node_id)
#         return manhole
#     except Exception as e:
#         raise e from None

def get_intersections(fixeddrainage_line, lines_1d2d, levee_line_geo):
    #Find intersection of lines.
    intersect_1d2d_levee = gpd.sjoin(lines_1d2d, levee_line_geo)
    intersect_1d2d_levee[type_col] = one_d_two_d_crosses_levee_val
    intersect_1d2d_levee.drop(['index_right'], axis=1, inplace=True)

    # remove duplicate 1d2d lines, created because multiple levees are crossed. Only the highest levee height is taken.
    intersect_1d2d_levee = intersect_1d2d_levee.sort_values(levee_height_col, ascending=False).drop_duplicates(
        one_d_node_id_col).sort_index()

    # Do the same for intersections with fixeddrainagelevelareas
    intersect_1d2d_fixed = gpd.sjoin(lines_1d2d, fixeddrainage_line)
    intersect_1d2d_fixed[type_col] = one_d_two_d_crosses_fixed
    intersect_1d2d_fixed = intersect_1d2d_fixed.drop_duplicates(one_d_node_id_col).sort_index()

    # Combine intersections with levees and fixeddrainagelevelarea
    intersect_1d2d_all = pd.concat([intersect_1d2d_levee, intersect_1d2d_fixed], ignore_index=False, sort=False)
    # Drop duplicate node id's, keeping highest levee. What about fixeddrainage?
    intersect_1d2d_all = intersect_1d2d_all.sort_values([levee_height_col, type_col], ascending=False).drop_duplicates(
        one_d_node_id_col).sort_index()
    return intersect_1d2d_levee, intersect_1d2d_fixed, intersect_1d2d_all

def get_nodes_in_wrong_area(conn_nodes_geo, fixeddrainage, lines_1d2d):
    """Create list of connection nodes that do not have the same initial waterlevel as most nodes in that area.
    These connection nodes are made isolated to avoid leaking over boundaries."""
    # For all connection nodes, see in which area they are
    nodes_in_drainage_area = gpd.sjoin(conn_nodes_geo, fixeddrainage[[peil_id_col, streefpeil_bwn_col, df_geo_col]])

    # initialize new dataframe
    nodes_in_wrong_area = nodes_in_drainage_area[0:0]

    # Loop over all nodes, per unique drainage area. Find the mode, and add all connection nodes that do not have an
    # initial waterlevel equal to the mode.
    for p_id in nodes_in_drainage_area[peil_id_col].unique():
        nodes_in_same_area = nodes_in_drainage_area[nodes_in_drainage_area[peil_id_col] == p_id]

        # Find the most occuring value of initial waterlevel, this is considered the initial waterlevel in the drainage area
        init_waterlevel = nodes_in_same_area[initial_waterlevel_col].mode().values[0]

        # Find which nodes have a different waterlevel than the initial waterlevel
        nodes_in_wrong_area = nodes_in_wrong_area.append(
            nodes_in_same_area[nodes_in_same_area[initial_waterlevel_col] != init_waterlevel], ignore_index=True)

    # Clean up dataframe and add columns so it can be used in the sql creation for manholes on these nodes.
    nodes_in_wrong_area = nodes_in_wrong_area[[a_conn_node_id, initial_waterlevel_col, storage_area_col, df_geo_col]]
    nodes_in_wrong_area[drain_level_col] = np.nan
    nodes_in_wrong_area[code_col] = nodes_in_wrong_area[a_conn_node_id].apply(lambda x: f'{a_conn_node_id}_' + str(x))
    nodes_in_wrong_area[type_col] = node_in_wrong_fixed_area

    # Check if manhole is already present.
    # Find connection node id's that have a manhole
    #     nodes_with_manhole = manhole[manhole['node_id'].isin(nodes_in_wrong_area['node_id'].tolist())]['node_id'].tolist()

    #     # Filter connection nodes that do not have a manhole yet.
    #     nodes_without_manhole = nodes_in_wrong_area[~nodes_in_wrong_area['node_id'].isin(nodes_with_manhole)]

    #     # Check if 1d2d line is present (otherwise manhole isnt needed)
    #     nodes_in_wrong_area = nodes_without_manhole[nodes_without_manhole['node_id'].isin(lines_1d2d['node_id'])]
    return nodes_in_wrong_area

def get_all_manhole_nodes(intersect_1d2d_all, nodes_in_wrong_area, manhole):
    """Uses the manhole table from the sqlite and the 1d2d flowlines that originate from a connection node.
    If the connection node is not already a manhole, they are added to the list. This function generates the
    dataframe from which the sql code can be made"""
    conn_nodes_1d2d_flowline = intersect_1d2d_all[intersect_1d2d_all[node_type_col] == connection_val]

    # Make a list of all connection node id's that are not yet a manhole
    conn_nodes_1d2d_flowline = conn_nodes_1d2d_flowline[~conn_nodes_1d2d_flowline[node_id_col].isin(manhole[a_man_conn_id])][
        [node_id_col, initial_waterlevel_col, storage_area_col, levee_height_col, type_col, node_geometry_col]]
    conn_nodes_1d2d_flowline.rename(columns={levee_height_col: drain_level_col, node_geometry_col: df_geo_col}, inplace=True)
    conn_nodes_1d2d_flowline[already_manhole_col] = 0

    # The manholes that have a levee height joined will have to get the levee height as drain level.
    # If this is not known, the nodes will be made isolated. This is done in another script.
    conn_nodes_1d2d_flowline[code_col] = conn_nodes_1d2d_flowline[node_id_col].apply(
        lambda x: f'{node_id_col}_' + str(x))

    # Combine the three lists: manholes, connection nodes with 1d2d flowline and connection nodes in wrong area
    all_manholes = manhole.copy()
    all_manholes[already_manhole_col] = 1
    # default manhole type, if manhole is added through other procedure,
    # this script doesnt know why it was added
    all_manholes[type_col] = unknown_val

    # Update current manholes with the type of manhole.
    all_manholes.set_index(a_man_conn_id, drop=False, inplace=True)
    all_manholes.update(intersect_1d2d_all.set_index(node_id_col)[type_col])

    # # Add new manholes that are not yet in sqlite
    all_manholes = all_manholes.append(conn_nodes_1d2d_flowline)

    # check if nodes in wrong area dont have manhole yet
    nodes_in_wrong_area_no_manhole = nodes_in_wrong_area[~nodes_in_wrong_area[a_conn_node_id].isin(all_manholes[a_man_conn_id])]
    nodes_in_wrong_area_no_manhole[already_manhole_col] = False

    # also add these to the list
    all_manholes = all_manholes.append(nodes_in_wrong_area_no_manhole, ignore_index=True)

    # Drop duplicates that are introduced by nodes_in_wrong_area
    all_manholes = all_manholes.sort_values(drain_level_col, ascending=False).drop_duplicates(a_conn_node_id).sort_index()
    all_manholes.reset_index(drop=True, inplace=True)  # Reset index

    all_manholes = gpd.GeoDataFrame(all_manholes, crs=f'EPSG:{DEF_TRGT_CRS}')
    return all_manholes

def create_all_intersecting_1d2d_flowlines(intersect_1d2d_all, lines_1d2d):
    """Create overview of all 1d2d flowlines and add information if these lines cross a levee"""
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

def gather_information(test_env):
    """
    Accumulates all needed information for recalculating banklevels
    """
    threedi_results = test_env.threedi_vars.result
    model_path = test_env.src_paths['model']
    datachecker_path = test_env.src_paths['datachecker']
    fixeddrainage_layer = test_env.src_paths['datachecker_fixed_drainage']
    conn = None
    try:
        conn = create_sqlite_connection(database_path=model_path)
        fixeddrainage = gpd.read_file(datachecker_path,
                                      layer=fixeddrainage_layer,
                                      reader=GPKG_DRIVER)
        fixeddrainage_lines = extract_boundary_from_polygon(fixeddrainage)
        levee_line_geo = import_levees(threedi_results)
        lines_1d2d = read_1d2d_lines(threedi_results)
        channel_line_geo = gdf_from_sql(conn=conn, query=channels_query, id_col=a_chan_id)
        cross_loc = gdf_from_sql(conn=conn, query=cross_section_location_query, id_col=a_cross_loc_id)
        conn_nodes_geo = gdf_from_sql(conn=conn, query=conn_nodes_query, id_col=a_conn_node_id)
        manholes = gdf_from_sql(conn=conn, query=manholes_query, id_col=a_man_id)
        # manholes = import_manhole(test_env, conn_nodes_geo)
        intersect_1d2d_levee, intersect_1d2d_fixed, intersect_1d2d_all = get_intersections(fixeddrainage_lines,
                                                                                           lines_1d2d,
                                                                                           levee_line_geo)
        nodes_in_wrong_area = get_nodes_in_wrong_area(conn_nodes_geo, fixeddrainage, lines_1d2d)
        all_manholes = get_all_manhole_nodes(intersect_1d2d_all, nodes_in_wrong_area, manholes)
        all_1d2d_flowlines = create_all_intersecting_1d2d_flowlines(intersect_1d2d_all, lines_1d2d)
        return all_manholes, intersect_1d2d_all, channel_line_geo, cross_loc, all_1d2d_flowlines
    except Exception as e:
        raise e from None
    finally:
        if conn:
            conn.close()
