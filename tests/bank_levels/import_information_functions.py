import geopandas as gpd
import pandas as pd
import shapely.wkt as wkt
from .geometry_conversions import extract_boundary_from_polygon, point_geometries_to_wkt
from ...wsa.conversion_functions import line_geometries_to_coords
from ...threedi.variables.results_mapping import one_d_two_d
from ...variables.default_variables import DEF_TRGT_CRS
from ...variables.types import UTF8
from ...sql_interaction.sql_functions import create_sqlite_connection
from ...dataframe_functions.conversion import gdf_from_sql
from ...variables.definitions import GPKG_DRIVER
from ...variables.database_aliases import a_man_id, a_chan_id, a_cross_loc_id, a_conn_node_id
from ...queries.tests.bank_levels_test.gather_information_queries import manholes_query, \
    channels_query, cross_section_location_query, conn_nodes_query
from .variables.dataframe_variables import levee_id_col, levee_height_col, one_d_node_id_col, node_id_col, \
    node_geometry_col, node_type_col, connection_val, added_calc_val, init_wlevel_col, storage_area_col

def read_1d2d_lines(results):
    """Uitlezen 1d2d lijnelementen
    Alle 1d2d lijnelementen in het model.
    """
    try:
        # Creates geodataframe with geometries of 1d2d subset of nodes in 3di results
        coords = line_geometries_to_coords(results.lines.subset(one_d_two_d).line_geometries)
        one_d_two_d_lines_gdf = gpd.GeoDataFrame(geometry=coords, crs=f'EPSG:{DEF_TRGT_CRS}')

        # 1d nodes om te bepalen bij welk kunstwerk het hoort
        one_d_two_d_lines_gdf[one_d_node_id_col] = [a[1] for a in results.lines.subset(one_d_two_d).line_nodes]
        one_d_two_d_lines_gdf[node_id_col] = results.nodes.filter(
            id__in=one_d_two_d_lines_gdf[one_d_node_id_col].tolist()).content_pk
        one_d_two_d_lines_gdf.index = one_d_two_d_lines_gdf[one_d_node_id_col]

        # Get values corresponding to id's in onetwo_line_geo from results and add to dataframe
        oned_nodes_list = results.nodes.filter(id__in=one_d_two_d_lines_gdf[one_d_node_id_col].tolist())
        oned_conn_nodes_id_list = oned_nodes_list.connectionnodes.id.tolist()
        oned_conn_nodes_init_wlvl_list = oned_nodes_list.connectionnodes.initial_waterlevel.tolist()
        oned_conn_nodes_storage_area_list = oned_nodes_list.connectionnodes.storage_area
        oned_added_calculation_nodes_list = oned_nodes_list.added_calculationnodes.id.tolist()

        # Add node geometries
        one_d_two_d_lines_gdf[node_geometry_col] = point_geometries_to_wkt(oned_nodes_list.coordinates)

        # Add information about node type
        one_d_two_d_lines_gdf.loc[
            one_d_two_d_lines_gdf[one_d_node_id_col].isin(oned_conn_nodes_id_list), node_type_col] = connection_val
        one_d_two_d_lines_gdf.loc[
            one_d_two_d_lines_gdf[one_d_node_id_col].isin(oned_added_calculation_nodes_list), node_type_col] = \
            added_calc_val

        # Add initial waterlevel to nodes
        one_d_two_d_lines_gdf.loc[
            one_d_two_d_lines_gdf[one_d_node_id_col].isin(oned_conn_nodes_id_list), init_wlevel_col] = \
            oned_conn_nodes_init_wlvl_list

        # Add storage area from connection nodes to the table
        storage_area_lst = [a.decode(UTF8) for a in oned_conn_nodes_storage_area_list]
        one_d_two_d_lines_gdf.loc[
            one_d_two_d_lines_gdf[one_d_node_id_col].isin(oned_conn_nodes_id_list), storage_area_col] = storage_area_lst
        one_d_two_d_lines_gdf[storage_area_col] = pd.to_numeric(one_d_two_d_lines_gdf[storage_area_col])
        return one_d_two_d_lines_gdf
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

def import_information(test_env):
    """
    Function that gathers all information from the model and datachecker that's needed
    to calculate the new manholes and bank levels
    """
    threedi_results = test_env.threedi_vars.result
    model_path = test_env.src_paths['model']
    datachecker_path = test_env.src_paths['datachecker']
    fixeddrainage_layer = test_env.src_paths['datachecker_fixed_drainage']
    conn = None
    try:
        conn = create_sqlite_connection(database_path=model_path)
        fixeddrainage_gdf = gpd.read_file(datachecker_path,
                                      layer=fixeddrainage_layer,
                                      reader=GPKG_DRIVER)
        fixeddrainage_lines = extract_boundary_from_polygon(fixeddrainage_gdf)
        levee_line_gdf = import_levees(threedi_results)
        one_d_two_d_lines_gdf = read_1d2d_lines(threedi_results)
        channels_gdf = gdf_from_sql(conn=conn, query=channels_query, id_col=a_chan_id)
        cross_loc_gdf = gdf_from_sql(conn=conn, query=cross_section_location_query, id_col=a_cross_loc_id)
        conn_nodes_gdf = gdf_from_sql(conn=conn, query=conn_nodes_query, id_col=a_conn_node_id)
        manholes_gdf = gdf_from_sql(conn=conn, query=manholes_query, id_col=a_man_id)
        return manholes_gdf, fixeddrainage_gdf, fixeddrainage_lines, one_d_two_d_lines_gdf, \
            conn_nodes_gdf, channels_gdf, cross_loc_gdf, levee_line_gdf
    except Exception as e:
        raise e from None
    finally:
        if conn:
            conn.close()
