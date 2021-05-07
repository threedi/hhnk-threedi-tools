from shapely import wkt
from hhnk_threedi_tools.dataframe_functions.conversion import gdf_from_sql
from ...variables.database_variables import id_col
from ...variables.database_aliases import a_geo_end_coord, a_geo_end_node, a_geo_start_coord, a_geo_start_node
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import geometry_check_query

def add_distance_checks(gdf):
    # Load as valid geometry type
    gdf['start_coord'] = gdf['start_coord'].apply(wkt.loads)
    gdf['start_node'] = gdf['start_node'].apply(wkt.loads)
    gdf['end_coord'] = gdf['end_coord'].apply(wkt.loads)
    gdf['end_node'] = gdf['end_node'].apply(wkt.loads)
    # Set as geometry column (geopandas doesn't support having more than one)
    gdf_start_coor = gdf.set_geometry(col='start_coord')
    gdf_start_node = gdf.set_geometry(col='start_node')
    gdf['start_dist_ok'] = round(gdf_start_node.distance(gdf_start_coor), 5) < 0.1
    gdf_end_coor = gdf.set_geometry(col='end_coord')
    gdf_end_node = gdf.set_geometry(col='end_node')
    gdf['end_dist_ok'] = round(gdf_end_node.distance(gdf_end_coor), 5) < 0.1

def run_geometry_checks(test_env):
    """
    Deze test checkt of de geometrie van een object in het model correspondeert met de start- of end node in de
    v2_connection_nodes tafel. Als de verkeerde ids worden gebruikt geeft dit fouten in het model.
    """
    try:
        query = geometry_check_query
        model_path = test_env.src_paths['model']
        gdf = gdf_from_sql(query=query, database_path=model_path, id_col=id_col)
        gdf['start_check'] = gdf[a_geo_start_node] == gdf[a_geo_start_coord]
        gdf['end_check'] = gdf[a_geo_end_node] == gdf[a_geo_end_coord]
        add_distance_checks(gdf)
        # Only rows where at least one of start_dist_ok and end_dist_ok is false
        result_db = gdf[~gdf[['start_dist_ok', 'end_dist_ok']].all(axis=1)]
        if not result_db.empty:
            result_db['error'] = "Error: mismatched geometry"
        return result_db
    except Exception as e:
        raise e from None
