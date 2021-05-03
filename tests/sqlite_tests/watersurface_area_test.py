import geopandas as gpd
from ...variables.datachecker_variables import peil_id_col, streefpeil_bwn_col, code_col, geometry_col
from ...variables.types import GPKG
from ...variables.definitions import ESRI_DRIVER, WKT
from ...dataframe_functions.conversion import gdf_from_sql
from ...variables.database_aliases import a_watersurf_conn_id
from ...variables.database_variables import storage_area_col
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import watersurface_conn_node_query
from .variables.dataframes_mapping import watersurface_nodes_area, watersurface_waterdeel_area, \
    watersurface_channels_area, watersurface_model_area, area_diff_col, area_diff_perc

def expand_multipolygon(df):
    """
    New version using explode, old version returned pandas dataframe not geopandas
    geodataframe (missing last line), I think it works now?
    """
    try:
        exploded = df.set_index([peil_id_col])[geometry_col]
        exploded = exploded.explode()
        exploded = exploded.reset_index()
        exploded = exploded.rename(columns={0: geometry_col, 'level_1': 'multipolygon_level'})
        merged = exploded.merge(df.drop(geometry_col, axis=1), left_on=peil_id_col, right_on=peil_id_col)
        merged = merged.set_geometry(geometry_col, crs=df.crs)
        return merged
    except Exception as e:
        raise e from None

def read_input(database_path, datachecker_path, channel_profile_path, damo_path,
               datachecker_layer, damo_layer):
    try:
        fixeddrainage = gpd.read_file(datachecker_path,
                                      layer=datachecker_layer,
                                      reader=GPKG)[[peil_id_col, code_col, streefpeil_bwn_col, geometry_col]]
        fixeddrainage = expand_multipolygon(fixeddrainage)
        modelbuilder_waterdeel = gpd.read_file(channel_profile_path, driver=ESRI_DRIVER)
        damo_waterdeel = gpd.read_file(damo_path,
                                       layer=damo_layer,
                                       reader=GPKG)
        conn_nodes_geo = gdf_from_sql(query=watersurface_conn_node_query,
                                      id_col=a_watersurf_conn_id,
                                      database_path=database_path)
        return fixeddrainage, modelbuilder_waterdeel, damo_waterdeel, conn_nodes_geo
    except Exception as e:
        raise e from None

def add_nodes_area(fixeddrainage, conn_nodes_geo):
    try:
        # join on intersection of geometries
        joined = gpd.sjoin(
            fixeddrainage, conn_nodes_geo, how='left', op='intersects', lsuffix='fd', rsuffix='conn')
        # Combine all rows with same peil_id and multipolygon level and sum their area
        group = joined.groupby([peil_id_col, 'multipolygon_level'])[storage_area_col].sum()
        # Add the aggregated area column to the original dataframe
        fixeddrainage = fixeddrainage.merge(group, how='left', on=[peil_id_col, 'multipolygon_level'])
        fixeddrainage.rename(columns={storage_area_col: watersurface_nodes_area}, inplace=True)
        return fixeddrainage
    except Exception as e:
        raise e from None

def add_waterdeel(fixeddrainage, to_add):
    try:
        # create dataframe containing overlaying geometry
        overl = gpd.overlay(fixeddrainage, to_add, how='intersection')
        # add column containing size of overlaying areas
        overl['area'] = overl[geometry_col].area
        # group overlaying area gdf by id's
        overl = overl.groupby([peil_id_col, 'multipolygon_level'])['area'].sum()
        # merge overlapping area size into fixeddrainage
        merged = fixeddrainage.merge(overl, how='left', on=[peil_id_col, 'multipolygon_level'])
        merged['area'] = round(merged['area'], 0)
        merged['area'] = merged['area'].fillna(0)
    except Exception as e:
        raise e from None
    return merged

def calc_perc(diff, waterdeel):
    try:
        return round((diff / waterdeel) * 100, 1)
    except:
        if diff == waterdeel:
            return 0.0
        else:
           return 100.0

def calc_area(fixeddrainage, modelbuilder_waterdeel, damo_waterdeel, conn_nodes_geo):
    try:
        fixeddrainage = add_nodes_area(fixeddrainage, conn_nodes_geo)
        fixeddrainage = add_waterdeel(fixeddrainage, damo_waterdeel)
        fixeddrainage.rename(columns={'area': watersurface_waterdeel_area}, inplace=True)
        fixeddrainage = add_waterdeel(fixeddrainage, modelbuilder_waterdeel)
        fixeddrainage.rename(columns={'area': watersurface_channels_area}, inplace=True)
        fixeddrainage[watersurface_model_area] = fixeddrainage[watersurface_channels_area] + fixeddrainage[watersurface_nodes_area]
        fixeddrainage[area_diff_col] = fixeddrainage[watersurface_model_area] - fixeddrainage[watersurface_waterdeel_area]
        fixeddrainage[area_diff_perc] = fixeddrainage.apply(
            lambda row: calc_perc(row[area_diff_col], row[watersurface_waterdeel_area]), axis=1)
        return fixeddrainage
    # This may be too restrictive
    except Exception as e:
        raise e from None

def calc_watersurface_area(test_env):
    datachecker_path = test_env.src_paths['datachecker']
    channel_from_profile = test_env.src_paths['channels_shapefile']
    datachecker_layer = test_env.src_paths['datachecker_fixed_drainage']
    damo_path = test_env.src_paths['damo']
    damo_layer = test_env.src_paths['damo_waterdeel_layer']
    try:
        fixeddrainage, modelbuilder_waterdeel, damo_waterdeel, conn_nodes_geo = read_input(
            database_path=test_env.src_paths['model'],
            datachecker_path=datachecker_path,
            channel_profile_path=channel_from_profile,
            damo_path=damo_path,
            datachecker_layer=datachecker_layer,
            damo_layer=damo_layer
        )
        fixeddrainage = calc_area(fixeddrainage, modelbuilder_waterdeel,
                                  damo_waterdeel, conn_nodes_geo)
        result_txt = """Gebied open water BGT: {} ha\nGebied open water model: {} ha""".format(
            round(fixeddrainage.sum()[watersurface_waterdeel_area] / 10000, 2),
            round(fixeddrainage.sum()[watersurface_model_area] / 10000, 2))
        return fixeddrainage, result_txt
    except Exception as e:
        raise e from None
