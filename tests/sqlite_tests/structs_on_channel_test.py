import geopandas as gpd
from hhnk_threedi_tools.dataframe_functions.conversion import gdf_from_sql
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import struct_channel_bed_query
from ...variables.database_aliases import a_chan_bed_struct_id, a_chan_bed_struct_code
from hhnk_threedi_tools.variables.definitions import OPEN_FILE_GDB_DRIVER
from .variables.dataframes_mapping import down_has_assumption, up_has_assumption, \
    height_inner_lower_down, height_inner_lower_up, datachecker_assumption_alias

damo_fields = ['CODE', 'HOOGTEBINNENONDERKANTBENE', 'HOOGTEBINNENONDERKANTBOV']
damo_link_on = 'CODE'
datachecker_fields = ['code', 'aanname']
datachecker_link_on = 'code'
datachecker_assumption_field = 'aanname'

def add_damo_info(damo_path, layer, gdf):
    try:
        damo_gdb = gpd.read_file(damo_path,
                                 driver=OPEN_FILE_GDB_DRIVER,
                                 layer=layer)
        new_gdf = gdf.merge(damo_gdb[damo_fields],
                            how='left', left_on=a_chan_bed_struct_code, right_on=damo_link_on)
        new_gdf.rename(columns={'HOOGTEBINNENONDERKANTBENE': height_inner_lower_down,
                                'HOOGTEBINNENONDERKANTBOV': height_inner_lower_up,
                                'CODE': 'damo_code'}, inplace=True)
    except Exception as e:
        raise e from None
    else:
        return new_gdf

def add_datacheck_info(datachecker_path, layer, gdf):
    try:
        datachecker_gdb = gpd.read_file(datachecker_path,
                                        driver=OPEN_FILE_GDB_DRIVER,
                                        layer=layer)
        new_gdf = gdf.merge(datachecker_gdb[datachecker_fields],
                            how='left',
                            left_on=a_chan_bed_struct_code,
                            right_on=datachecker_link_on)
        new_gdf.rename(columns={datachecker_assumption_field: datachecker_assumption_alias}, inplace=True)
    except Exception as e:
        raise e from None
    else:
        return new_gdf

def check_struct_channel_bed_level(test_env):
    """
    Checks whether the reference level of any of the adjacent cross section locations (channels) to a structure
    is lower than the reference level for that structure (3di crashes if it is)
    """
    damo_path = test_env.src_paths['damo']
    datachecker_path = test_env.src_paths['datachecker']
    datachecker_culvert_layer = test_env.src_paths['datachecker_culvert_layer']
    damo_duiker_sifon_layer = test_env.src_paths['damo_duiker_sifon_layer']
    model_path = test_env.src_paths['model']
    try:
        below_ref_query = struct_channel_bed_query
        gdf_below_ref = gdf_from_sql(query=below_ref_query,
                                     id_col=a_chan_bed_struct_id,
                                     database_path=model_path)
        # See git issue about below statements
        gdf_with_damo = add_damo_info(damo_path=damo_path,
                                      layer=damo_duiker_sifon_layer,
                                      gdf=gdf_below_ref)
        gdf_with_datacheck = add_datacheck_info(datachecker_path, datachecker_culvert_layer, gdf_with_damo)
        gdf_with_datacheck.loc[:, down_has_assumption] = (
            gdf_with_datacheck[height_inner_lower_down].isna())
        gdf_with_datacheck.loc[:, up_has_assumption] = (
            gdf_with_datacheck[height_inner_lower_up].isna())
        return gdf_with_datacheck
    except Exception as e:
        raise e from None