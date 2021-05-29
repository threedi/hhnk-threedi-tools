import numpy as np
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import profiles_used_query
import hhnk_research_tools as hrt
from ...tests.sqlite_tests.variables.dataframes_mapping import primary_col, water_level_width_col, \
    max_depth_col
from ...variables.database_aliases import a_zoom_cat
from ...variables.database_variables import width_col, height_col, initial_waterlevel_col, \
    reference_level_col

def calc_width_at_waterlevel(row):
    """Bereken de breedte van de watergang op het streefpeil"""
    x_pos = [b / 2 for b in row[width_col]]
    y = [row.reference_level + b for b in row[height_col]]
    ini = row[initial_waterlevel_col]

    # Interpoleer tussen de x en y waarden (let op: de x en y zijn hier verwisseld)
    width_wl = round(np.interp(ini, xp=y, fp=x_pos), 2) * 2
    return width_wl

def split_round(item):
    """
    Split items in width and height columns by space, round all items in resulting list and converts to floats
    """
    return [round(float(n), 2) for n in str(item).split(' ')]

def get_max_depth(row):
    """
    calculates difference between initial waterlevel and reference level
    """
    return round(float(row[initial_waterlevel_col]) - float(row[reference_level_col]), 2)

def get_used_profiles(test_env):
    """
    Koppelt de v2_cross_section_definition laag van het model (discrete weergave van de natuurlijke geometrie van de
    watergangen) aan de v2_channel laag (informatie over watergangen in het model). Het resultaat van deze toets is een
    weergave van de breedtes en dieptes van watergangen in het model ter controle.
    """
    try:
        model_path = test_env.src_paths['model']
        #TODO use hrt.sqlite_table_to_gdf instead?
        channels_df = hrt.execute_sql_selection(query=profiles_used_query,
                                            database_path=model_path)
        channels_gdf = hrt.df_convert_to_gdf(df=channels_df)
        # If zoom category is 4, channel is considered primary
        channels_gdf[primary_col] = channels_gdf[a_zoom_cat].apply(
            lambda zoom_cat: zoom_cat == 4)
        channels_gdf[width_col] = channels_gdf[width_col].apply(split_round)
        channels_gdf[height_col] = channels_gdf[height_col].apply(split_round)
        channels_gdf[water_level_width_col] = channels_gdf.apply(func=calc_width_at_waterlevel, axis=1)
        channels_gdf[max_depth_col] = channels_gdf.apply(func=get_max_depth, axis=1)
        # Conversion to string because lists are not valid for storing in gpkg
        channels_gdf[width_col] = channels_gdf[width_col].astype(str)
        channels_gdf[height_col] = channels_gdf[height_col].astype(str)
        return channels_gdf
    except Exception as e:
        raise e from None
