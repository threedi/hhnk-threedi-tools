import numpy as np
from ....toolbox_universal.queries.tests.sqlite_tests.quick_tests import profiles_used_query
from ....toolbox_universal.sql_interaction.sql_functions import execute_sql_selection
from ....toolbox_universal.dataframe_functions.conversion import convert_df_to_gdf
from ....toolbox_universal.tests.sqlite_tests.variables.dataframes_mapping import primary_col, water_level_width_col, \
    max_depth_col
from ....toolbox_universal.variables.database_aliases import a_zoom_cat
from ....toolbox_universal.variables.database_variables import width_col, height_col, initial_waterlevel_col, \
    reference_level_col
from ....toolbox_universal.read_write_functions.write_to_file_functions import gdf_write_to_csv, gdf_write_to_geopackage

def calc_width_at_waterlevel(row):
    """Bereken de breedte van de watergang op het streefpeil"""
    x_pos = [b / 2 for b in row[width_col]]
    y = [row.reference_level + b for b in row[height_col]]
    ini = row[initial_waterlevel_col]

    # Interpoleer tussen de x en y waarden (let op: de x en y zijn hier verwisseld)
    width_wl = round(np.interp(ini, xp=y, fp=x_pos), 2) * 2
    return str(width_wl)

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
    Gathers information about channel depth and width based on cross_sections and
    connection nodes
    """
    try:
        model_path = test_env.src_paths['model']
        query = profiles_used_query
        channels_df = execute_sql_selection(query=query, database_path=model_path)
        channels_gdf = convert_df_to_gdf(df=channels_df)
        # If zoom category is 4, channel is considered primary
        channels_gdf[primary_col] = channels_gdf[a_zoom_cat].apply(
            lambda zoom_cat: zoom_cat == 4)
        channels_gdf[width_col] = channels_gdf[width_col].apply(split_round)
        channels_gdf[height_col] = channels_gdf[height_col].apply(split_round)
        channels_gdf[water_level_width_col] = channels_gdf.apply(func=calc_width_at_waterlevel, axis=1)
        channels_gdf[max_depth_col] = channels_gdf.apply(func=get_max_depth, axis=1)
        gdf_write_to_csv(channels_gdf,
                         path=test_env.output_vars['log_path'],
                         filename=test_env.output_vars['profiles_used_filename'])
        # Conversion to string because lists are not valid for storing in gpkg
        channels_gdf[width_col] = channels_gdf[width_col].astype(str)
        channels_gdf[height_col] = channels_gdf[height_col].astype(str)
        gdf_write_to_geopackage(channels_gdf,
                                path=test_env.output_vars['layer_path'],
                                filename=test_env.output_vars['profiles_used_filename'])
    except Exception as e:
        raise e from None
