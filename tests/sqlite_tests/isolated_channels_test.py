from ....toolbox_universal.queries.tests.sqlite_tests.quick_tests_selection_queries import isolated_channels_query
from ....toolbox_universal.sql_interaction.sql_functions import execute_sql_selection
from ....toolbox_universal.dataframe_functions.conversion import convert_df_to_gdf
from ....toolbox_universal.variables.database_variables import calculation_type_col
from ....toolbox_universal.variables.definitions import channels_isolated_calc_type
from ....toolbox_universal.variables.database_aliases import df_geo_col
from ....toolbox_universal.tests.sqlite_tests.variables.dataframes_mapping import length_in_meters_col

def calc_len_percentage(channels_gdf):
    total_length = round(channels_gdf.geometry.length.sum() / 1000, 2)
    isolated_channels_gdf = channels_gdf[channels_gdf[calculation_type_col] ==
                                         channels_isolated_calc_type]
    if not isolated_channels_gdf.empty:
        isolated_length = round(isolated_channels_gdf.geometry.length.sum() / 1000, 2)
    else:
        isolated_length = 0
    percentage = round((isolated_length / total_length) * 100, 0)
    return isolated_channels_gdf, isolated_length, total_length, percentage

def get_isolated_channels(test_env):
    """
    Test bepaalt welke watergangen niet zijn aangesloten op de rest van de watergangen. Deze watergangen worden niet
    meegenomen in de uitwisseling in het watersysteem. De test berekent teven de totale lengte van watergangen en welk
    deel daarvan geisoleerd is.
    """
    try:
        model_path = test_env.src_paths['model']
        channels_df = execute_sql_selection(query=isolated_channels_query,
                                            database_path=model_path)
        channels_gdf = convert_df_to_gdf(df=channels_df)
        channels_gdf[length_in_meters_col] = (round(channels_gdf[df_geo_col].length, 2))
        isolated_channels_gdf, isolated_length, total_length, percentage = calc_len_percentage(channels_gdf)
        result = f"Totale lengte watergangen {total_length} km\n" \
                 f"Totale lengte geisoleerde watergangen {isolated_length} km\n" \
                 f"Percentage geisoleerde watergangen {percentage}%\n"
        return isolated_channels_gdf, result
    except Exception as e:
        raise e from None
