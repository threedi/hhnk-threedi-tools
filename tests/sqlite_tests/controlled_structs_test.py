import geopandas as gpd
from ...sql_interaction.sql_functions import execute_sql_selection
from ...dataframe_functions.conversion import convert_df_to_gdf
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import controlled_structures_query
from ...variables.database_variables import target_type_col, action_col, weir_layer, code_col
from ...variables.definitions import OPEN_FILE_GDB_DRIVER

start_action = 'start_action_value'
min_action = 'min_action_value'
max_action = 'max_action_value'
hdb_kruin_min = 'hdb_kruin_min'
hdb_kruin_max = 'hdb_kruin_max'
hdb_streefpeil = 'hdb_streefpeil'

def get_action_values(row):
    if row[target_type_col] is weir_layer:
        action_values = [float(b.split(';')[1]) for b in row[action_col].split('#')]
    else:
        action_values = [float(b.split(';')[1].split(' ')[0]) for b in row[action_col].split('#')]
    return action_values[0], min(action_values), max(action_values)

def check_controlled_structures(test_env):
    '''
    Adds information about structure control to map
    '''
    hdb_path = test_env.src_paths['hdb']
    hdb_layer = test_env.src_paths['hdb_sturing_3di_layer']
    model_path = test_env.src_paths['model']
    try:
        model_control_db = execute_sql_selection(query=controlled_structures_query, database_path=model_path)
        model_control_gdf = convert_df_to_gdf(df=model_control_db)
        model_control_gdf[[start_action, min_action, max_action]] = model_control_gdf.apply(
            get_action_values, axis=1, result_type='expand')
        hdb_stuw_gdf = gpd.read_file(hdb_path,
                                     driver=OPEN_FILE_GDB_DRIVER,
                                     layer=hdb_layer)[['CODE', 'STREEFPEIL', 'MIN_KRUINHOOGTE', 'MAX_KRUINHOOGTE']]
        hdb_stuw_gdf.rename(columns={'CODE': code_col,
                                     'STREEFPEIL': hdb_streefpeil,
                                     'MIN_KRUINHOOGTE': hdb_kruin_min,
                                     'MAX_KRUINHOOGTE': hdb_kruin_max}, inplace=True)
        control_final = model_control_gdf.merge(hdb_stuw_gdf, on=code_col, how='left')
        return control_final
    except Exception as e:
        raise e from None
