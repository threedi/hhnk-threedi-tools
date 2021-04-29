import os
from .variables.definitions import hydraulic_test_state, one_d_two_d_state, undefined_state, invalid_path
from ...sql_interaction.sql_functions import execute_sql_selection
from ...queries.query_functions import construct_select_query
from ...variables.database_variables import global_settings_layer, control_group_col

def detect_model_states(model_path):
    """
    Detects the current state of the model by checking:
    1. How many entries global_settings has (4 if 1d2d state, 1 if hydraulic test state)
    2. Whether control of controlled structs is on (on for 1d2d, off for hydraulic tests)
    """
    def is_unique(column):
        a = column.to_numpy()
        return (a[0] == a).all()
    if model_path is None or not os.path.exists(model_path):
        return invalid_path
    try:
        global_settings_df = execute_sql_selection(query=construct_select_query(table=global_settings_layer),
                                                   database_path=model_path)
        if not global_settings_df.empty:
            number_of_rows = global_settings_df.shape[0]
            control_group_value_unique = is_unique(global_settings_df[control_group_col])
            if control_group_value_unique and \
                    not global_settings_df[control_group_col].iloc[0] \
                    and number_of_rows == 1:
                return hydraulic_test_state
            elif global_settings_df[control_group_col].notnull().all() \
                    and number_of_rows == 4:
                return one_d_two_d_state
        return undefined_state
    except Exception as e:
        raise e from None
