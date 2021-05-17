import os
from .variables.definitions import hydraulic_test_state, one_d_two_d_state, undefined_state, \
    invalid_path, zero_d_one_d_name
from hhnk_research_tools.sql_interaction.sql_functions import execute_sql_selection, table_exists
from hhnk_research_tools.query_functions import construct_select_query
from ...variables.database_variables import global_settings_layer, control_group_col, name_col
from ...variables.backups_table_names import GLOBAL_SETTINGS_TABLE

def detect_model_states(model_path):
    """
    Detects the current state of the model by checking:
    It a backup of v2_global settings exists (if not, we assume an undefined state (possibly
    state right after modelbuilder)

    If so we check:
    1. How many entries global_settings has
        If 4: if none of the control_group_id column is NULL and none of name column is '0d1d_test'
        --> 1d2d state
        If 1: if control_group_id column is NULL and name column is '0d1d_test'
        --> Hydraulic test state
    In all other cases, we return undefined
    """
    def is_unique(column):
        a = column.to_numpy()
        return (a[0] == a).all()
    if model_path is None or not os.path.exists(model_path):
        return invalid_path
    try:
        global_settings_df = execute_sql_selection(query=construct_select_query(table=global_settings_layer),
                                                   database_path=model_path)
        global_settings_backup_exists = table_exists(database_path=model_path, table_name=GLOBAL_SETTINGS_TABLE)
        if not global_settings_df.empty and global_settings_backup_exists:
            number_of_rows = global_settings_df.shape[0]
            control_group_value_unique = is_unique(global_settings_df[control_group_col])
            if control_group_value_unique and \
                    not global_settings_df[control_group_col].iloc[0] \
                    and number_of_rows == 1 \
                    and global_settings_df[name_col].iloc[0] == zero_d_one_d_name:
                return hydraulic_test_state
            elif global_settings_df[control_group_col].notnull().all() \
                    and number_of_rows == 4 \
                    and global_settings_df[global_settings_df[name_col] == zero_d_one_d_name].empty:
                return one_d_two_d_state
        return undefined_state
    except Exception as e:
        raise e from None
