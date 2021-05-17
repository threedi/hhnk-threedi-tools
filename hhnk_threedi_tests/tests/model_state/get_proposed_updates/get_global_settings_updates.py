import pandas as pd
from ..variables.definitions import hydraulic_test_state, one_d_two_d_state, undefined_state
from hhnk_threedi_tools.sql_interaction.sql_functions import execute_sql_selection
from hhnk_threedi_tools.query_functions import construct_select_query
from ....queries.model_states.read_backups_queries import create_global_settings_from_backup_query
from ....variables.database_variables import zero_d_one_d_val, global_settings_layer, id_col, \
    name_col, control_group_col
from ..variables.new_columns_mapping import global_settings_new_col_name

def get_rows_to_add(model_path, to_state, rows_in_model_df, id_column):
    """
    Gets all rows that should be in the new model state from the backup, checks
    which ones are already in the model, returns list of the ones that are not
    """
    try:
        query = create_global_settings_from_backup_query(to_state=to_state)
        rows_should_be_in_model = execute_sql_selection(query=query, database_path=model_path)
        return rows_should_be_in_model.loc[~rows_should_be_in_model[id_column].isin(
            rows_in_model_df[id_column].tolist())]
    except Exception as e:
        raise e from None

def get_rows_to_delete(rows_in_model_df, to_state, selection_col, id_column):
    """
    Selects ids that are currently in the model and selects which ones
    should be deleted based on the new model state
    """
    delete_ids = []
    if to_state == hydraulic_test_state:
        delete_ids = rows_in_model_df[rows_in_model_df[selection_col] != zero_d_one_d_val][id_column].tolist()
    elif to_state == one_d_two_d_state:
        delete_ids = rows_in_model_df[rows_in_model_df[selection_col] == zero_d_one_d_val][id_column].tolist()
    return delete_ids

def get_global_settings_model(model_path):
    try:
        query = construct_select_query(table=global_settings_layer)
        global_settings_model = execute_sql_selection(query=query, database_path=model_path)
        return global_settings_model
    except Exception as e:
        raise e from None

def get_proposed_adjustments_global_settings(test_env):
    """
    Creates widget to display proposed changes to global settings
    If current state is modelbuilder -> delete rows and turn model control on or off based on state to convert to
    If current state is hydraulic test, delete all rows, add rows from backup, make sure model control is on
    If current state is 1d2d test, delete all rows, add rows from backup, make sure model control is off
    """
    try:
        model_path = test_env.src_paths['model']
        to_state = test_env.conversion_vars.to_state
        rows_in_model = get_global_settings_model(model_path=model_path)
        rows_to_add = get_rows_to_add(model_path=model_path,
                                      to_state=to_state,
                                      rows_in_model_df=rows_in_model,
                                      id_column=id_col)
        rows_to_delete = get_rows_to_delete(rows_in_model_df=rows_in_model,
                                            to_state=to_state,
                                            selection_col=name_col,
                                            id_column=id_col)
        preview_df = pd.concat([rows_in_model, rows_to_add])[[id_col, name_col, control_group_col]]
        if to_state == hydraulic_test_state:
            new_value = None
        elif to_state == one_d_two_d_state:
            new_value = 1
        else:
            raise Exception("No new state defined (or unknown)")
        preview_df.insert(preview_df.columns.get_loc(control_group_col) + 1, global_settings_new_col_name, new_value)
        return preview_df, rows_to_delete, rows_to_add[id_col].tolist()
    except Exception as e:
        raise e from None
