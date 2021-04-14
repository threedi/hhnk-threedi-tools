from ...variables.database_variables import global_settings_layer, control_group_col, name_col, \
    zero_d_one_d_val, id_col, manhole_layer, calculation_type_col
from ...variables.backups_table_names import GLOBAL_SETTINGS_TABLE
from ...tests.model_state.variables.definitions import one_d_two_d_state, hydraulic_test_state
from ...queries.query_functions import create_update_case_statement
#--------------------------------------------------------------------------------
# Get global settings update query
#--------------------------------------------------------------------------------

def create_global_settings_update_query(to_state, ids_to_add=[], ids_to_delete=[]):
    """
    Add rows from backup, delete rows from model
    Once the correct rows are in the model, set model control
    """
    update_control_query = f"""
    UPDATE {global_settings_layer}
    SET {control_group_col} = {{value}}
    WHERE {name_col} {{operator}} '{zero_d_one_d_val}'
    """

    add_rows_query = f"""
    INSERT INTO {global_settings_layer}
    SELECT *
    FROM {GLOBAL_SETTINGS_TABLE}
    WHERE id in ({{}})
    """

    delete_rows_query = f"""
    DELETE FROM {global_settings_layer}
    WHERE ID IN ({{}})
    """
    query_list = []
    if ids_to_add:
        query_list.append(add_rows_query.format(','.join(map(str, ids_to_add))))
    if ids_to_delete:
        query_list.append(delete_rows_query.format(','.join(map(str, ids_to_delete))))
    query_list.append(
        update_control_query.format(value=(1 if to_state == one_d_two_d_state else 'NULL'),
                                    operator=('!=' if to_state == one_d_two_d_state else '==')))
    query = ';\n'.join(query_list)
    return query

def construct_manholes_update_query(manholes_to_update_df):
    try:
        query = create_update_case_statement(df=manholes_to_update_df,
                                             layer=manhole_layer,
                                             df_id_col=id_col,
                                             db_id_col=id_col,
                                             old_val_col=calculation_type_col,
                                             new_val_col=new_calc_type_col)
        return query
    except Exception as e:
        raise e from None