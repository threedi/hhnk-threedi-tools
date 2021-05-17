from ...variables.database_variables import global_settings_layer, control_group_col, id_col, \
    manhole_layer, calculation_type_col, width_col, cross_sec_def_layer, channels_layer
from ...variables.backups_table_names import GLOBAL_SETTINGS_TABLE
from ...variables.database_aliases import a_weir_cross_def_id
from hhnk_threedi_tools.query_functions import create_update_case_statement
from ...tests.model_state.variables.new_columns_mapping import manholes_new_calc_type, weirs_new_width_col, \
    channels_new_calc_type, global_settings_new_col_name
from ...queries.bank_levels_manholes import create_bank_levels_update_query, create_new_manholes_query

#--------------------------------------------------------------------------------
# Get global settings update query
#--------------------------------------------------------------------------------

def create_global_settings_rows_update_query(excluded_ids, ids_to_add=[], ids_to_delete=[]):
    """
    Add rows from backup, delete rows from model
    Once the correct rows are in the model, set model control
    """
    add_rows_query = f"""
    INSERT INTO {global_settings_layer}
    SELECT *
    FROM {GLOBAL_SETTINGS_TABLE}
    WHERE {id_col} in ({{}})
    AND {id_col} NOT IN ({', '.join(map(str, excluded_ids))})
    """

    delete_rows_query = f"""
    DELETE FROM {global_settings_layer}
    WHERE {id_col} IN ({{}})
    AND {id_col} NOT IN ({', '.join(map(str, excluded_ids))})
    """
    query_list = []
    if ids_to_add:
        query_list.append(add_rows_query.format(', '.join(map(str, ids_to_add))))
    if ids_to_delete:
        query_list.append(delete_rows_query.format(', '.join(map(str, ids_to_delete))))
    query = ';\n'.join(query_list)
    return query

def construct_global_settings_control_group_query(global_settings_to_update_df, excluded_ids=[]):
    query = create_update_case_statement(df=global_settings_to_update_df,
                                         layer=global_settings_layer,
                                         df_id_col=id_col,
                                         db_id_col=id_col,
                                         old_val_col=control_group_col,
                                         new_val_col=global_settings_new_col_name,
                                         excluded_ids=excluded_ids)
    return query

def construct_manholes_update_query(manholes_to_update_df, excluded_ids=[]):
    try:
        query = create_update_case_statement(df=manholes_to_update_df,
                                             layer=manhole_layer,
                                             df_id_col=id_col,
                                             db_id_col=id_col,
                                             old_val_col=calculation_type_col,
                                             new_val_col=manholes_new_calc_type,
                                             excluded_ids=excluded_ids)
        return query
    except Exception as e:
        raise e from None

def construct_weir_height_update_statement(weir_widths_to_update_df, excluded_ids=[]):
    try:
        query = create_update_case_statement(df=weir_widths_to_update_df,
                                             layer=cross_sec_def_layer,
                                             df_id_col=a_weir_cross_def_id,
                                             db_id_col=id_col,
                                             old_val_col=width_col,
                                             new_val_col=weirs_new_width_col,
                                             excluded_ids=excluded_ids)
        return query
    except Exception as e:
        raise e from None

def construct_channels_update_statement(channels_to_update_df, excluded_ids=[]):
    try:
        query = create_update_case_statement(df=channels_to_update_df,
                                             layer=channels_layer,
                                             df_id_col=id_col,
                                             db_id_col=id_col,
                                             old_val_col=calculation_type_col,
                                             new_val_col=channels_new_calc_type,
                                             excluded_ids=excluded_ids)
        return query
    except Exception as e:
        raise e from None
