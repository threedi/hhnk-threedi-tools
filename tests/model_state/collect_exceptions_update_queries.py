from ...variables.database_variables import global_settings_layer, id_col, manhole_layer,\
    conn_node_id_col, cross_sec_loc_layer, cross_sec_def_layer, channels_layer, control_group_col, \
    bank_level_col, calculation_type_col, width_col
from ...variables.database_aliases import a_cross_loc_id, a_weir_cross_def_id
from ...queries.query_functions import create_update_case_statement
from ...variables.definitions import proposed_value_col
from ...variables.database_variables import manhole_layer, cross_sec_loc_layer, global_settings_layer,\
    cross_sec_def_layer, channels_layer

def collect_excluded(global_settings_excluded=None, bank_levels_excluded=None, new_manholes_excluded=None,
                     manhole_updates_excluded=None, weirs_heights_excluded=None, channels_excluded=None):
    exception_format = "Id's overgeslagen:\nTabel: {}\nKolom: {}\nIds: {}"
    exceptions_list = []
    if global_settings_excluded:
        global_settings_body = exception_format.format(global_settings_layer,
                                                       id_col,
                                                       ', '.join(map(str, global_settings_excluded)))
        exceptions_list.append("Global settings overgeslagen ids\n" + global_settings_body)
    if bank_levels_excluded:
        bank_levels_body = exception_format.format(cross_sec_loc_layer,
                                                   id_col,
                                                   ', '.join(map(str, bank_levels_excluded)))
        exceptions_list.append("Bank levels die niet zijn aangepast\n" + bank_levels_body)
    if new_manholes_excluded:
        new_manholes_body = exception_format.format(manhole_layer,
                                                    conn_node_id_col,
                                                    ', '.join(map(str, new_manholes_excluded)))
        exceptions_list.append("Connection nodes waar geen putten aan zijn toegevoegd\n" + new_manholes_body)
    if manhole_updates_excluded:
        update_manholes_body = exception_format.format(manhole_layer,
                                                       conn_node_id_col,
                                                       ', '.join(map(str, manhole_updates_excluded)))
        exceptions_list.append("Manholes aanpassingen overgeslagen ids\n" + update_manholes_body)
    if weirs_heights_excluded:
        update_weirs_body = exception_format.format(cross_sec_def_layer,
                                                    id_col,
                                                    ', '.join(map(str, weirs_heights_excluded)))
        exceptions_list.append("Gestuurde stuwen waar breedte niet van is aangepast\n" + update_weirs_body)
    if channels_excluded:
        update_channels_body = exception_format.format(channels_layer,
                                                       id_col,
                                                       ', '.join(map(str, channels_excluded)))
        exceptions_list.append("Calculation type watergangen niet aangepast\n" + update_channels_body)
    exceptions_string = '\n\n'.join(exceptions_list)
    return exceptions_string

def collect_manual_adjustments(global_settings_manual_df=None, bank_levels_manual_df=None,
                               manhole_update_manual_df=None, weir_widths_manual_df=None,
                               channels_manual_df=None):
    """
    Add the possibility to make columns editable for changes that affect columns (so not the ones
    deleting or adding rows currently). We collect these manual changes and can return them to the user
    for logging purposes (or to make further adjustments).
    """
    try:
        queries_list = []
        if global_settings_manual_df is not None and not global_settings_manual_df.empty:
            queries_list.append(create_update_case_statement(df=global_settings_manual_df,
                                                             layer=global_settings_layer,
                                                             df_id_col=id_col,
                                                             db_id_col=id_col,
                                                             old_val_col=proposed_value_col,
                                                             new_val_col=control_group_col,
                                                             show_proposed=True))
        if bank_levels_manual_df is not None and not bank_levels_manual_df.empty:
            queries_list.append(create_update_case_statement(df=bank_levels_manual_df,
                                                             layer=cross_sec_loc_layer,
                                                             df_id_col=a_cross_loc_id,
                                                             db_id_col=id_col,
                                                             old_val_col=proposed_value_col,
                                                             new_val_col=bank_level_col,
                                                             show_proposed=True))
        if manhole_update_manual_df is not None and not manhole_update_manual_df.empty:
            queries_list.append(create_update_case_statement(df=manhole_update_manual_df,
                                                             layer=manhole_layer,
                                                             df_id_col=id_col,
                                                             db_id_col=id_col,
                                                             old_val_col=proposed_value_col,
                                                             new_val_col=calculation_type_col,
                                                             show_proposed=True))
        if weir_widths_manual_df is not None and not weir_widths_manual_df.empty:
            queries_list.append(create_update_case_statement(df=weir_widths_manual_df,
                                                             layer=cross_sec_def_layer,
                                                             df_id_col=a_weir_cross_def_id,
                                                             db_id_col=id_col,
                                                             old_val_col=proposed_value_col,
                                                             new_val_col=width_col,
                                                             show_proposed=True))
        if channels_manual_df is not None and not channels_manual_df.empty:
            queries_list.append(create_update_case_statement(df=channels_manual_df,
                                                             layer=channels_layer,
                                                             df_id_col=id_col,
                                                             db_id_col=id_col,
                                                             old_val_col=proposed_value_col,
                                                             new_val_col=calculation_type_col,
                                                             show_proposed=True))
        return '\n'.join(queries_list)
    except Exception as e:
        raise e from None
