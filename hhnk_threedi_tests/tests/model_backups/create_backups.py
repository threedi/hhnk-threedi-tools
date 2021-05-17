from hhnk_research_tools.sql_interaction.sql_functions import replace_or_add_table
from ..model_state.variables.definitions import undefined_state, one_d_two_d_state
from ...variables.backups_table_names import GLOBAL_SETTINGS_TABLE, BANK_LVLS_TABLE, MANHOLES_TABLE, \
    CONTR_WEIR_WIDTH_BACKUP, CHANNELS_TABLE
from ...variables.database_variables import global_settings_layer, cross_sec_loc_layer, channels_layer, \
    cross_sec_def_layer, manhole_layer
from ...queries.model_backup_queries import weir_width_backup_query

def create_backups(model_path, state=None, manholes_bank_levels_only=False):
    """
    Creates backups based on current model state. If manholes_bank_levels_only is true,
    the function is being called after successful changes by bank_levels function.

    This way, if the bank_levels test was run already, we can use those manholes and bank_levels
    instead of recalculating.
    """
    try:
        if manholes_bank_levels_only == False:
            if state == undefined_state:
                    replace_or_add_table(db=model_path,
                                         dst_table_name=GLOBAL_SETTINGS_TABLE,
                                         src_table_name=global_settings_layer)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=BANK_LVLS_TABLE,
                                         src_table_name=cross_sec_loc_layer)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=CHANNELS_TABLE,
                                         src_table_name=channels_layer)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=CONTR_WEIR_WIDTH_BACKUP,
                                         src_table_name=cross_sec_def_layer,
                                         select_statement=weir_width_backup_query)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=MANHOLES_TABLE,
                                         src_table_name=manhole_layer)
            elif state == one_d_two_d_state:
                    replace_or_add_table(db=model_path,
                                         dst_table_name=BANK_LVLS_TABLE,
                                         src_table_name=cross_sec_loc_layer)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=MANHOLES_TABLE,
                                         src_table_name=manhole_layer)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=CHANNELS_TABLE,
                                         src_table_name=channels_layer)
                    replace_or_add_table(db=model_path,
                                         dst_table_name=CONTR_WEIR_WIDTH_BACKUP,
                                         src_table_name=cross_sec_def_layer,
                                         select_statement=weir_width_backup_query)
        else:
            replace_or_add_table(db=model_path,
                                 dst_table_name=BANK_LVLS_TABLE,
                                 src_table_name=cross_sec_loc_layer)
            replace_or_add_table(db=model_path,
                                 dst_table_name=MANHOLES_TABLE,
                                 src_table_name=manhole_layer)
    except Exception as e:
        raise e from None
