from ...variables.backups_table_names import GLOBAL_SETTINGS_TABLE
from ...tests.model_state.variables.definitions import hydraulic_test_state, one_d_two_d_state
from ...variables.database_variables import name_col, zero_d_one_d_val, weir_layer, cross_def_id_col,\
    code_col, id_col, width_col, control_table_layer, target_id_col, cross_sec_def_layer
from ...variables.database_aliases import a_weir_cross_def_id, a_weir_code, a_weir_id
from ...variables.backups_table_names import CONTR_WEIR_WIDTH_BACKUP

#--------------------------------------------------------------------------------
# Read global settings from backup
#--------------------------------------------------------------------------------

all_global_settings = f"""
    SELECT * FROM {GLOBAL_SETTINGS_TABLE}
    WHERE {{}}
    """

def create_global_settings_from_backup_query(to_state):
    where_clause = f"{name_col} {{}} '{zero_d_one_d_val}'"
    if to_state == hydraulic_test_state:
        where_clause = where_clause.format('==')
    elif to_state == one_d_two_d_state:
        where_clause = where_clause.format('!=')
    query = all_global_settings.format(where_clause)
    return query

#--------------------------------------------------------------------------------
# Read weir widths from backup
#--------------------------------------------------------------------------------

weir_widths_from_backup_query = f"""
    SELECT
    {weir_layer}.{cross_def_id_col} as {a_weir_cross_def_id},
    {weir_layer}.{code_col} as {a_weir_code},
    {weir_layer}.{id_col} as {a_weir_id},
    {CONTR_WEIR_WIDTH_BACKUP}.{width_col}
    FROM {weir_layer}
    INNER JOIN {CONTR_WEIR_WIDTH_BACKUP} ON {weir_layer}.{cross_def_id_col} = {CONTR_WEIR_WIDTH_BACKUP}.{id_col}
    INNER JOIN {control_table_layer} ON {weir_layer}.{id_col} = {control_table_layer}.{target_id_col}
    """
