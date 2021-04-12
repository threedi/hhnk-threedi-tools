from ...variables.backups_table_names import GLOBAL_SETTINGS_TABLE
from ...tests.model_state.definitions import hydraulic_test_state, one_d_two_d_state
from ...variables.database_variables import name_col, zero_d_one_d_val

#--------------------------------------------------------------------------------
# Read global settings from backup
#--------------------------------------------------------------------------------

all_global_settings = f"""
    SELECT * FROM {GLOBAL_SETTINGS_TABLE}
    WHERE {{}}
    """

def create_global_settings_from_backup_query(to_state):
    print(to_state)
    where_clause = f"{name_col} {{}} '{zero_d_one_d_val}'"
    if to_state == hydraulic_test_state:
        where_clause = where_clause.format('==')
    elif to_state == one_d_two_d_state:
        where_clause = where_clause.format('!=')
    print(where_clause)
    query = all_global_settings.format(where_clause)
    return query
