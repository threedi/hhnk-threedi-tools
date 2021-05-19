from ...variables.backups_table_names import BANK_LVLS_LAST_CALC
from ...queries.model_states.bank_levels_calculated import bank_lvls_source_update_query, \
    bank_lvls_source_creation_query
from hhnk_research_tools.sql_interaction.sql_functions import execute_sql_changes, table_exists

def update_bank_levels_last_calc(db):
    """
    Everytime we calculate the bank levels again, we update the timestamp when they were last calculated
    """
    try:
        if not table_exists(database_path=db, table_name=BANK_LVLS_LAST_CALC):
            execute_sql_changes(query=bank_lvls_source_creation_query, database=db)
        execute_sql_changes(query=bank_lvls_source_update_query, database=db)
    except Exception as e:
        raise e from None
