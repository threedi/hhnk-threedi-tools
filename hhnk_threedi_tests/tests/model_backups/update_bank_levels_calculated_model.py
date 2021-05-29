from ...variables.backups_table_names import BANK_LVLS_LAST_CALC
from ...queries.model_states.bank_levels_calculated import bank_lvls_source_update_query, \
    bank_lvls_source_creation_query
import hhnk_research_tools as hrt

def update_bank_levels_last_calc(db):
    """
    Everytime we calculate the bank levels again, we update the timestamp when they were last calculated
    """
    try:
        if not hrt.sql_table_exists(database_path=db, table_name=BANK_LVLS_LAST_CALC):
            hrt.execute_sql_changes(query=bank_lvls_source_creation_query, database=db)
        hrt.execute_sql_changes(query=bank_lvls_source_update_query, database=db)
    except Exception as e:
        raise e from None
