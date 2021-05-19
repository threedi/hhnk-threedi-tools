from ...variables.backups_table_names import BANK_LVLS_LAST_CALC

bank_lvls_source_creation_query = f"create table {BANK_LVLS_LAST_CALC}(id int PRIMARY KEY, dt datetime)"
bank_lvls_source_update_query = f"INSERT OR REPLACE INTO {BANK_LVLS_LAST_CALC}\nVALUES(1, current_timestamp)"
bank_lvls_last_changed = f"SELECT datetime(dt, 'localtime') AS dt FROM {BANK_LVLS_LAST_CALC} limit 1"
