from ....sql_interaction.sql_functions import get_table_as_df
from ...model_backups.find_differences_model_backup import select_values_to_update_from_backup
from ....variables.database_variables import cross_sec_loc_layer, id_col, bank_level_col
from ....variables.database_aliases import a_cross_loc_id
from ...bank_levels.variables.dataframe_variables import new_bank_level_col
from ....variables.backups_table_names import BANK_LVLS_TABLE

def get_bank_levels_to_update_from_backup(test_env):
    """
    Collects the bank level that need to be updated by comparing the bank levels currently in the model
    to the bank levels in the backup. Any rows where these values differ are returned in the 'bank_levels_to_update'
    dataframe
    """
    try:
        model_path = test_env.src_paths['model']
        model_bank_levels_df = get_table_as_df(database_path=model_path,
                                               table_name=cross_sec_loc_layer)
        backup_bank_levels_df = get_table_as_df(database_path=model_path,
                                                table_name=BANK_LVLS_TABLE)
        bank_levels_to_update = select_values_to_update_from_backup(model_df=model_bank_levels_df,
                                                                    backup_df=backup_bank_levels_df,
                                                                    left_id_col=id_col,
                                                                    right_id_col=id_col,
                                                                    old_val_col=bank_level_col,
                                                                    new_val_col=new_bank_level_col)
        if bank_levels_to_update is not None and not bank_levels_to_update.empty:
            bank_levels_to_update.rename(columns={id_col: a_cross_loc_id},
                                         inplace=True)
            return bank_levels_to_update
        else:
            return None
    except Exception as e:
        raise e from None
