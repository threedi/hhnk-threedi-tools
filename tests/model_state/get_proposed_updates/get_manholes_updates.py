from ....sql_interaction.sql_functions import get_table_as_df
from ..variables.definitions import hydraulic_test_state
from ..variables.new_columns_mapping import manholes_new_calc_type
from ....variables.database_variables import manhole_layer, calculation_type_col, id_col
from ....variables.backups_table_names import MANHOLES_TABLE
from ...model_backups.find_differences_model_backup import select_values_to_update_from_backup

def get_proposed_updates_manholes(test_env):
    """
    If the model is currently in hydraulic test state, we need to reset the original values for the calculation_type
    field from the backup.
    If we are converting to hydraulic test state, we need to set all calculation_type values to isolated
    (in this case: 1)
    """
    try:
        model_path = test_env.src_paths['model']
        to_state = test_env.conversion_vars.to_state
        from_state = test_env.conversion_vars.from_state
        manholes_in_model_df = get_table_as_df(database_path=model_path, table_name=manhole_layer)
        if to_state == hydraulic_test_state:
            # We have to set all calculation types to isolated
            manholes_in_model_df.insert(manholes_in_model_df.columns.get_loc(calculation_type_col) + 1,
                                        manholes_new_calc_type,
                                        1)
            manholes_to_update = manholes_in_model_df[manholes_in_model_df[calculation_type_col]
                                                      != manholes_in_model_df[manholes_new_calc_type]]
        elif from_state == hydraulic_test_state:
            # we have to restore original calculation types from backup
            manholes_backup_df = get_table_as_df(database_path=model_path, table_name=MANHOLES_TABLE)
            manholes_to_update = select_values_to_update_from_backup(model_df=manholes_in_model_df,
                                                                     backup_df=manholes_backup_df,
                                                                     left_id_col=id_col,
                                                                     right_id_col=id_col,
                                                                     old_val_col=calculation_type_col,
                                                                     new_val_col=manholes_new_calc_type)
        return manholes_to_update
    except Exception as e:
        raise e from None
