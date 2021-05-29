import hhnk_research_tools as hrt
from ..variables.definitions import hydraulic_test_state
from ....variables.database_variables import channels_layer, id_col, calculation_type_col
from ....variables.backups_table_names import CHANNELS_TABLE
from ..variables.new_columns_mapping import channels_new_calc_type
from ...model_backups.find_differences_model_backup import select_values_to_update_from_backup

def get_proposed_adjustments_channels(test_env):
    """
    If the model is currently in hydraulic test state, we need to reset the width
    of controlled weirs to what they were before
    If we are converting to hydraulic test state, we need to multiply them by 10
    """
    try:
        model_path = test_env.src_paths['model']
        to_state = test_env.conversion_vars.to_state
        from_state = test_env.conversion_vars.from_state
        channels_in_model_df = hrt.sqlite_table_to_df(database_path=model_path, table_name=channels_layer)
        if from_state == hydraulic_test_state:
            # reset backup
            channels_backup_df = hrt.sqlite_table_to_df(database_path=model_path, table_name=CHANNELS_TABLE)
            channels_to_update_df = select_values_to_update_from_backup(model_df=channels_in_model_df,
                                                                        backup_df=channels_backup_df,
                                                                        left_id_col=id_col,
                                                                        right_id_col=id_col,
                                                                        old_val_col=calculation_type_col,
                                                                        new_val_col=channels_new_calc_type)
        elif to_state == hydraulic_test_state:
            # set to isolated
            channels_in_model_df.insert(channels_in_model_df.columns.get_loc(calculation_type_col) + 1,
                                        channels_new_calc_type,
                                        101)
            channels_to_update_df = channels_in_model_df[channels_in_model_df[calculation_type_col] !=
                                                         channels_new_calc_type]
        return channels_to_update_df
    except Exception as e:
        raise e from None
