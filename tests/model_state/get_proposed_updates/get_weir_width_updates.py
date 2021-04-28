import pandas as pd
from ....sql_interaction.sql_functions import execute_sql_selection
from ..variables.definitions import hydraulic_test_state
from ....variables.database_variables import width_col
from ....variables.database_aliases import a_weir_id
from ..variables.new_columns_mapping import weirs_new_width_col
from ...model_backups.find_differences_model_backup import select_values_to_update_from_backup
from ....queries.model_states.selection_queries import controlled_weirs_selection_query as from_model_query
from ....queries.model_states.read_backups_queries import weir_widths_from_backup_query as from_backup_query

def get_proposed_adjustments_weir_width(test_env):
    """
    If the model is currently in hydraulic test state, we need to reset the width
    of controlled weirs to what they were before
    If not, we need to multiply them by 10
    """
    try:
        model_path = test_env.src_paths['model']
        to_state = test_env.conversion_vars.to_state
        from_state = test_env.conversion_vars.from_state
        weir_widths_in_model_df = execute_sql_selection(query=from_model_query, database_path=model_path)
        if from_state == hydraulic_test_state:
            # reset backup
            weir_widths_backup_df = execute_sql_selection(query=from_backup_query, database_path=model_path)
            weir_widths_to_update = select_values_to_update_from_backup(model_df=weir_widths_in_model_df,
                                                                        backup_df=weir_widths_backup_df,
                                                                        left_id_col=a_weir_id,
                                                                        right_id_col=a_weir_id,
                                                                        old_val_col=width_col,
                                                                        new_val_col=weirs_new_width_col)
        elif to_state == hydraulic_test_state:
            # multiply by 10
            weir_widths_in_model_df[width_col] = pd.to_numeric(weir_widths_in_model_df[width_col])
            weir_widths_in_model_df.insert(weir_widths_in_model_df.columns.get_loc(width_col) + 1,
                                           weirs_new_width_col,
                                           weir_widths_in_model_df[width_col].apply(lambda x: round((x * 10), 3)))
            weir_widths_to_update = weir_widths_in_model_df
        return weir_widths_to_update
    except Exception as e:
        raise e from None
