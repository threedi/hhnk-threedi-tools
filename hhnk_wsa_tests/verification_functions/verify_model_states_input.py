from hhnk_wsa_tests.tests.model_state.variables.definitions import hydraulic_test_state, one_d_two_d_state, \
    one_d_two_d_from_backup, one_d_two_d_from_calc
from hhnk_threedi_tools.sql_interaction.sql_functions import table_exists
from hhnk_wsa_tests.variables.backups_table_names import MANHOLES_TABLE, BANK_LVLS_TABLE
from .path_verification_functions import is_valid_model_path, is_valid_geodatabase_path, is_valid_results_folder
from ..input_error_messages import invalid_model_path, no_bank_levels_1d2d_backup, \
    no_manholes_backup, invalid_datachecker_path, invalid_threedi_result, from_and_to_states_same

def verify_input(model_path, from_state, to_state, one_d_two_d_from=None,
                 threedi_result_folder=None, datachecker_path=None):
    """
    Checks whether all fields are correctly filled out

    verify_input(model_path, from_state, to_state, one_d_two_d_from -> None,
                threedi_result_folder -> None, datachecker_path -> None)

    return values: valid_input (bool), error_message (empty string if no error, else message to display)
    """
    if not is_valid_model_path(model_path):
        return False, invalid_model_path.format(model_path)
    if to_state == from_state:
        return False, from_and_to_states_same
    else:
        if to_state == hydraulic_test_state:
            return True, ""
        elif to_state == one_d_two_d_state:
            if one_d_two_d_from == one_d_two_d_from_backup:
                if not table_exists(model_path, BANK_LVLS_TABLE):
                    return False, no_bank_levels_1d2d_backup
                if not table_exists(model_path, MANHOLES_TABLE):
                    return False, no_manholes_backup
            elif one_d_two_d_from == one_d_two_d_from_calc:
                if not is_valid_geodatabase_path(datachecker_path):
                    return False, invalid_datachecker_path.format(datachecker_path)
                if not is_valid_results_folder(threedi_result_folder):
                    return False, invalid_threedi_result.format(threedi_result_folder)
    return True, ""

