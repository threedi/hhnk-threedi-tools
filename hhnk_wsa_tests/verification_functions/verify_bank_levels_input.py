from .path_verification_functions import is_valid_geodatabase_path, is_valid_model_path
from ..input_error_messages import no_output_folder, invalid_model_path, \
    invalid_datachecker_path, no_result_selected

def verify_input(model_path, datachecker_path, output_path, revision_selected):
    """
    Checks whether all provided input is valid

    verify_input(model_path, datachecker_path, output_path, revision_selected (name of folder))

    return values: valid_input (bool), error_message (empty string if no error)
    """
    if revision_selected is None or not revision_selected:
        return False, no_result_selected
    if not is_valid_model_path(model_path):
        return False, invalid_model_path.format(model_path)
    if not is_valid_geodatabase_path(datachecker_path):
        return False, invalid_datachecker_path.format(datachecker_path)
    if output_path is None or not output_path:
        return False, no_output_folder
    return True, ""
