from ..input_error_messages import no_output_folder, no_result_selected, \
    invalid_model_path
from .path_verification_functions import is_valid_raster, is_valid_model_path

def verify_input(revision_selected, dem_path, model_path, output_path):
    """
    Checks whether all fields are correctly filled out

    verify_input(revision_selected, dem_path, model_path, output_path)

    return values: valid_input (bool), error_message (empty string if no error, else message to display)
    """
    if revision_selected is None or not revision_selected:
        return False, no_result_selected
    if not is_valid_raster(dem_path):
        return False, dem_path
    if not is_valid_model_path(model_path):
        return False, invalid_model_path.format(model_path)
    if output_path is None or not output_path:
        return False, no_output_folder
    return True, ""



