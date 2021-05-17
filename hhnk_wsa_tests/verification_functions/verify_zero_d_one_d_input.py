from ..input_error_messages import no_output_folder, no_result_selected

def verify_input(output_path, revision_selected):
    """
    Checks whether all fields are correctly filled out

    verify_input(revision_selected, dem_path, model_path, output_path)

    return values: valid_input (bool), error_message (empty string if no error, else message to display)
    """
    if output_path is None or not output_path:
        return False, no_output_folder
    if revision_selected is None or not revision_selected:
        return False, no_result_selected
    return True, ""
