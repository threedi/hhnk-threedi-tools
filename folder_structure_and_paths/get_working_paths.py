import os
from .build_threedi_paths_dict import build_threedi_source_paths_dict
from .build_output_files_dict import build_output_files_dict

def get_working_paths(type, active_paths,
                      base_folder_output,
                      threedi_results_path=None,
                      threedi_revision_name=None,
                      threedi_revision_path=None):
    """
    1 -> sqlite tests
    2 -> 0d1d tests
    3 -> bank levels
    4 -> 1d2d tests
    Gathers source paths. Copies the active paths, creates input paths for 3di results (needs
    the revision directory AND the results path OR the revision path)
    if appropriate and generates output paths and layer names
    """
    source_paths = active_paths.copy()
    output_dict = build_output_files_dict(type=type,
                                           base_folder=base_folder_output,
                                           revision_dir_name=threedi_revision_name)
    if threedi_results_path is not None and threedi_revision_name is not None:
        threedi_revision_path = os.path.join(threedi_results_path, threedi_revision_name)
    if threedi_revision_path is not None:
        threedi_source_paths = build_threedi_source_paths_dict(revision_path=threedi_revision_path)
    src_paths = {}
    src_paths.update(source_paths)
    if threedi_revision_path is not None:
        src_paths.update(threedi_source_paths)
    return src_paths, output_dict
