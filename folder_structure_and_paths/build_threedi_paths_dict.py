import os
from ...toolbox_universal.variables.types import file_types_dict, NC, H5

def build_threedi_source_paths_dict(results_path=None, revision_dir=None, revision_path=None):
    """
    Builds a dictionary containing paths to files pertaining to 3di results.

        build_threedi_source_paths_dict(
                results_path -> None (full path to main results folder (ex: C:/.../0d1d_tests))
                revision_dir -> None (name of revision folder (ex: heiloo_#13_1d2d_test))
                revision_path -> None (full path to revision (ex: C:/.../0d1d_tests/heiloo_#13_1d2d_test))

                Provide EITHER revision_path or results_path AND revision_dir
            )

    returns dictionary containing paths to .h5 ('h5_file') and .nc files ('nc_file')
    If there are multiple files with those extensions, it will choose the last instance
    """
    try:
        results_dict = {}
        if (not revision_path and not (results_path and revision_dir)) \
                or (revision_path and (results_path or revision_dir)):
            raise Exception("Provide either revision_path or results_path and revision_dir")
        if revision_path is None:
            path = os.path.join(results_path, revision_dir)
        else:
            path = revision_path
        for item in os.listdir(path):
            if item.endswith(file_types_dict[NC]):
                results_dict['nc_file'] = os.path.join(path, item)
            if item.endswith(file_types_dict[H5]):
                results_dict['h5_file'] = os.path.join(path, item)
        return results_dict
    except Exception as e:
        raise e from None
