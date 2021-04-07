import os
from ...toolbox_universal.variables.types import file_types_dict, NC, H5

def build_threedi_source_paths_dict(results_path=None, revision_dir=None, revision_path=None):
    results_dict = {}
    if revision_path == None:
        path = os.path.join(results_path, revision_dir)
    else:
        path = revision_path
    for item in os.listdir(path):
        if item.endswith(file_types_dict[NC]):
            results_dict['nc_file'] = os.path.join(path, item)
        if item.endswith(file_types_dict[H5]):
            results_dict['h5_file'] = os.path.join(path, item)
    return results_dict
