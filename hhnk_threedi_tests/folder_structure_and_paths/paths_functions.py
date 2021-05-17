import os
from hhnk_research_tools.variables.types import file_types_dict, TIF

def get_top_level_directories(folder, condition_test=None):
    """
    Resturns a list of all top level directories, can be filtered with a function (condition_test)
    that returns a bool and takes one argument (directory)
    """
    return [item for item in (os.path.join(folder, d1) for d1 in os.listdir(folder))
            if os.path.isdir(item) and (condition_test(item) if condition_test is not None else True)]

def create_tif_path(folder, filename):
    """
    Takes a folder name (ex: C:../output/Layers) and base filename (ex: raster) as arguments
    and returns full path (ex: C:../output/Layers/raster.tif)
    """
    try:
        full_path = os.path.join(folder, filename + file_types_dict[TIF])
        return full_path
    except Exception as e:
        raise e from None
