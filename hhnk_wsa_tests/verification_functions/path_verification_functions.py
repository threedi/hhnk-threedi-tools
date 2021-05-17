import os
from pathlib import Path
from hhnk_threedi_tools.variables.types import file_types_dict, SQLITE, \
    GDB, NC, H5, TIF, SHAPE, SHX, DBF, PRJ

def check_extension(path, extension):
    return Path(path).suffix == file_types_dict[extension]

def valid_path(path):
    if not path or path is None or not os.path.exists(path):
        return False
    return True

def is_valid_model_path(model_path):
    if not valid_path(model_path) or \
            not check_extension(model_path, SQLITE):
        return False
    return True

def is_valid_geodatabase_path(datachecker_path):
    if not valid_path(datachecker_path) or \
        not check_extension(datachecker_path, GDB):
        return False
    return True

def is_valid_results_folder(threedi_result_folder):
    nc_count = 0
    h5_count = 0
    if valid_path(threedi_result_folder):
        for item in os.listdir(threedi_result_folder):
            if Path(item).suffix == file_types_dict[H5]:
                h5_count += 1
            if Path(item).suffix == file_types_dict[NC]:
                nc_count += 1
        return nc_count == 1 and h5_count == 1
    return False

def is_valid_raster(raster_path):
    if not valid_path(raster_path) or \
            not check_extension(raster_path, TIF):
        return False
    return True

def shapefile_dependencies_present(shapefile_path):
    base_path = Path(shapefile_path).parent
    base_name = Path(shapefile_path).stem
    shx_file = os.path.join(base_path, f"{base_name}{file_types_dict[SHX]}")
    dbf_file = os.path.join(base_path, f"{base_name}{file_types_dict[DBF]}")
    prj_file = os.path.join(base_path, f"{base_name}{file_types_dict[PRJ]}")
    if not os.path.exists(shx_file) or not os.path.exists(dbf_file) or \
            not os.path.exists(prj_file):
        return False
    return True

def is_valid_shapefile(shapefile_path):
    if not valid_path(shapefile_path) or \
        not check_extension(shapefile_path, SHAPE) or \
            not shapefile_dependencies_present(shapefile_path):
        return False
    return True
