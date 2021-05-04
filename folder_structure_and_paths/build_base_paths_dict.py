import os
from .default_paths_class import DefaultPaths
from ..variables.damo_hdb_datachecker_variables import damo_duiker_sifon_hevel, \
    damo_waterdeel, datachecker_culvert_layer, datachecker_fixed_drainage, \
    hdb_sturing_3di, waterlevel_val_field

def build_base_paths_dict(polder_path):
    """
    Returns dictionary containing paths to source files according to set project structure.

        build_base_paths_dict(
                polder_path (string: path to project folder (highest level))
            )
    """
    paths_object = DefaultPaths(base=polder_path)
    paths_dict = {}

    def if_exists(path):
        return path if os.path.exists(path) else None

    # Source data
    paths_dict['polder_folder'] = polder_path
    paths_dict['datachecker'] = if_exists(paths_object.source_data.datachecker)
    paths_dict['damo'] = if_exists(paths_object.source_data.damo)
    paths_dict['hdb'] = if_exists(paths_object.source_data.hdb)
    paths_dict['polder_shapefile'] = if_exists(paths_object.source_data.polder_polygon)
    paths_dict['channels_shapefile'] = if_exists(paths_object.source_data.modelbuilder.channel_from_profiles)

    # Layer names source data
    paths_dict['damo_duiker_sifon_layer'] = damo_duiker_sifon_hevel
    paths_dict['damo_waterdeel_layer'] = damo_waterdeel
    paths_dict['datachecker_culvert_layer'] = datachecker_culvert_layer
    paths_dict['datachecker_fixed_drainage'] = datachecker_fixed_drainage
    paths_dict['hdb_sturing_3di_layer'] = hdb_sturing_3di
    paths_dict['init_waterlevel_val_field'] = waterlevel_val_field

    # Model folder
    paths_dict['model'] = if_exists(paths_object.model.database)
    paths_dict['dem'] = if_exists(paths_object.model.rasters.dem)

    # Threedi
    paths_dict['0d1d_results_dir'] = if_exists(paths_object.threedi_results.zeroDoneD.base)
    paths_dict['1d2d_results_dir'] = if_exists(paths_object.threedi_results.oneDtwoD.base)

    # Default output folders
    paths_dict['base_output'] = os.path.join(polder_path, 'Output')
    paths_dict['sqlite_tests_output'] = os.path.join(paths_dict['base_output'], 'sqlite_tests')
    paths_dict['0d1d_output'] = os.path.join(paths_dict['base_output'], '0d1d_tests')
    paths_dict['bank_levels_output'] = os.path.join(paths_dict['base_output'], 'bank_levels')
    paths_dict['1d2d_output'] = os.path.join(paths_dict['base_output'], '1d2d_tests')
    return paths_dict
