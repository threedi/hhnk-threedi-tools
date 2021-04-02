import os
from ...variables.universal.damo_hdb_datachecker_vars import damo_duiker_sifon_hevel, \
    damo_waterdeel, datachecker_culvert_layer, datachecker_fixed_drainage, \
    hdb_sturing_3di, waterlevel_val_field

def build_source_paths_dict(paths_object):
    paths_dict = {}

    def if_exists(path):
        return path if os.path.exists(path) else None

    # Source data
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
    paths_dict['0d1d_results_dir'] = paths_object.threedi_results.zeroDoneD.base
    paths_dict['1d2d_results_dir'] = paths_object.threedi_results.oneDtwoD.base
    return paths_dict
