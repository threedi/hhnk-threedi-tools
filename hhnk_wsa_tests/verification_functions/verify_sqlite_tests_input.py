from .path_verification_functions import is_valid_model_path, \
    is_valid_geodatabase_path, is_valid_raster, is_valid_shapefile
from ..input_error_messages import no_output_folder, invalid_model_path, \
    dem_needed, datachecker_needed, channel_shape_needed_watersurface, \
    hdb_needed_controlled_structs, damo_needed, polder_shapefile_needed_imp_surface, \
    no_tests_selected

def verify_input(chosen_tests_list, output_path, model_path, dem_path,
                 datachecker_path, channel_shapefile, hdb_path, damo_path,
                 polder_shapefile):
    """
    Checks whether all fields are correctly filled out

    verify_input(chosen_tests_list (list), output_path (dir), model_path (.sqlite), dem_path (.tif),
                 datachecker_path (.gdb), channel_shapefile (.shp), hdb_path (.gdb), damo_path (.gdb),
                 polder_shapefile (.shp))

    return values: valid_input (bool), error_message (empty string if no error, else message to display)
    """
    dewatering_depth_name = 'ontwateringsdiepte'
    max_value_dem_name = 'maximale waarde dem'
    watersurface_area_name = 'oppervlaktewater'
    structs_on_channels_name = 'kunstwerken op watergangen'

    if not chosen_tests_list:
        return False, no_tests_selected
    if output_path is None or not output_path:
        return False, no_output_folder
    if not is_valid_model_path(model_path):
        return False, invalid_model_path.format(model_path)
    # Certain paths are only needed if certain tests are selected. We only verify these path if the tests are selected
    if not is_valid_raster(dem_path) and ('max_dem_chk' in chosen_tests_list
                                          or 'dewatering_depth_chk' in chosen_tests_list):
        return False, dem_needed.format(dem_path, max_value_dem_name if 'max_dem_chk' in chosen_tests_list
                                        else dewatering_depth_name)
    if not is_valid_geodatabase_path(datachecker_path) and ('dewatering_depth_chk' in chosen_tests_list
                                                            or 'watersurface_area_chk' in chosen_tests_list
                                                            or 'structs_channel_chk' in chosen_tests_list):
        if 'dewatering_depth_chk' in chosen_tests_list:
            return False, datachecker_needed.format(dewatering_depth_name)
        elif 'watersurface_area_chk' in chosen_tests_list:
            return False, datachecker_needed.format(watersurface_area_name)
        elif 'structs_channel_chk' in chosen_tests_list:
            return False, datachecker_needed.format(structs_on_channels_name)
    if not is_valid_shapefile(channel_shapefile) and 'watersurface_area_chk' in chosen_tests_list:
        return False, channel_shape_needed_watersurface
    if not is_valid_geodatabase_path(hdb_path) and 'controlled_structs_chk' in chosen_tests_list:
        return False, hdb_needed_controlled_structs.format(hdb_path)
    if not is_valid_geodatabase_path(damo_path) and ('structs_channel_chk' in chosen_tests_list
                                                     or 'watersurface_area_chk' in chosen_tests_list):
        return False, damo_needed.format(damo_path,
                                         structs_on_channels_name if 'structs_channel_chk' in chosen_tests_list
                                         else watersurface_area_name)
    if not is_valid_shapefile(polder_shapefile) and 'impervious_surface_chk' in chosen_tests_list:
        return False, polder_shapefile_needed_imp_surface
    return True, ""
