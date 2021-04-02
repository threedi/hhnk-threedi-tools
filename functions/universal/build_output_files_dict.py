import os

def build_output_files_dict(type, base_path):
    """
    Creates a dict containing the filepaths (without extensions) we will
    use to write the output to files
    types:
    1 -> sqlite tests
    2 -> 0d1d tests
    3 -> bank levels
    4 -> 1d2d tests
    """
    files_dict = {}
    files_dict['log_path'] = os.path.join(base_path, 'Logs')
    files_dict['layer_path'] = os.path.join(base_path, 'Layers')
    if type == 1:
        files_dict['impervious_surface_filename'] = 'ondoorlatend_oppervlak'
        files_dict['profiles_used_filename'] = 'gebruikte_profielen'
        files_dict['controlled_structs_filename'] = 'gestuurde_kunstwerken'
        files_dict['weir_heights_filename'] = 'stuw_hoogtes'
        files_dict['geometry_filename'] = 'geometrie'
        files_dict['structs_channel_filename'] = 'kunstwerken_op_watergangen'
        files_dict['general_checks_filename'] = 'algemene_tests'
        files_dict['isolated_channels_filename'] = 'geisoleerde_watergangen'
        files_dict['dem_max_val_filename'] = 'maximale_waarde_dem'
        files_dict['dewatering_filename'] = 'drooglegging'
        files_dict['watersurface_filename'] = 'oppervlaktewater'

    if type == 2:
        files_dict['zero_d_one_d_filename'] = '0d1d_toetsing'
        files_dict['hyd_test_channels_filename'] = 'hydraulische_toets_watergangen'
        files_dict['hyd_test_structs_filename'] = 'hydraulische_toets_kunstwerken'

    if type == 3:
        files_dict['flow_1d2d_flowlines_filename'] = 'stroming_1d2d_flowlines'
        files_dict['flow_1d2d_cross_sections_filename'] = 'stroming_1d2d_cross_sections'
        files_dict['flow_1d2d_channels_filename'] = 'stroming_1d2d_watergangen'
        files_dict['flow_1d2d_manholes_filename'] = 'stroming_1d2d_putten'
        files_dict['overview_changes_bank_levels_filename'] = 'overzicht_aanpassingen_bank_levels'

    if type == 4:
        files_dict['grid_nodes_2d_filename'] = 'grid_nodes_2d'
        files_dict['1d2d_all_flowlines_filename'] = '1d2d_alle_stroming'
        files_dict['water_level_filename_template'] = 'waterstand_T{}_uur'
        files_dict['water_depth_filename_template'] = 'waterdiepte_T{}_uur'
    




