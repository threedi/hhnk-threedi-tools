import os


def build_output_files_dict(test_type, base_folder, revision_dir_name=None):
    """
    Creates a dict containing the file paths (without extensions) we will
    use to write the output to files
    types:
    1 -> sqlite tests
    2 -> 0d1d tests
    3 -> bank levels
    4 -> 1d2d tests
    Base folder is the highest folder in the hierarchy specific to
    a types of test (output/sqlite_tests or output/0d1d_tests for example)
    """
    files_dict = {}
    if test_type in (2, 4) and revision_dir_name:
        # If 3di revisions are involved, we add the revisions name to the output path
        # ex: output/0d1d_tests/{polder name}_#{revision number}_{test type}
        output_revision_dir = revision_dir_name.replace(" ", "_")
        files_dict["output"] = os.path.join(base_folder, output_revision_dir)
    else:
        files_dict["output"] = base_folder

    def add_log_layer_path(base_path):
        # Creates log and layer folders in test specific output folder
        # ex: output/sqlite_tests/Logs and output/sqlite_tests/Layers
        files_dict["log_path"] = os.path.join(base_path, "Logs")
        files_dict["layer_path"] = os.path.join(base_path, "Layers")

    if test_type == 1:
        add_log_layer_path(base_path=files_dict["output"])
        files_dict["impervious_surface_filename"] = "ondoorlatend_oppervlak"
        files_dict["profiles_used_filename"] = "gebruikte_profielen"
        files_dict["controlled_structs_filename"] = "gestuurde_kunstwerken"
        files_dict["weir_heights_filename"] = "bodemhoogte_stuw"
        files_dict["geometry_filename"] = "geometrie"
        files_dict["structs_channel_filename"] = "bodemhoogte_kunstwerken"
        files_dict["general_checks_filename"] = "algemene_tests"
        files_dict["isolated_channels_filename"] = "geisoleerde_watergangen"
        files_dict["init_water_level_filename"] = "initieel_water_level"
        files_dict["dewatering_filename"] = "drooglegging"
        files_dict["water_surface_filename"] = "oppervlaktewater"
        files_dict["cross_section_filename"] = "overlappende_profielen"
        files_dict["cross_section_intersection_filename"] = "profielen_geen_vertex"
    if test_type == 2:
        add_log_layer_path(base_path=files_dict["output"])
        files_dict["zero_d_one_d_filename"] = "0d1d_toetsing"
        files_dict["hyd_test_channels_filename"] = "hydraulische_toets_watergangen"
        files_dict["hyd_test_structs_filename"] = "hydraulische_toets_kunstwerken"

    if test_type == 3:
        add_log_layer_path(base_path=files_dict["output"])
        files_dict["flow_1d2d_flowlines_filename"] = "stroming_1d2d_flowlines"
        files_dict["flow_1d2d_cross_sections_filename"] = "stroming_1d2d_cross_sections"
        files_dict["flow_1d2d_channels_filename"] = "stroming_1d2d_watergangen"
        files_dict["flow_1d2d_manholes_filename"] = "stroming_1d2d_putten"

    if test_type == 4:
        add_log_layer_path(base_path=files_dict["output"])
        files_dict["grid_nodes_2d_filename"] = "grid_nodes_2d"
        files_dict["1d2d_all_flowlines_filename"] = "1d2d_alle_stroming"
        # The actual filename depends on the time steps we are looking at in the test
        # Therefore we create a template rather than a set name
        files_dict["water_level_filename_template"] = "waterstand_T{}_uur"
        files_dict["water_depth_filename_template"] = "waterdiepte_T{}_uur"

    return files_dict
