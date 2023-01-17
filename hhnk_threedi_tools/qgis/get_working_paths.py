import os
from hhnk_research_tools.variables import file_types_dict, NC, H5


def get_working_paths(
    test_type,
    active_paths,
    base_folder_output,
    threedi_results_path=None,
    threedi_revision_name=None,
    threedi_revision_path=None,
):
    """
    1 -> sqlite tests
    2 -> 0d1d tests
    3 -> bank levels
    4 -> 1d2d tests

    Gathers source paths. Copies the active paths, creates input paths for 3di results (needs
    the revision directory AND the results path OR the revision path)
    if appropriate and generates output paths, output file names and layer names
    """
    source_paths = active_paths.copy()
    output_dict = build_output_files_dict(
        test_type=test_type,
        base_folder=base_folder_output,
        revision_dir_name=threedi_revision_name,
    )
    if threedi_results_path is not None and threedi_revision_name is not None:
        threedi_revision_path = os.path.join(
            threedi_results_path, threedi_revision_name
        )
    if threedi_revision_path is not None:
        threedi_source_paths = build_threedi_source_paths_dict(
            revision_path=threedi_revision_path
        )
    src_paths = {}
    src_paths.update(source_paths)
    if threedi_revision_path is not None:
        src_paths.update(threedi_source_paths)
    return src_paths, output_dict


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


def build_threedi_source_paths_dict(
    results_path=None, revision_dir=None, revision_path=None
):
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
        if (not revision_path and not (results_path and revision_dir)) or (
            revision_path and (results_path or revision_dir)
        ):
            raise Exception(
                "Provide either revision_path or results_path and revision_dir"
            )
        if revision_path is None:
            path = os.path.join(results_path, revision_dir)
        else:
            path = revision_path
        for item in os.listdir(path):
            if item.endswith(file_types_dict[NC]):
                results_dict["nc_file"] = os.path.join(path, item)
            if item.endswith(file_types_dict[H5]):
                results_dict["h5_file"] = os.path.join(path, item)
        return results_dict
    except Exception as e:
        raise e from None
