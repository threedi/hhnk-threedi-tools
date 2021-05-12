from .default_paths_class import DefaultPaths

def build_folder_structure_dict(polder_path):
    """
    Creates a dictionary containing all folder paths that need to be made when creating
    a new project

    Input: polder (project) path in which to create the structure
    """
    paths_class = DefaultPaths(polder_path)
    base_dict = {}
    base_dict['model_folder'] = paths_class.model.base
    base_dict['threedi_results_folder'] = paths_class.threedi_results.base
    base_dict['threedi_0d1d_results_folder'] = paths_class.threedi_results.zeroDoneD.base
    base_dict['threedi_1d2d_results_folder'] = paths_class.threedi_results.oneDtwoD.base
    base_dict['threedi_climate_results_folder'] = paths_class.threedi_results.climateResults.base
    base_dict['source_data_folder'] = paths_class.source_data.base
    base_dict['output_folder'] = paths_class.output.base
    return base_dict
