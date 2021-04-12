from ...toolbox_universal.threedi.variables.variables_container import ThreediInformation

class testEnvironment():
    """
    class contains all arguments needed by test and/or conversion functions:
    source paths (model, datachecker etc)
    output paths (logs, layers)
    qgis layer names
    qgis generated layers information
    qgis groups
    If relevant: 3di variables (result, rain scenario as df)
    conversion variables for model state conversions
    """
    def __init__(self, source_paths_dict, output_vars_dict=None, layers=None,
                 conversion_vars=None, selected_tests=None):
        self.src_paths = source_paths_dict
        self.output_vars = output_vars_dict
        # Only for use with plugin: interaction with qgis
        self.layers = layers
        self.threedi_vars = None
        self.conversion_vars = conversion_vars
        # Only for sqlite tests
        self.selected_tests = selected_tests

