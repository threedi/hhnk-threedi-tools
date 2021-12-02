from .conversion_vars import modelConversionVariables
from .environment import testEnvironment

import pathlib

current_path = str(pathlib.Path(__file__).parent.absolute())
klimaatsommen_qgis_project = current_path + "/qgis3_export_pdfs.qgz"
