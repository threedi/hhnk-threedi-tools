import sys

# sys.path.append("C:/Users/chris.kerklaan/Documents/Github/hhnk-research-tools")
# sys.path.append("C:/Users/chris.kerklaan/Documents/Github/hhnk-research-tools")
# sys.path.insert(0, 'C:\\Users\wvangerwen\github\hhnk-threedi-tools')

# folder
from osgeo import gdal
from hhnk_threedi_tools.core.folders import Folders


import hhnk_threedi_tools.core
import hhnk_threedi_tools.resources


#api
from hhnk_threedi_tools.core.api.read_api_file import read_api_file

# tests
from hhnk_threedi_tools.core.checks.bank_levels import (
    BankLevelTest,
)  # FIXME TypeError: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.checks.sqlite import SqliteTest  # FIXME
from hhnk_threedi_tools.core.checks.zero_d_one_d import ZeroDOneDTest


# notebooks
from hhnk_threedi_tools.utils.notebooks.run import add_notebook_paths
from hhnk_threedi_tools.utils.notebooks.run import write_notebook_json
from hhnk_threedi_tools.utils.notebooks.run import read_notebook_json
from hhnk_threedi_tools.utils.notebooks.run import open_server
from hhnk_threedi_tools.utils.notebooks.run import copy_notebooks

# qgis
from hhnk_threedi_tools.qgis.project import copy_projects

# backup
from hhnk_threedi_tools.core.checks.model_backup import (
    create_backups,
    select_values_to_update_from_backup,
    update_bank_levels_last_calc,
)
