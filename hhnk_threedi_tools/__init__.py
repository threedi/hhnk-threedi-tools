import sys

# folder
from osgeo import gdal
from hhnk_threedi_tools.core.folders import Folders


import hhnk_threedi_tools.core
import hhnk_threedi_tools.resources



# checks
from hhnk_threedi_tools.core.checks.bank_levels import (
    BankLevelTest,
)  # FIXME TypeError: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.checks.sqlite.sqlite_main import SqliteCheck  # FIXME
from hhnk_threedi_tools.core.checks.zero_d_one_d import ZeroDOneDTest
from hhnk_threedi_tools.core.schematisation.migrate import MigrateSchema

from hhnk_threedi_tools.core.schematisation import migrate, model_backup, model_splitter, upload

# notebooks
from hhnk_threedi_tools.utils.notebooks.run import add_notebook_paths
from hhnk_threedi_tools.utils.notebooks.run import write_notebook_json
from hhnk_threedi_tools.utils.notebooks.run import read_notebook_json
from hhnk_threedi_tools.utils.notebooks.run import open_server
from hhnk_threedi_tools.utils.notebooks.run import copy_notebooks


# backup
from hhnk_threedi_tools.core.schematisation.model_backup import (
    create_backups,
    select_values_to_update_from_backup,
    update_bank_levels_last_calc,
)


__version__ = '2023.3.2'