import sys

# folder
from osgeo import gdal

import hhnk_threedi_tools.core
import hhnk_threedi_tools.qgis
import hhnk_threedi_tools.resources
import hhnk_threedi_tools.resources.qgis_layer_styles
import hhnk_threedi_tools.resources.qgis_layer_styles.zero_d_one_d
import hhnk_threedi_tools.resources.schematisation_builder

# checks
from hhnk_threedi_tools.core.checks.bank_levels import BankLevelTest

# FIXME TypeError: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.checks.sqlite.sqlite_main import SqliteCheck  # FIXME
from hhnk_threedi_tools.core.checks.zero_d_one_d import ZeroDOneDTest
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.folders_modelbuilder import FoldersModelbuilder

from hhnk_threedi_tools.core.results.netcdf_timeseries import NetcdfTimeSeries
from hhnk_threedi_tools.core.results.netcdf_essentials import NetcdfEssentials

from hhnk_threedi_tools.core.result_rasters.grid_to_raster import GridToWaterDepth, GridToWaterLevel
from hhnk_threedi_tools.core.result_rasters.grid_to_raster_old import GridToRaster  # TODO deprecate
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import NetcdfToGPKG
from hhnk_threedi_tools.core.schematisation import (
    migrate,
    model_backup,
    model_splitter,
    upload,
)
from hhnk_threedi_tools.core.schematisation.migrate import MigrateSchema

# backup
from hhnk_threedi_tools.core.schematisation.model_backup import (
    create_backups,
    select_values_to_update_from_backup,
    update_bank_levels_last_calc,
)

#
from hhnk_threedi_tools.qgis.layer_structure import (
    LayerStructure,
    QgisAllGroupsSettings,
    QgisGroupSettings,
    QgisLayerSettings,
    SelectedRevisions,
)

# notebooks
from hhnk_threedi_tools.utils.notebooks.run import (
    # add_notebook_paths,
    copy_notebooks,
    open_server,
    read_notebook_json,
    write_notebook_json,
)
