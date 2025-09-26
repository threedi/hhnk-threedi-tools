import os

import hhnk_research_tools as hrt
import numpy as np
from hhnk_research_tools.folder_file_classes.file_class import File
from hhnk_research_tools.folder_file_classes.folder_file_classes import Folder

# from hhnk_research_tools.folder_file_classes.sqlite_class import Sqlite  # TODO move
from hhnk_research_tools.folder_file_classes.spatial_database_class import SpatialDatabase
from hhnk_research_tools.rasters.raster_class import Raster  # FIXME new import

# from hhnk_research_tools.gis.raster import RasterOld as Raster
from hhnk_research_tools.threedi.threediresult_loader import ThreediResultLoader

logger = hrt.logging.get_logger(name=__name__)


class ThreediSchematisation(Folder):
    """Threedi model/schematisation.
    expected files are the;
    .gpkg
    /rasters
        - content depends on model type, they are read from the global settings in the schematisation.
    """

    def __init__(self, base, name, create=True):
        super().__init__(os.path.join(base, name), create=create)

        # File
        # self.add_file("database", self.model_path())
        database = self.find_ext("gpkg")
        if len(database) > 1:
            raise ValueError("More than 1 gpkg found in folder, cannot determine which to use.")
        elif len(database) == 0:
            logger.warning(f"No gpkg found in {self.path}.")

    @property
    def rasters(self):
        return self.ThreediRasters(base=self.base, caller=self)

    @property
    def database(self):
        filepath = self.model_path()
        if filepath in [None, ""]:
            filepath = ""

        model_cls = SpatialDatabase(filepath)
        # if os.path.exists(sqlite_cls.path):
        #     return sqlite_cls
        # else:
        #     return None
        return model_cls

    @property
    def structure(self):
        return f"""  
            {self.space}model
            {self.space}└── rasters
            """

    @property
    def database_path(self):
        return str(self.database)

    @property
    def model_paths(self):
        """Return all models (gpkg) in folder"""
        return self.find_ext("gpkg")

    @property
    def model_names(self):
        """Return all models (gpkg) in folder"""
        return [sp.stem for sp in self.model_paths]

    def model_path(self, idx=0, name=None):
        """Find a model using an index"""
        if name:
            try:
                idx = self.model_names.index(name)
            except Exception:
                raise ValueError("name of model given, but cannot be found")
        if len(self.model_paths) >= 1:
            return self.model_paths[idx]
        else:
            return ""

    class ThreediRasters(Folder):
        def __init__(self, base, caller):
            super().__init__(os.path.join(base, "rasters"), create=True)
            self.caller = caller

            self.dem = self.get_raster_path(table_name="model_settings", col_name="dem_file")
            self.storage = self.get_raster_path(
                table_name="simple_infiltration",
                col_name="max_infiltration_volume_file",
            )
            self.friction = self.get_raster_path(table_name="model_settings", col_name="friction_coefficient_file")
            self.infiltration = self.get_raster_path(
                table_name="simple_infiltration", col_name="infiltration_rate_file"
            )
            self.initial_wlvl_2d = self.get_raster_path(
                table_name="initial_conditions", col_name="initial_water_level_file"
            )

            # Waterschadeschatter required 50cm resolution.
            self.dem_50cm = self.full_path("dem_50cm.tif")

            self.landuse = self.find_file_by_name("landuse_*.tif")

            self.add_file("soil", "soil.tif")
            # Groundwaterlevel (used to create storage)
            self.add_file("gwlvl_glg", "gwlvl_glg.tif")
            self.add_file("gwlvl_ggg", "gwlvl_ggg.tif")
            self.add_file("gwlvl_ghg", "gwlvl_ghg.tif")

        def find_file_by_name(self, name: str) -> File:
            tifs = list(self.path.glob(name))
            if len(tifs) == 0:
                tifs = [""]
            return File(tifs[0])

        def get_raster_path(self, table_name: str, col_name: str) -> Raster:
            """Read raster path from database using SpatialDatabase class."""

            if self.caller.database.exists():
                spdb = SpatialDatabase(self.caller.database.path)
                df = spdb.load(layer=table_name)
                if len(df) > 1:
                    logger.warning(f"{table_name} has more than 1 row. Choosing the first row for the rasters.")
                    df = df.iloc[[0]]
                if len(df) == 0:
                    raster_name = None
                    logger.warning(f"{table_name} has no rows, cannot find raster for {self.caller.database.path}.")
                else:
                    raster_name = df.iloc[0][col_name]

                if raster_name is None:
                    raster_path = ""
                else:
                    raster_path = self.caller.full_path(raster_name)
            else:
                raster_path = ""
            return Raster(raster_path)

        def __repr__(self):
            return f"""  
    dem - {self.dem.name}
    storage - {self.storage.name}
    friction - {self.friction.name}
    infiltration - {self.infiltration.name}
    landuse - {self.landuse.name}
    initial_wlvl_2d - {self.initial_wlvl_2d.name}
    dem_50cm - {self.dem_50cm.name}
"""


class ThreediResult(Folder):
    """Result of threedi simulation. Base files are .nc and .h5.
    Use .grid to access GridH5ResultAdmin and .admin to access GridH5Admin
    """

    def __init__(self, base, create=False):
        super().__init__(base, create=create)

        # Files
        self.add_file("grid_path", "results_3di.nc")
        self.add_file("admin_path", "gridadmin.h5")
        self.add_file("aggregate_grid_path", "aggregate_results_3di.nc")

    @property
    def grid(self):
        # moved imports here because gridbuilder has h5py issues
        from threedigrid.admin.gridresultadmin import GridH5ResultAdmin

        return GridH5ResultAdmin(self.admin_path.base, self.grid_path.base)

    @property
    def aggregate_grid(self):
        # moved imports here because gridbuilder has h5py issues
        from threedigrid.admin.gridresultadmin import GridH5AggregateResultAdmin

        return GridH5AggregateResultAdmin(self.admin_path.base, self.aggregate_grid_path.base)

    @property
    def admin(self):
        from threedigrid.admin.gridadmin import GridH5Admin

        return GridH5Admin(self.admin_path.base)

    @property
    def load(self) -> ThreediResultLoader:
        return ThreediResultLoader(self.grid)

    def __repr__(self):
        return f"""{self.path.name} @ {self.path}
exists: {self.exists()}
type: {type(self)}
functions: {hrt.get_functions(self)}
variables: {hrt.get_variables(self)}
"""


class RevisionsDir(Folder):
    """directory with subfolders.
    When the dir is accessed with indexing ["foldername"], the returnclass is retured.
    This defaults to the ThreediResult directory with .nc and .h5. But for climate results
    the folder structure is a bit different.
    """

    def __init__(self, base, name, returnclass=ThreediResult, create=False):
        super().__init__(os.path.join(base, name), create=create)
        self.isrevisions = True
        self.returnclass = returnclass  # eg ClimateResult
        self.sub_folders = {}  # revisions that are already initialized.

    def __getitem__(self, revision):
        """revision can be a integer or a path"""
        create = True
        if revision in ["", None]:
            create = False

        if isinstance(revision, int):  # revision number as input
            revision_dir = self.revisions[revision]
        elif os.path.isabs(str(revision)):  # full path as input
            revision_dir = revision
        elif os.sep not in str(revision):
            revision_dir = self.full_path(revision)
        else:
            raise ValueError(f"{str(revision)} is not valid input for `revision`")

        revision_dir = Folder(revision_dir)
        if revision_dir.name not in self.sub_folders.keys():
            self.sub_folders[revision_dir.name] = self.returnclass(revision_dir, create=create)

        return self.sub_folders[revision_dir.name]

    def revision_structure(self, name: str):
        spacing = "\n\t\t\t\t\t\t\t"
        structure = f""" {spacing}{name} """
        for i, rev in enumerate(self.revisions):
            if i == len(self.revisions) - 1:
                structure = structure + f"{spacing}└── {rev}"
            else:
                structure = structure + f"{spacing}├── {rev}"

        return structure

    @property
    def revisions(self) -> list:
        return self.content

    @property
    def revisions_mtime(self):
        """Sorted list of revisions by:
        mtime -> latest edit date first
        """
        revisions_sorted = np.take(self.revisions, np.argsort([item.lstat().st_mtime for item in self.revisions]))[
            ::-1
        ]
        return revisions_sorted

    @property
    def revisions_rev(self):
        """Sort list of revisions by:
        rev -> revisions. highest revisionnr first
        """
        lst_items = []
        for item in self.revisions:
            try:
                lst_items += [int(str(item.name).split("#")[1].split(" ")[0])]
            except:
                lst_items += [0]  # add 0 so its always at end of list.
        revisions_sorted = np.take(self.revisions, np.argsort(lst_items))[::-1]
        return revisions_sorted
