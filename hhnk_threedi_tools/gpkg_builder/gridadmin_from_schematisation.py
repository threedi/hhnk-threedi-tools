# %%
import logging
import sys
from pathlib import Path

import geopandas as gpd

sys.path.append(r"D:\github\jacostabarragan\hhnk-hydro-core\src")

from hhnk_hydro_core.files_and_folders.utils import check_create_new_file
from threedigrid_builder import make_gridadmin

logger = logging.getLogger(__name__)


class GridadminFromSchematisation:
    """
    Class to convert geospatial data from Rana/threedi schematisation into grid admin (.gpkg).

    Parameters
    ----------
    schematisation_fp : Path
        Path to schematisation GeoPackage file.
    dem_fp : Path
        Path dem model dem TIFF file.
    output_fp: Path = None
        Path to output GeoPackage file. If None, it will be saved in the schematisation folder
    overwrite : bool, True
        Will overwrite by default
    """

    def __init__(
        self,
        schematisation_fp: Path,
        dem_fp: Path,
        output_fp: Path = None,
        overwrite: bool = True,
    ):
        self.gridadmin_fp = Path(schematisation_fp)
        self.dem_fp = Path(dem_fp)
        if output_fp is None:
            basepath = schematisation_fp.parents[1]
            self.output_fp = basepath / "gridadmin.gpkg"  # TODO where to put this file?
        else:
            self.output_fp = Path(output_fp)
        self.overwrite = overwrite

    @property
    def create(self) -> bool:
        can_create, message = check_create_new_file(path=self.output_fp, overwrite=self.overwrite)
        if can_create:
            logger.info(message)
            return can_create
        else:
            raise FileExistsError(f"{self.gridadmin_fp} already exists.")

    def run_make_gridadmin(self):
        """
        Use threedigrid_builder to convert schematisation to gpkg with all elements.
        threedigrid documentation and parameter names have not been updated and are therefore confusing
        """

        if self.create:
            gridadmin = make_gridadmin(sqlite_path=self.gridadmin_fp, dem_path=self.dem_fp, out_path=self.output_fp)
            logger.info(f"Saved grid, nodes and lines to {self.output_fp}")

            # TODO add metadata user and time result modified utils function
            # TODO test with breach lines? Seperate layer?
        return self.output_fp

    def transform_columns_values(self):
        replace_data_nodes = {
            "NODE_2D_OPEN_WATER": 1,
            "NODE_1D_NO_STORAGE": 3,
            "NODE_1D_BOUNDARIES": 7,
            "NODE_1D_STORAGE": 4,
        }

        replace_data_lines = {
            "LINE_1D2D_DOUBLE_CONNECTED_CLOSED": 53,
            "LINE_1D2D_DOUBLE_CONNECTED_OPEN_WATER": 54,
            "LINE_1D2D_SINGLE_CONNECTED_CLOSED": 51,
            "LINE_1D2D_SINGLE_CONNECTED_OPEN_WATER": 52,
            "LINE_1D_CONNECTED": 2,
            "LINE_1D_DOUBLE_CONNECTED": 5,
            "LINE_1D_EMBEDDED": 0,
            "LINE_1D_ISOLATED": 1,
            "LINE_1D_LONG_CRESTED": 3,
            "LINE_1D_SHORT_CRESTED": 4,
            "LINE_2D_OBSTACLE_U": 101,
            "LINE_2D_OBSTACLE_V": 101,
            "LINE_2D_U": 100,
            "LINE_2D_V": 100,
        }

        #fix nodes
        grid_gdf_nodes = gpd.read_file(self.output_fp, layer="nodes")

        grid_gdf_nodes["node_type"] = grid_gdf_nodes["node_type"].replace(replace_data_nodes)

        grid_gdf_nodes.to_file(filename=self.output_fp, driver="GPKG", layer="nodes", engine="pyogrio")

        #fix lines
        grid_gdf_lines = gpd.read_file(self.output_fp, layer="lines")

        grid_gdf_lines.rename(columns={"kcu": "line_type"})

        grid_gdf_lines["line_type"] = grid_gdf_lines["line_type"].replace(replace_data_lines)

    def get_obstacles_lines(self):


        
# %%columns={"A": "a", "B": "c"}
if True:
    import logging

    import pytest
    from hhnk_hydro_core.utils.time import current_time

    from tests.config import FOLDER_TEST, TEMP_DIR

    # rana_result_admin_fp = FOLDER_TEST.threedi_results.one_d_two_d[0].admin_path
    path = Path(
        r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
    )
    dem_fp = Path(
        r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\schematisation\rasters\dem_1_met_amstelmeer_2m.tif"
    )
    self = GridadminFromSchematisation(path, dem_fp)
    self.create
    self.run_make_gridadmin()
    self.transform_columns_values()

# %%
