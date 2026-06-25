# %%
import logging
import getpass

from pathlib import Path

import geopandas as gpd

from hhnk_hydro_core.utils.time import current_time
from hhnk_hydro_core.files_and_folders.utils import check_create_new_file
from hhnk_hydro_core.spatial_database import SpatialDatabase
from hhnk_hydro_core.netcdf.rana_netcdf import RESULT_GEOPACKAGE
from threedigrid.admin.exporters.geopackage import GeopackageExporter

from hhnk_hydro_business.rana.rana_utils import load_gridadmin

logger = logging.getLogger(__name__)
# TODO include styling in the gridadmin export, e.g. by adding a styling table to the gpkg and linking it to the elements in the gridadmin tables

class GridadminFromResult:
    """
    Class to convert geospatial data from Rana/threedi netCDF result into GeoPackage.

    Parameters
    ----------
    rana_result_admin_fp : Path
        Path to rana result gridadmin.h5 file.
    output_fp: Path = None
        Path to output GeoPackage file. If None, it will be saved in the result folder
    overwrite : bool, True
        Will overwrite by default
    """

    def __init__(self,
                 rana_result_admin_fp: Path,
                 output_fp: Path = None,
                 overwrite: bool = True,
                 ):
        self.gridadmin_fp = Path(rana_result_admin_fp)
        if output_fp is None:
            self.output_fp = rana_result_admin_fp.parent / RESULT_GEOPACKAGE
        else:
            self.output_fp = Path(output_fp)
        self.overwrite = overwrite


    @property
    def create(self) -> bool:
        can_create, message =  check_create_new_file(path=self.output_fp, overwrite=self.overwrite)
        if can_create:
            logger.info(message)
            return can_create
        else:
            raise FileExistsError(f"{self.gridadmin_fp} already exists.")


    def run_make_gridadmin(self) -> dict[str, gpd.GeoDataFrame]:
        """Use threedigrid to convert h5 to gpkg with all elements."""

        if self.create:
            gridadmin = GeopackageExporter(self.gridadmin_fp,self.output_fp)
            gridadmin.export()
            logger.info(f"Saved grid, nodes and lines to {self.output_fp}")

            # Load the gridadmin data into a dictionary of GeoDataFrames
            gridadmin_dict = load_gridadmin(self.output_fp)
            # Add extra metadata to the gridadmin dict
            gridadmin_dict["meta"]["created_at"] = current_time(include_date =True)
            gridadmin_dict["meta"]["created_by"] = getpass.getuser()

            # Write metadata back to the gpkg
            meta_gdf = gpd.GeoDataFrame(gridadmin_dict["meta"])
            gridadmin_db = SpatialDatabase(self.output_fp)
            gridadmin_db.write_layer(meta_gdf, "meta", if_exists="replace")

            # Remove 2D nodes (duplicate output)
            gridadmin_dict["node"] = gridadmin_dict["node"][~gridadmin_dict["node"]["node_type"].isin([1,2])].copy()
            gridadmin_db.write_layer(gridadmin_dict["node"], "node", if_exists="replace")

            return gridadmin_dict

# %%
