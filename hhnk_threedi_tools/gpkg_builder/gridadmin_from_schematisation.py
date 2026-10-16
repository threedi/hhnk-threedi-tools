# %%
import logging
import sys
from pathlib import Path

import geopandas as gpd

sys.path.append(r"D:\github\jacostabarragan\hhnk-hydro-core\src")

from hhnk_hydro_core.files_and_folders.utils import check_create_new_file
from threedigrid.admin.exporters.geopackage import GeopackageExporter
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
        self.schematisation_fp = Path(schematisation_fp)
        self.dem_fp = Path(dem_fp)

        if output_fp is None:
            basepath = schematisation_fp.parents[1]
            self.gpkg_output = basepath / "computation_grid.gpkg"
        else:
            self.gridadmin = Path(output_fp)

        self.gridadmin = self.gpkg_output.with_suffix(".h5")
        self.overwrite = overwrite

    @property
    def create(self) -> bool:
        can_create, message = check_create_new_file(path=self.gridadmin, overwrite=self.overwrite)
        if can_create:
            logger.info(message)
            return can_create
        else:
            raise FileExistsError(f"{self.schematisation_fp} already exists.")

    def run_make_gridadmin(self):
        """
        Use threedigrid_builder to convert schematisation to gpkg with all elements.
        threedigrid documentation and parameter names have not been updated and are therefore confusing
        """

        if self.create:
            make_gridadmin(
                sqlite_path=self.schematisation_fp,
                dem_path=self.dem_fp,
                out_path=self.gridadmin,
            )

            logger.info(f"Saved gridadmin to {self.gridadmin}")

            exporter = GeopackageExporter(
                self.gridadmin,
                self.gpkg_output,
            )

            exporter.export()

            logger.info(f"Saved computational grid to {self.gpkg_output}")
        return self.gridadmin


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
    output_gpkg_path = r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\tests\gridadmin_test_V2.gpkg"
    self.run_make_gridadmin()


# %%
