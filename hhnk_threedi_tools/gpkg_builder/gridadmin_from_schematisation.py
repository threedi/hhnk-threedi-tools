# %%
import logging
from pathlib import Path

from hhnk_hydro_core.files_and_folders.utils import check_create_new_file
from threedigrid_builder import make_gridadmin

logger = logging.getLogger(__name__)


class GridadminFromSchematisation:
    f"""
    Class to convert geospatial data from Rana/threedi schematisation into grid admin (.gpkg).

    Parameters
    ----------
    schematisation_fp : Path
        Path to schematisation GeoPackage file.
    dem_fp : Path
        Path dem model dem TIFF file.
    output_fp: Path = None
        Path to output GeoPackage file. If None, it will be saved in the schematisation folder as {RESULT_GEOPACKAGE}
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
            gridadmin = make_gridadmin(
                sqlite_path=self.rana_result_admin_fp, dem_path=self.dem_fp, out_path=self.output_fp
            )
            logger.info(f"Saved grid, nodes and lines to {self.output_fp}")

            # TODO add metadata user and time result modified utils function
            # TODO test with breach lines? Seperate layer?


# %%
if True:
    import logging

    import pytest
    from hhnk_hydro_core.utils.time import current_time

    from tests.config import FOLDER_TEST, TEMP_DIR

    rana_result_admin_fp = FOLDER_TEST.threedi_results.one_d_two_d[0].admin_path

    self = GridadminFromSchematisation(rana_result_admin_fp, dem_fp)
    self.create
    self.run_make_gridadmin()

# %%
