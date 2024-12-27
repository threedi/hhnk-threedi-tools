# %%
from typing import Union

import hhnk_research_tools as hrt
import xarray as xr

from hhnk_threedi_tools.core.folders_modelbuilder import FoldersModelbuilder

logger = hrt.logging.get_logger(__name__)


class ClipModelRasterCalc(hrt.RasterCalculatorRxr):
    def __init__(
        self,
        raster_out: hrt.Raster,
        raster_paths_dict: dict[str, hrt.Raster],
        metadata_key: str,
        raster_name: str,
        tempdir: hrt.Folder = None,
    ):
        r"""Clip raster to polder bounds."""

        super().__init__(
            raster_out=raster_out,
            raster_paths_dict=raster_paths_dict,
            metadata_key=metadata_key,
            tempdir=tempdir,
        )
        self.raster_name = raster_name

    def run(
        self,
        nodata_keys: list[str],
        dtype: str = "float32",
        waterdeel_value: Union[float, None] = None,
        chunksize: Union[int, None] = None,
        overwrite: bool = False,
    ):
        """
        Parameters
        ----------
        nodata_keys : list of str
            Keys in raster_paths_dict to to use as nodatamask. This will apply
            nodata to the outputraster if any of the inputs have nodata.
        waterdeel_value : float, optional
            Set raster values to this value where `waterdeel` equals 1 (default: None).
        dtype : str
            Data type of the output raster (default: "float32").
        chunksize : int, optional
            Chunk size for processing (default: None).
        overwrite : bool, optional
            Overwrite output (default: False).
        """
        if not self.verify(overwrite=overwrite):
            logger.info(f"Skipping creation of {self.raster_name} as overwrite is not allowed.")
            return

        # Load data arrays
        da_dict = {}
        for key, r in self.raster_paths_same_bounds.items():
            da_dict[key] = r.open_rxr(chunksize)

        da_out = da_dict[self.raster_name]
        crs = da_out.rio.crs  # CRS is lost during xr.where; preserve it.

        # Set waterdeel value if specified (default=10 for dem)
        if waterdeel_value is not None:
            da_out = xr.where(da_dict["waterdeel"] == 1, waterdeel_value, da_out)

        # Create and apply global nodata mask
        nodata = hrt.variables.DEFAULT_NODATA_VALUES[dtype]
        da_nodatamasks = self.get_nodatamasks(da_dict=da_dict, nodata_keys=nodata_keys)
        da_out = xr.where(da_nodatamasks, nodata, da_out)

        # Write to file
        da_out.rio.set_crs(crs)  # Reapply crs
        self.raster_out = hrt.Raster.write(
            self.raster_out,
            result=da_out,
            nodata=nodata,
            dtype=dtype,
            chunksize=chunksize,
        )


def create_schematisation_rasters(folder_mb: FoldersModelbuilder, pytests: bool = False):
    r"""Create schematisation rasters by clipping to model extent

    Requires these functions from hhnk_threedi_tools\core\modelbuilder\create_calculation_rasters.py
    to be run first
    - create_polder_tif
    - create_waterdeel_tif
    """
    tempdir = folder_mb.dst.full_path(f"temp_{hrt.current_time(date=True)}")

    # List of raster types to process
    raster_names = ["dem", "glg", "ggg", "ghg", "infiltration", "friction", "landuse"]

    if pytests:
        raster_names = ["dem", "landuse"]  # Reduced list for tests

    for raster_name in raster_names:
        # Set parameters based on the raster type
        raster_paths_dict = {
            raster_name: getattr(folder_mb.src, raster_name),
            "polder": folder_mb.dst.polder,
            **({"waterdeel": folder_mb.dst.waterdeel} if raster_name == "dem" else {}),
        }
        nodata_keys = ["polder", raster_name]
        waterdeel_value = 10 if raster_name == "dem" else None
        dtype = "uint8" if raster_name == "landuse" else "float32"

        raster_calc = ClipModelRasterCalc(
            raster_out=getattr(folder_mb.dst, raster_name),
            raster_paths_dict=raster_paths_dict,
            metadata_key="polder",
            tempdir=tempdir,
            raster_name=raster_name,
        )

        raster_calc.run(
            nodata_keys=nodata_keys,
            waterdeel_value=waterdeel_value,
            dtype=dtype,
            overwrite=False,
        )
