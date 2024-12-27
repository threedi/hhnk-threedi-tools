# %%
"""Aanmaken rasters voor model"""

from typing import Union

import hhnk_research_tools as hrt
import numpy as np
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
        r"""Create model raster with nodata outside polder.
        Bounds based on polder_polygon raster.

        requires these functions from hhnk_threedi_tools\core\modelbuilder\create_calculation_rasters.py
        to be run first
        - create_polder_tif
        - create_waterdeel_tif
        """

        super().__init__(
            raster_out=raster_out,
            raster_paths_dict=raster_paths_dict,
            metadata_key=metadata_key,
            tempdir=tempdir,
        )
        self.raster_name = raster_name

    def run(self, waterdeel_value=None, chunksize: Union[int, None] = None, overwrite: bool = False):
        """
        Parameters
        ----------
        waterdeel_value : float, default None
            Set all values in raster where waterdeel to this value
        chunksize : Union[int, None], optional
            Chunksize to perform calculation on.
        overwrite : bool, optional
            Overwrite output
        """
        cont = self.verify(overwrite=overwrite)

        if cont:
            # Load DataArrays
            da_dict = {}
            da_out = da_dict[self.raster_name] = self.raster_paths_same_bounds[self.raster_name].open_rxr(chunksize)
            da_polder = da_dict["polder"] = self.raster_paths_same_bounds["polder"].open_rxr(chunksize)  # Nodata key

            crs = da_out.rio.crs  # This is lost when xr.where is used.

            if waterdeel_value is not None:
                # waterdeel ophogen naar 10mNAP (default voor dem)
                da_waterdeel = da_dict["waterdeel"] = self.raster_paths_same_bounds["waterdeel"].open_rxr(chunksize)
                da_out = xr.where(da_waterdeel == 1, waterdeel_value, da_out)

            # Create global no data mask
            nodata = np.nan

            da_nodatamasks = self.get_nodatamasks(da_dict=da_dict, nodata_keys=["polder"])
            da_out = xr.where(da_nodatamasks, nodata, da_out)  # Polder extent
            da_out.rio.set_crs(crs)
            self.raster_out = hrt.Raster.write(self.raster_out, result=da_out, nodata=nodata, chunksize=chunksize)
        else:
            logger.info("Cont is false")


def create_schematisation_rasters(folder_mb: FoldersModelbuilder, pytests=False):
    """Create schematisation rasters by clipping to model extent"""
    tempdir = folder_mb.dst.full_path(f"temp_{hrt.current_time(date=True)}")

    # List of raster types to process
    raster_names = ["dem", "glg", "ggg", "ghg", "infiltration", "friction"]

    for raster_name in raster_names:
        # Set parameters based on the raster type
        waterdeel_value = 10 if raster_name == "dem" else None
        raster_paths_dict = {
            raster_name: getattr(folder_mb.src, raster_name),
            "polder": folder_mb.dst.polder,
            **({"waterdeel": folder_mb.dst.waterdeel} if raster_name == "dem" else {}),
        }

        # Initialize the raster calculator
        self = raster_calc = ClipModelRasterCalc(
            raster_out=getattr(folder_mb.dst, raster_name),
            raster_paths_dict=raster_paths_dict,
            metadata_key="polder",
            tempdir=tempdir,
            raster_name=raster_name,
        )

        # Run calculation of output raster
        raster_calc.run(waterdeel_value=waterdeel_value, overwrite=False)

        # If running pytest, break after the first iteration
        if pytests:
            break


if __name__ == "__main__":
    chunksize = None
    overwrite = False
