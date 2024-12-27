# %%
"""Aanmaken rasters voor model"""

from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import xarray as xr

import hhnk_threedi_tools as htt

logger = hrt.logging.get_logger(__name__)


class ModelbuilderRasters:
    """This class will create the rasters for a schematisation.
    The folder attribute contains the links to the source files
    that will be used to create these rasters.
    """

    def __init__(
        self,
        folder: htt.FoldersModelbuilder,
        resolution: float = 0.5,
        nodata: int = -9999,
        overwrite: bool = False,
        verbose: bool = False,
    ):
        """
        folder : htt.FoldersModelbuilder
            folder structure of modelbuilder with src files and dst paths
        resolution : float, optional, by default 0.5
            resultion of output raster
        nodata : int, optional, by default -9999
            nodata value of output raster
        overwrite : bool, optional, by default False
            overwrite the output file if it already exists
        verbose : bool, optional, by default False
            print all debug statements
        """
        self.folder = folder
        self.resolution = resolution
        self.nodata = nodata
        self.overwrite = overwrite
        self.verbose = verbose

        # Assigned during function calls.
        self.dem_calc = None

    def prepare_input(self):
        """
        Rasterize polder and waterdeel
        Create vrt of source dem and gxg with bounds of polder raster.
        """
        # Rasterize polder polygon
        if not self.folder.dst.tmp.polder.exists():
            gdf_polder = gpd.read_file(self.folder.src.polder.path)
            metadata = hrt.RasterMetadataV2.from_gdf(gdf=gdf_polder, res=self.resolution)
            gdf_polder["value"] = 1
            hrt.gdf_to_raster(
                gdf=gdf_polder,
                value_field="value",
                raster_out=self.folder.dst.tmp.polder,
                nodata=self.nodata,
                metadata=metadata,
                read_array=False,
            )

        # Rasterize watergangen
        if not self.folder.dst.tmp.waterdeel.exists():
            gdf = gpd.read_file(self.folder.src.waterdeel.path)
            metadata = self.folder.dst.tmp.polder.metadata
            gdf["value"] = 1
            hrt.gdf_to_raster(
                gdf=gdf,
                value_field="value",
                raster_out=self.folder.dst.tmp.waterdeel,
                nodata=self.nodata,
                metadata=metadata,
                read_array=False,
                overwrite=False,
            )

    def create_rasters(self):
        """Create output rasters dem and gxg"""

        # Dem creation
        def run_dem_window(block):
            """Custom calc function on blocks in hrt.RasterCalculatorV2"""
            block_out = block.blocks["dem"]

            # waterdeel ophogen naar +10mNAP
            block_out[block.blocks["waterdeel"] == 1] = 10

            block_out[block.masks_all] = self.nodata
            return block_out

        # init calculator
        self.dem_calc = hrt.RasterCalculatorV2(
            raster_out=self.folder.dst.dem,
            raster_paths_dict={
                "dem": self.folder.src.dem,
                "polder": self.folder.dst.tmp.polder,
                "waterdeel": self.folder.dst.tmp.waterdeel,
            },
            nodata_keys=["polder"],
            mask_keys=["polder", "dem"],
            metadata_key="polder",
            custom_run_window_function=run_dem_window,
            output_nodata=self.nodata,
            min_block_size=4096,
            verbose=self.verbose,
            tempdir=self.folder.dst.tmp,
        )
        # Run calculation of output raster
        self.dem_calc.run(overwrite=True)

        # raster creation
        def run_rtype_window(block):
            """Custom calc function on blocks in hrt.RasterCalculatorV2"""
            block_out = block.blocks["rtype"]

            # Nodatamasks toepassen
            block_out[block.masks_all] = self.nodata
            return block_out

        for rtype in ["glg", "ggg", "ghg", "infiltration", "friction"]:
            # init calculator
            raster_calc = hrt.RasterCalculatorV2(
                raster_out=getattr(self.folder.dst, rtype),
                raster_paths_dict={
                    "rtype": getattr(self.folder.src, rtype),
                    "polder": self.folder.dst.tmp.polder,
                },
                nodata_keys=["polder"],
                mask_keys=["polder", "rtype"],
                metadata_key="polder",
                custom_run_window_function=run_rtype_window,
                output_nodata=self.nodata,
                min_block_size=4096,
                verbose=self.verbose,
                tempdir=self.folder.dst.tmp,
            )
            # Run calculation of output raster
            raster_calc.run(overwrite=False)

    def run(self):
        self.prepare_input()
        self.create_rasters()


# %%
# import cProfile
# import pstats

# profiler = cProfile.Profile()
# profiler.enable()
# t()
# profiler.disable()
# stats = pstats.Stats(profiler)
# # stats.strip_dirs()
# stats.sort_stats("tottime")
# stats.print_stats()


# %% RXR


class ClipModelRasterCalc(hrt.RasterCalculatorRxr):
    def __init__(
        self,
        raster_out: hrt.Raster,
        raster_paths_dict: dict[str : hrt.Raster],
        metadata_key: str,
        verbose: bool = False,
        tempdir: hrt.Folder = None,
    ):
        """Create model raster with nodata outside polder.
        Bounds based on polder_polygon raster.
        """
        super().__init__(
            raster_out=raster_out,
            raster_paths_dict=raster_paths_dict,
            nodata_keys=[],  # TODO uitfaseren
            metadata_key=metadata_key,
            verbose=verbose,
            tempdir=tempdir,
        )

    def run(self, waterdeel_value=None, chunksize: Union[int, None] = None, overwrite: bool = False):
        """
        Parameters
        ----------
        waterdeel_value : float, default None
            Set all values in raster where waterdeel to this value
        chunksize : Union[int, None], optional
            _description_, by default None
        overwrite : bool, optional
            _description_, by default False
        """
        cont = self.verify(overwrite=overwrite)

        if cont:
            # Load DataArrays
            da_dict = {}
            da_out = da_dict["rtype"] = self.raster_paths_same_bounds["rtype"].open_rxr(chunksize)
            da_polder = da_dict["polder"] = self.raster_paths_same_bounds["polder"].open_rxr(chunksize)  # Nodata key

            if waterdeel_value is not None:
                # waterdeel ophogen naar 10mNAP (default voor dem)
                da_waterdeel = da_dict["waterdeel"] = self.raster_paths_same_bounds["waterdeel"].open_rxr(chunksize)
                da_out = xr.where(da_waterdeel == 1, waterdeel_value, da_out)

            # Create global no data mask
            nodata = np.nan

            da_nodatamasks = self.get_nodatamasks(da_dict=da_dict, nodata_keys=["polder"])
            da_out = xr.where(da_nodatamasks, nodata, da_out)  # Polder extent

            self.raster_out = hrt.Raster.write(self.raster_out, result=da_out, nodata=nodata, chunksize=chunksize)
        else:
            logger.info("Cont is false")


# %%
tempdir = folder_mb.dst.full_path(f"temp_{hrt.current_time(date=True)}")


for rtype in ["dem", "glg", "ggg", "ghg", "infiltration", "friction"]:
    # init calculator
    if rtype == "dem":
        waterdeel_value = 10
        raster_paths_dict = {
            "rtype": getattr(folder_mb.src, rtype),
            "polder": folder_mb.dst.polder,
            "waterdeel": folder_mb.dst.waterdeel,
        }

    else:
        waterdeel_value = None
        raster_paths_dict = {
            "rtype": getattr(folder_mb.src, rtype),
            "polder": folder_mb.dst.polder,
        }

    self = raster_calc = ClipModelRasterCalc(
        raster_out=getattr(folder_mb.dst, rtype),
        raster_paths_dict=raster_paths_dict,
        metadata_key="polder",
        verbose=verbose,
        tempdir=tempdir,
    )
    # Run calculation of output raster
    raster_calc.run(waterdeel_value=waterdeel_value, overwrite=False)


# %% ENDTEST
if __name__ == "__main__":
    verbose = True
    chunksize = None
    overwrite = False
