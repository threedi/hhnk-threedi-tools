# %%
"""Aanmaken rasters voor model"""

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools as htt


# %%
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
        Rasterize polder and watervlakken
        Create vrt of source dem and gxg with bounds of polder raster.
        """
        # Rasterize polder polygon
        if not self.folder.dst.tmp.polder.exists():
            gdf_polder = gpd.read_file(self.folder.src.polder.path)
            metadata = hrt.RasterMetadataV2.from_gdf(gdf_polder, res=self.resolution)
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
        if not self.folder.dst.tmp.watervlakken.exists():
            gdf = gpd.read_file(self.folder.src.watervlakken.path)
            metadata = self.folder.dst.tmp.polder.metadata
            gdf["value"] = 1
            hrt.gdf_to_raster(
                gdf=gdf,
                value_field="value",
                raster_out=self.folder.dst.tmp.watervlakken,
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

            # Watervlakken ophogen naar +10mNAP
            block_out[block.blocks["watervlakken"] == 1] = 10

            block_out[block.masks_all] = self.nodata
            return block_out

        # init calculator
        self.dem_calc = hrt.RasterCalculatorV2(
            raster_out=self.folder.dst.dem,
            raster_paths_dict={
                "dem": self.folder.src.dem,
                "polder": self.folder.dst.tmp.polder,
                "watervlakken": self.folder.dst.tmp.watervlakken,
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
