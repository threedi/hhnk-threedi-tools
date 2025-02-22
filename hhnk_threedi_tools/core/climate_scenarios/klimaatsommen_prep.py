# %%
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd

from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.result_rasters.grid_to_raster import GridToWaterDepth, GridToWaterLevel
from hhnk_threedi_tools.core.result_rasters.grid_to_raster_old import GridToRaster
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import NetcdfToGPKG

logger = hrt.logging.get_logger(__name__)


class KlimaatsommenPrep:
    """Postprocessing of climate scenarios. This object will turn the
    raw netcdf results into depth and damage rasters for each scenario.
    """

    def __init__(
        self,
        folder: Folders,
        batch_name: str,
        cfg_file="cfg_lizard.cfg",
        landuse_file: str = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
        min_block_size=1024,
        use_aggregate: bool = False,
        verify=True,
        old_wlvl=False,
    ):
        """
        Parameters
        ----------
        old_wlvl : bool #TODO Deprecate in 2025.2
            Use the Deprecated GridToRaster to calculate the wlvl and wdepth.
        """
        if isinstance(cfg_file, str):
            cfg_file = hrt.get_pkg_resource_path(package_resource=hrt.waterschadeschatter.resources, name=cfg_file)
            if not cfg_file.exists():
                raise FileNotFoundError(f"{cfg_file} doesnt exist.")

        self.folder = folder
        self.batch_fd = self.folder.threedi_results.batch[batch_name]

        self.cfg_file = cfg_file
        self.landuse_file = landuse_file
        self.min_block_size = min_block_size
        self.use_aggregate = use_aggregate
        self.old_wlvl = old_wlvl

        if verify:
            self.verify_input()

    def verify_input(self):
        """Verify if we can run"""
        if not self.batch_fd.exists():
            raise FileNotFoundError(f"INPUTERROR - batchfolder {self.batch_fd.name} missing")

        grid_name = "grid_path"
        if self.use_aggregate:
            grid_name = "aggregate_grid_path"

        netcdf_missing = [
            name
            for name in self.batch_fd.downloads.names
            if not getattr(getattr(self.batch_fd.downloads, name).netcdf, grid_name).exists()
        ]
        if any(netcdf_missing):
            raise FileNotFoundError(f"INPUTERROR - netcdf missing for scenarios; {netcdf_missing}")

        h5_missing = [
            name
            for name in self.batch_fd.downloads.names
            if not getattr(self.batch_fd.downloads, name).netcdf.admin_path.exists()
        ]
        if any(h5_missing):
            raise FileNotFoundError(f"INPUTERROR - h5 missing for scenarios; {h5_missing}")
        return True

    def get_dem(self):
        """Update dem resolution to 0.5m
        #Get the dem. If dem doesnt have correct resolution it will be reprojected to 0.5m
        """
        dem = self.folder.model.schema_base.rasters.dem

        # Reproject to 0.5m if necessary
        # TODO use schadedem?
        if dem.metadata.pixel_width != 0.5:
            new_dem_path = self.batch_fd.downloads.full_path(f"{dem.stem}_50cm.tif")
            if not new_dem_path.exists():
                dem = hrt.Raster.reproject(src=dem, dst=new_dem_path, target_res=0.5)
            else:
                dem = hrt.Raster(new_dem_path)
            return dem
        else:
            return dem

    def get_scenario(self, name):
        """Get individual threediresult of a scenario.
        (e.g. netcdf folder of blok_gxg_T10)
        """
        scenario = getattr((self.batch_fd.downloads), name)
        return scenario

    def netcdf_to_grid(
        self,
        threedi_result: hrt.ThreediResult,
        grid_filename: str = "grid_wlvl.gpkg",
        overwrite: bool = False,
    ):
        """Transform netcdf to grid gpkg and apply wlvl correction
        output will be stored in wlvl_corr_max column
        """
        # Select result
        netcdf_gpkg = NetcdfToGPKG.from_folder(
            folder=self.folder,
            threedi_result=threedi_result,
            use_aggregate=self.use_aggregate,
        )

        # Convert netcdf to grid gpkg
        netcdf_gpkg.run(
            output_file=threedi_result.full_path(grid_filename),
            timesteps_seconds=["max"],
            overwrite=overwrite,
        )

    def calculate_wlvl_wdepth_rasters(
        self,
        wlvl_raster,
        wdepth_raster,
        threedi_result: hrt.ThreediResult,
        create_wdepth: bool = True,
        grid_filename: str = "grid_wlvl.gpkg",
        wlvl_col_name: str = "wlvl_corr_max",
        overwrite: bool = False,
    ):
        """Mode options are: 'MODE_WDEPTH', 'MODE_WLVL'"""
        grid_gdf = threedi_result.full_path(grid_filename).load()

        calculator_kwargs = {
            "dem_path": self.dem.base,
            "grid_gdf": grid_gdf,
            "wlvl_column": wlvl_col_name,
        }

        if not self.old_wlvl:
            # Create wlvl raster
            with GridToWaterLevel(**calculator_kwargs) as wlvlcalc:
                wlvlcalc.run(output_file=wlvl_raster, chunksize=self.min_block_size, overwrite=overwrite)

            if create_wdepth:
                # Create wdepth raster
                with GridToWaterDepth(
                    dem_path=self.dem.base,
                    wlvl_path=wlvl_raster,
                ) as wdepth_calc:
                    wdepth_calc.run(output_file=wdepth_raster, chunksize=self.min_block_size, overwrite=overwrite)
        else:
            # TODO Old depth calculation. Depecrate in next update.
            with GridToRaster(**calculator_kwargs) as basecalc:
                basecalc.run(
                    output_file=wlvl_raster,
                    mode="MODE_WLVL",
                    min_block_size=self.min_block_size,
                    overwrite=overwrite,
                )

            # Init calculator
            with GridToRaster(**calculator_kwargs) as basecalc:
                basecalc.run(
                    output_file=wdepth_raster,
                    mode="MODE_WDEPTH",
                    min_block_size=self.min_block_size,
                    overwrite=overwrite,
                )

    def calculate_damage(self, scenario, overwrite=False):
        # Variables
        depth_file = scenario.depth_max
        output_raster = scenario.damage_total

        wss_settings = {
            "inundation_period": 48,  # uren
            "herstelperiode": "10 dagen",
            "maand": "sep",
            "cfg_file": self.cfg_file,
            "dmg_type": "gem",
        }

        if output_raster.exists() and not overwrite:
            return

        # Calculation
        wss = hrt.Waterschadeschatter(
            depth_file=depth_file,
            landuse_file=self.landuse_file,
            wss_settings=wss_settings,
            min_block_size=self.min_block_size,
        )

        # Berekenen schaderaster
        wss.run(output_raster=output_raster, calculation_type="sum", overwrite=overwrite)

    def run(
        self,
        gridgpkg=True,
        wlvl_wdepth=True,
        create_wdepth=True,
        dmg=True,
        overwrite=False,
        testing=False,
        verbose=False,
    ):
        try:
            self.dem = self.get_dem()

            for name in self.batch_fd.downloads.names:
                logger.info(name)
                scenario = self.get_scenario(name=name)
                threedi_result = scenario.netcdf

                # Transform netcdf to grid gpkg
                if gridgpkg:
                    if verbose:
                        print("     netcdf to gpkg")
                    self.netcdf_to_grid(
                        threedi_result=threedi_result,
                        grid_filename="grid_wlvl.gpkg",
                        overwrite=overwrite,
                    )

                # Create wlvl and wdepth raster
                if wlvl_wdepth:
                    wlvl_raster = scenario.wlvl_max
                    wdepth_raster = scenario.depth_max
                    if verbose:
                        print("     create wlvl and wdepth raster")
                    self.calculate_wlvl_wdepth_rasters(
                        wlvl_raster=wlvl_raster,
                        wdepth_raster=wdepth_raster,
                        threedi_result=threedi_result,
                        create_wdepth=create_wdepth,
                        grid_filename="grid_wlvl.gpkg",
                        wlvl_col_name="wlvl_corr_max",
                        overwrite=overwrite,
                    )

                # Schaderaster berekenen
                if dmg:
                    if verbose:
                        print("     create damage raster")
                    self.calculate_damage(scenario=scenario, overwrite=overwrite)

                if testing:
                    # For pytests we dont need to run this 18 times
                    break

            self.create_scenario_metadata(overwrite=overwrite, testing=testing)
        except Exception as e:
            raise e from None

    def create_scenario_metadata(self, overwrite=False, testing=False):
        """Metadata of all scenarios together."""
        # TODO create checks on a result if they make sense
        # e.g. total volume of scenarios should be higher with more precip.
        # Also bounds should be same.

        self.info_file = {}

        for raster_type in ["depth_max", "damage_total"]:
            self.info_file[raster_type] = self.batch_fd.full_path(f"{raster_type}_info.csv")

            data = []

            # Get statistics for all 18 scenarios
            for name in self.batch_fd.downloads.names:
                scenario = self.get_scenario(name=name)

                data += [self._scenario_metadata_row(scenario=scenario, raster_type=raster_type)]
                # Add row to df

                if testing:
                    # For pytests we dont need to run this 18 times
                    break

            # Write to file
            info_df = gpd.GeoDataFrame(data)
            info_df.set_index(["filename"], inplace=True)
            info_df.to_csv(self.info_file[raster_type].path, sep=";")

    def _scenario_metadata_row(self, scenario, raster_type) -> pd.Series:
        """Raster statistics for single scenario"""
        raster = getattr(scenario, raster_type)

        stats = raster.statistics()

        # Fill row data
        info_row = pd.Series(dtype=object)
        info_row["filename"] = raster.stem
        info_row["min"] = stats["min"]
        info_row["max"] = stats["max"]
        info_row["mean"] = stats["mean"]
        info_row["std"] = stats["std"]
        info_row["bounds"] = raster.metadata.bounds
        info_row["x_res"] = str(raster.metadata.x_res)
        info_row["y_res"] = str(raster.metadata.y_res)

        return info_row


# %%
if __name__ == "__main__":
    from pathlib import Path

    from hhnk_threedi_tools import Folders

    TEST_MODEL = r"E:\02.modellen\HKC23010_Eijerland_WP"
    folder = Folders(TEST_MODEL)

    self = KlimaatsommenPrep(
        folder=folder,
        batch_name="nhflo_gxg",
        cfg_file="cfg_lizard.cfg",
        landuse_file=r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
        use_aggregate=True,
        verify=True,
    )

    self.run(overwrite=False, dmg=False, verbose=True)

# %%
