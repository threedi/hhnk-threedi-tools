# %%
import os
from hhnk_threedi_tools import Folders
import pandas as pd
import geopandas as gpd
import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import ThreediGrid
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG

import sys
from pathlib import Path

class KlimaatsommenPrep:
    """Postprocessing of climate scenarios. This object will turn the 
    raw netcdf results into depth and damage rasters for each scenario. 
    """
    def __init__(
        self,
        folder: Folders,
        batch_name: str,
        cfg_file = "cfg_lizard.cfg",
        landuse_file:str = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
        verify=True,
    ):
        if type(cfg_file) == str:
            cfg_file = hrt.get_pkg_resource_path(package_resource=hrt.waterschadeschatter.resources, 
                                name=cfg_file)
            if not cfg_file.exists():
                raise Exception(f"{cfg_file} doesnt exist.")            

        self.folder = folder
        self.batch_fd = self.folder.threedi_results.batch[batch_name]

        self.cfg_file = cfg_file
        self.landuse_file = landuse_file
        
        if verify:
            self.verify_input()

    def verify_input(self):
        """Verify if we can run"""
        if not self.batch_fd.exists():
            raise Exception(f"INPUTERROR - {self.batch_fd.name} missing")
        
        netcdf_missing = [name for name in self.batch_fd.downloads.names if not getattr(self.batch_fd.downloads,name).netcdf.grid_path.exists()]
        if any(netcdf_missing):
            raise Exception(f"INPUTERROR - netcdf missing for scenarios; {netcdf_missing}")
        
        h5_missing = [name for name in self.batch_fd.downloads.names if not getattr(self.batch_fd.downloads,name).netcdf.admin_path.exists()]
        if any(h5_missing):
            raise Exception(f"INPUTERROR - h5 missing for scenarios; {h5_missing}")
        return True
    

    def get_dem(self):
        """Update dem resolution to 0.5m
        #Get the dem. If dem doesnt have correct resolution it will be reprojected to 0.5m
        """
        dem = self.folder.model.schema_base.rasters.dem

        #Reproject to 0.5m if necessary
        if dem.metadata.pixel_width != 0.5:
            new_dem_path = self.batch_fd.downloads.full_path(f"{dem.stem}_05m.tif")
            if not new_dem_path.exists():
                hrt.reproject(src = dem, 
                            target_res = 0.5,
                            output_path = new_dem_path
                )
            dem = hrt.Raster(new_dem_path)
            return dem
        else:
            return dem


    def get_scenario(self, name):
        """Get individual threediresult of a scenario.
        (e.g. netcdf folder of blok_gxg_T10)"""
        scenario = getattr((self.batch_fd.downloads), name)
        return scenario


    def netcdf_to_grid(self, 
                       threedi_result, 
                       corrected_col_name = "wlvl_max_replaced", 
                       grid_raw_filename = "grid_raw.gpkg",
                       grid_corr_filename = "grid_corr.gpkg", 
                       overwrite=False,
                       ):
        """Transform netcdf to grid gpkg and apply wlvl correction"""
        #Select result
        threedigrid = ThreediGrid(folder=self.folder, 
                                  threedi_result=threedi_result,
                                  grid_raw_filename = grid_raw_filename,
                                  grid_corr_filename = grid_corr_filename,
                                  )

        #Convert netcdf to grid gpkg
        threedigrid.netcdf_to_grid_gpkg(overwrite=overwrite)

        #Replace waterlevel of selected cells with avg of neighbours.
        threedigrid.waterlevel_correction(output_col=corrected_col_name, overwrite=overwrite)


    def calculate_raster(self, 
                         scenario_raster, 
                         threedi_result, 
                         mode, 
                         grid_filename="grid_corr.gpkg", 
                         wlvl_col_name="wlvl_max_replaced", 
                         overwrite=False,
                         ):
        """mode options are: "MODE_WDEPTH", "MODE_WLVL" """
        grid_gdf = threedi_result.full_path(grid_filename).load()

        calculator_kwargs = {"dem_path":self.dem.base,
                                "grid_gdf":grid_gdf, 
                                "wlvl_column":wlvl_col_name}

        output_file = scenario_raster

        #Init calculator
        with BaseCalculatorGPKG(**calculator_kwargs) as basecalc:
            basecalc.run(output_file=output_file,  
                        mode=mode,
                        overwrite=overwrite)
            

    def calculate_depth(self, 
                        scenario, 
                        threedi_result:hrt.ThreediResult, 
                        grid_filename="grid_corr.gpkg",
                        wlvl_col_name="wlvl_max_replaced", 
                        overwrite=False):
        scenario_raster = scenario.depth_max
        self.calculate_raster(scenario_raster=scenario_raster, 
                              threedi_result=threedi_result, 
                              mode= "MODE_WDEPTH",
                              grid_filename=grid_filename,
                              wlvl_col_name=wlvl_col_name,
                              overwrite=overwrite)


    def calculate_wlvl(self, 
                       scenario, 
                       threedi_result:hrt.ThreediResult, 
                       grid_filename="grid_corr.gpkg",
                       wlvl_col_name="wlvl_max_replaced", 
                       overwrite=False):
        scenario_raster = scenario.wlvl_max
        self.calculate_raster(scenario_raster=scenario_raster, 
                              threedi_result=threedi_result, 
                              mode= "MODE_WLVL", 
                              grid_filename=grid_filename,
                              wlvl_col_name=wlvl_col_name,
                              overwrite=overwrite)


    def calculate_damage(self, scenario, overwrite=False):
            #Variables
            depth_file = scenario.depth_max
            output_raster = scenario.damage_total
            
            wss_settings = {'inundation_period': 48, #uren
                            'herstelperiode':'10 dagen',
                            'maand':'sep',
                            'cfg_file':self.cfg_file,
                            'dmg_type':'gem'}

            if output_raster.exists() and not overwrite:
                return

            #Calculation
            wss = hrt.Waterschadeschatter(depth_file=depth_file, 
                                    landuse_file=self.landuse_file, 
                                    wss_settings=wss_settings)

            # Berekenen schaderaster
            wss.run(output_raster=output_raster, 
                    calculation_type="sum",
                    overwrite=overwrite)


    def run(self, gridgpkg=True, depth=True, dmg=True, wlvl=False, overwrite=False, testing=False):
        try:
            self.dem = self.get_dem()

            for name in self.batch_fd.downloads.names:
                scenario = self.get_scenario(name=name)
                threedi_result = scenario.netcdf

                #Transform netcdf to grid gpkg
                if gridgpkg:
                    self.netcdf_to_grid(threedi_result=threedi_result, 
                                        grid_raw_filename = "grid_raw.gpkg",
                                        grid_corr_filename = "grid_corr.gpkg",
                                        overwrite=overwrite)

                #Diepterasters berekenen
                if depth:
                    self.calculate_depth(scenario=scenario,
                                        threedi_result=threedi_result, 
                                        grid_filename="grid_corr.gpkg",
                                        overwrite=overwrite)

                #Schaderaster berekenen
                if dmg:
                    self.calculate_damage(scenario=scenario, 
                                          overwrite=overwrite)

                #Waterlevelraster berekenen
                if wlvl:
                    self.calculate_wlvl(scenario=scenario,
                                            threedi_result=threedi_result, 
                                            overwrite=overwrite)


                if testing:
                    #For pytests we dont need to run this 18 times
                    break

            self.create_scenario_metadata(overwrite=overwrite, testing=testing)
        except Exception as e:
            raise e from None


    def create_scenario_metadata(self, overwrite=False, testing=False):
        """Metadata of all scenarios together. """
        #TODO create checks on a result if they make sense 
        #e.g. total volume of scenarios should be higher with more precip.
        #Also bounds should be same.
        
        self.info_file = {}

        for raster_type in ["depth_max", "damage_total"]:
            self.info_file[raster_type] = self.batch_fd.full_path(f"{raster_type}_info.csv")
            
            info_df = gpd.GeoDataFrame()

            #Get statistics for all 18 scenarios
            for name in self.batch_fd.downloads.names:
                scenario = self.get_scenario(name=name)

                info_row = self._scenario_metadata_row(scenario=scenario,
                                                        raster_type=raster_type)
                #Add row to df
                info_df = info_df.append(info_row, ignore_index=True)

                if testing:
                    #For pytests we dont need to run this 18 times
                    break

            #Write to file

            info_df.set_index(['filename'], inplace=True)
            info_df.to_csv(self.info_file[raster_type].path, sep=';')
        

    def _scenario_metadata_row(self, scenario, raster_type) -> pd.Series:
        """Raster statistics for single scenario"""
        raster = getattr(scenario, raster_type)
   
        stats = raster.statistics(approve_ok=True, force=True)

        #Fill row data
        info_row = pd.Series(dtype=object)
        info_row['filename']  = raster.stem
        info_row['min'] = stats["min"]
        info_row['max'] = stats["max"]
        info_row['mean'] = stats["mean"]
        info_row['std'] = stats["std"]
        info_row['bounds'] = raster.metadata.bounds
        info_row['x_res'] = str(raster.metadata.x_res)
        info_row['y_res'] = str(raster.metadata.y_res)
        
        return info_row
    
# %%
if __name__ == "__main__":

    from pathlib import Path
    from hhnk_threedi_tools import Folders

    # TEST_MODEL = Path(__file__).parent.parent.parent.parent / "tests/data/model_test/"
    TEST_MODEL = r"E:\02.modellen\model_test_v2"
    folder = Folders(TEST_MODEL)

    self = KlimaatsommenPrep(folder=folder,
        batch_name="batch_test2",
        cfg_file = 'cfg_lizard.cfg',
        landuse_file = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
        verify=True
    )

    self.run(overwrite=False)
