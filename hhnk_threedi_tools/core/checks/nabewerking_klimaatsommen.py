import os
import json
try:
    with open(os.getcwd() + "/notebook_data.json") as f:
        notebook_data = json.load(f)
except:
    pass
from hhnk_threedi_tools import Folders
import pandas as pd
import geopandas as gpd
from osgeo import gdal
import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import ThreediGrid
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
import zipfile

import sys
from pathlib import Path

SCHADESCHATTER_PATH = Path(r"E:\01.basisgegevens\hhnk_schadeschatter")
if str(SCHADESCHATTER_PATH) not in sys.path:
    sys.path.insert(1, str(SCHADESCHATTER_PATH))
import hhnk_schadeschatter as hhnk_wss


class KlimaatsommenPrep:
    """Postprocessing of climate scenarios. This object will turn the 
    raw netcdf results into depth and damage rasters for each scenario. 
    """
    def __init__(
        self,
        folder: Folders,
        batch_name: str,
        cfg_file:Path = SCHADESCHATTER_PATH/'01_data/cfg/cfg_lizard.cfg',
        landuse_file:str = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
    ):
        self.folder = folder
        self.batch_fd = self.folder.threedi_results.batch[batch_name]

        self.cfg_file = cfg_file
        self.landuse_file = landuse_file


    def get_dem(self):
        """Update dem resolution to 0.5m
        #Get the dem. If dem doesnt have correct resolution it will be reprojected to 0.5m
        """
        dem_path = self.folder.model.schema_base.rasters.dem.path
        dem = hrt.Raster(dem_path)

        if dem.metadata.pixel_width != 0.5:
            #Reproject to 0.5m if necessary
            new_dem_path = self.batch_fd.downloads.pl/f"{dem.pl.stem}_05m.tif"
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


    def netcdf_to_grid(self, threedi_result, corrected_col_name="wlvl_max_replaced"):
        """Transform netcdf to grid gpkg and apply wlvl correction"""
        #Select result
        threedigrid = ThreediGrid(folder=self.folder, threedi_result=threedi_result)

        #Convert netcdf to grid gpkg
        threedigrid.netcdf_to_grid_gpkg()

        #Replace waterlevel of selected cells with avg of neighbours.
        threedigrid.waterlevel_correction(output_col=corrected_col_name)


    def calculate_raster(self, scenario_raster, threedi_result, mode, corrected_col_name="wlvl_max_replaced", overwrite=False):
        """mode options are: "MODE_WDEPTH", "MODE_WLVL" """
        grid_gdf = gpd.read_file(threedi_result.pl/"grid_corr.gpkg", driver="GPKG")

        calculator_kwargs = {"dem_path":self.dem.source_path,
                                "grid_gdf":grid_gdf, 
                                "wlvl_column":corrected_col_name}

        #Init calculator
        with BaseCalculatorGPKG(**calculator_kwargs) as basecalc:
            basecalc.run(output_folder=scenario_raster.pl.parent,  
                        output_raster_name=scenario_raster.pl.name,
                        mode=mode,
                        overwrite=overwrite)
            

    def calculate_depth(self, scenario, threedi_result, corrected_col_name="wlvl_max_replaced", overwrite=False):
        scenario_raster = scenario.depth_max
        self.calculate_raster(scenario_raster=scenario_raster, 
                              threedi_result=threedi_result, 
                              mode= "MODE_WDEPTH", 
                              corrected_col_name="wlvl_max_replaced",
                              overwrite=overwrite)


    def calculate_wlvl(self, scenario, threedi_result, corrected_col_name="wlvl_max_replaced", overwrite=False):
        scenario_raster = scenario.wlvl_max
        self.calculate_raster(scenario_raster=scenario_raster, 
                              threedi_result=threedi_result, 
                              mode= "MODE_WLVL", 
                              corrected_col_name="wlvl_max_replaced",
                              overwrite=overwrite)


    def calculate_damage(self, scenario):
            #Variables
            depth_file = scenario.depth_max.path
            output_file = scenario.damage_total.path
            
            wss_settings = {'inundation_period': 48, #uren
                            'herstelperiode':'10 dagen',
                            'maand':'sep',
                            'cfg_file':self.cfg_file,
                            'dmg_type':'gem'}

            #Calculation                
            wss = hhnk_wss.wss_main.Waterschadeschatter(depth_file=depth_file, 
                                    landuse_file=self.landuse_file, 
                                    output_file=output_file,
                                    wss_settings=wss_settings)

            # Aanmaken leeg output raster.
            wss.create_output_raster()

            # Berekenen schaderaster
            wss.run(initialize_output=False)


    def run(self, gridgpkg=True, depth=True, dmg=True, wlvl=False):
        try:
            self.dem = self.get_dem()

            for name in self.batch_fd.downloads.names:
                scenario = self.get_scenario(name=name)
                threedi_result = scenario.netcdf

                #Transform netcdf to grid gpkg
                if gridgpkg:
                    self.netcdf_to_grid(threedi_result=threedi_result)

                #Diepterasters berekenen
                if depth:
                    self.calculate_depth(scenario=scenario,
                                        threedi_result=threedi_result)

                #Schaderaster berekenen
                if dmg:
                    self.calculate_damage(scenario=scenario)

                #Waterlevelraster berekenen
                if wlvl:
                    self.calculate_wlvl(scenario=scenario,
                                            threedi_result=threedi_result)

        except Exception as e:
            raise e from None


    def create_scenario_metadata(self):
        """Metadata of all scenarios together. """
        #TODO create checks on a result if they make sense 
        #e.g. total volume of scenarios should be higher with more precip.

        for raster_type in ["depth_max", "damage_total"]:
            info_df = gpd.GeoDataFrame()

            #Get statistics for all 18 scenarios
            for name in self.batch_fd.names:
                scenario = self.get_scenario(name=name)

                info_row = self._scenario_metadata_row(scenario=scenario,
                                                        raster_type=raster_type)
                #Add row to df
                info_df.append(info_row, ignore_index=True)

            #Write to file
            output_path = self.batch_fd.full_file(f"{raster_type}_info.csv")

            info_df.set_index(['filename'], inplace=True)
            info_df.to_file(output_path)

        

    def _scenario_metadata_row(self, scenario, raster_type):
        raster = hrt.Raster(getattr(scenario, raster_type))
        band = raster.open_gdal_source().GetRasterBand(1)
        
        #Calculate statistics
        stats =  band.GetStatistics(bApproxOK=True, bForce=True) 

        #Fill row data
        info_row = pd.Series()
        info_row['filename']  = scenario.name
        info_row['min'] = round(stats[0],6)
        info_row['max'] = round(stats[1],6)
        info_row['mean'] = round(stats[2],4)
        info_row['StdDev']= round(stats[3],4)
        info_row['bounds'] = raster.bounds
        return info_row
    
# %%
if __name__ == "__main__":

    from pathlib import Path
    from hhnk_threedi_tools import Folders

    TEST_MODEL = Path(__file__).parent.parent.parent.parent / "tests/data/model_test/"
    folder = Folders(TEST_MODEL)

    self = KlimaatsommenPrep(folder=folder,
        batch_name="batch_test",
        cfg_file = SCHADESCHATTER_PATH/'01_data/cfg/cfg_lizard.cfg',
        landuse_file = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
    ):





