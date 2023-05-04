import os
import json
try:
    with open(os.getcwd() + "/notebook_data.json") as f:
        notebook_data = json.load(f)
except:
    pass
from hhnk_threedi_tools import Folders
import pandas as pd
import threedi_raster_edits as tre
from hhnk_threedi_tools import Folders
import geopandas as gpd
from osgeo import gdal
import hhnk_research_tools as hrt
from hhnk_research_tools.raster_functions import reproject
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import ThreediGrid
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
import zipfile


class Nabewerking_Klimaatsommen:
    def __init__(
        self,
        folder: Folders,

    ):
        self.fenv = folder
        self.model = self.fenv.model.schema_base.database.path
        self.dem = self.fenv.model.schema_base.rasters.dem.path
        self.threedi_results = self.fenv.threedi_results
        self.batch = self.fenv.threedi_results.batch

    def run_nabewerking_klimaatsommen(self):

        batch_name =  "batch_test"
        try:
            dem_path = self.dem
            dem = hrt.Raster(dem_path)

            if dem.metadata.pixel_width != 0.5:

                new_dem_path = self.batch [batch_name].downloads.pl/f"{dem.pl.stem}_05m.tif"
                if not new_dem_path.exists():
                    reproject(src = dem, 
                                target_res = 0.5,
                                output_path = new_dem_path
                    )

                dem = hrt.Raster(new_dem_path)

            for name in self.batch[batch_name].downloads.names:
                print(name)
                scenario = getattr((self.batch[batch_name].downloads), name)
                threedi_result = scenario.netcdf
                #Select result
                threedigrid = ThreediGrid(folder=self.fenv, threedi_result=threedi_result)

                #Convert netcdf to grid gpkg
                threedigrid.netcdf_to_grid_gpkg()

                #Replace waterlevel of selected cells with avg of neighbours.
                threedigrid.waterlevel_correction(output_col="wlvl_max_replaced")

            OVERWRITE=False

            #%%
            for name in self.threedi_results.batch[batch_name].downloads.names:
                print(name)
                
                scenario = getattr(self.batch[batch_name].downloads, name)
                
                threedi_result = scenario.netcdf

                grid_gdf = gpd.read_file(threedi_result.pl/"grid_corr.gpkg", driver="GPKG")


                calculator_kwargs = {"dem_path":dem.source_path,
                                        "grid_gdf":grid_gdf, 
                                        "wlvl_column":"wlvl_max_replaced"}

                #Init calculator
                with BaseCalculatorGPKG(**calculator_kwargs) as basecalc:
                    basecalc.run(output_folder=scenario.depth_max.pl.parent,  

                                output_raster_name=scenario.depth_max.pl.name,
                                mode="MODE_WDEPTH",
                                overwrite=OVERWRITE)
                    print("Done.")
            # %%
            from pathlib import Path
            schadeschatter_path = Path(r"E:\01.basisgegevens\hhnk_schadeschatter")
            import sys



            if str(schadeschatter_path) not in sys.path:
                sys.path.append(str(schadeschatter_path))


            sys.path.insert(1, r'E:\01.basisgegevens\hhnk_schadeschatter')
            import hhnk_schadeschatter as hhnk_wss
            for name in self.batch[batch_name].downloads.names:
                print(name)
                scenario = getattr(self.batch[batch_name].downloads, name)
                
                #Variables
                # cfg_file = schadeschatter_path/'01_data/cfg/cfg_hhnk_2020.cfg'
                cfg_file = schadeschatter_path/'01_data/cfg/cfg_lizard.cfg'
                
                
                landuse_file = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt"
    	        
                depth_file = scenario.depth_max.path
                
                output_file = scenario.damage_total.path
                
                wss_settings = {'inundation_period': 48, #uren
                                'herstelperiode':'10 dagen',
                                'maand':'sep',
                                'cfg_file':cfg_file,
                                'dmg_type':'gem'}

                #Calculation                
                wss = hhnk_wss.wss_main.Waterschadeschatter(depth_file=depth_file, 
                                        landuse_file=landuse_file, 
                                        output_file=output_file,
                                        wss_settings=wss_settings)

                # Aanmaken leeg output raster.
                wss.create_output_raster()

                # Berkenen schaderaster
                wss.run(initialize_output=False)

                # Once the rasters are done, we read the information stored in them and the compared that 
                # with the data from the CSV, which the data used to evaluate if the rasters are good or no.
            dems_location = self.batch[batch_name].downloads.path
            files = os.listdir(dems_location)
            damage_rasters = []
            depth_rasters = []
            file_names_damage = []
            min_damage= []
            max_damage = []
            mean_damage = []
            stdDev_damage = []
            west_damage = []
            north_damage = [] 
            damage_info = gpd.GeoDataFrame()

            location_file = self.threedi_results.batch['batch_test'].path

            for raster_file in files:
                if raster_file.endswith('.tif') and raster_file.split('_')[0]== 'damage':
                    path_damage = os.path.join(dems_location, raster_file)
                    damage_rasters.append(path_damage)
                elif raster_file.endswith('.tif') and raster_file.split('_')[0]== 'depth':
                    path_depth = os.path.join(dems_location, raster_file)
                    depth_rasters.append(path_depth)
                    
            for damage_raster in damage_rasters:
                file_name = Path(damage_raster).name
                file_names_damage.append(file_name)
                gtif = gdal.Open(damage_raster)
                srcband = gtif.GetRasterBand(1)
                stats =  srcband.GetStatistics(True, True) 
                min_damage.append(round(stats[0],6))
                max_damage.append(round(stats[1],6))
                mean_damage.append(round(stats[2],4))
                stdDev_damage.append(round(stats[3],4))
                metadata_damage = hrt.RasterMetadata(gtif)
                west_damage.append(metadata_damage.bounds[0])
                north_damage.append(metadata_damage.bounds[-1])

            damage_info['file name']  = file_names_damage
            damage_info['min'] = min_damage
            damage_info['max'] = max_damage
            damage_info['mean'] = mean_damage
            damage_info['StdDev']= stdDev_damage
            damage_info['bound west'] = west_damage
            damage_info['bound north'] = north_damage
            damage_info.set_index(['file name'], inplace = True)
                            
            # %%
            file_names_depth = []
            min_depth= []
            max_depth = []
            mean_depth = []
            stdDev_depth = []
            west_depth = []
            north_depth = [] 
            depth_info = gpd.GeoDataFrame()
            location_file = self.threedi_results.batch['batch_test'].path
            name ='raster_depth_info.csv'
            raster_csv_path = os.path.join(location_file, name)
            depth_data = pd.read_csv((raster_csv_path))

            for depth_raster in depth_rasters:
                file_name = Path(depth_raster).name
                file_names_depth.append(file_name)
                gtif = gdal.Open(depth_raster)
                srcband = gtif.GetRasterBand(1)
                stats =  srcband.GetStatistics(True, True) 
                min_depth.append(round(stats[0],2))
                max_depth.append(round(stats[1],2))
                mean_depth.append(round(stats[2],2))
                stdDev_depth.append(round(stats[3],2))
                metadata = hrt.RasterMetadata(gtif)
                west_depth.append(metadata.bounds[0])
                north_depth.append(metadata.bounds[-1])

            depth_info['file name']  = file_names_depth
            depth_info['min'] = min_depth
            depth_info['max'] = max_depth
            depth_info['mean'] = mean_depth
            depth_info['StdDev']= stdDev_depth
            depth_info['bound west'] = west_depth
            depth_info['bound north'] = north_depth
            depth_info.set_index(['file name'], inplace = True)

            frames = [ damage_info, depth_info]
            result = pd.concat(frames)
            return result
            # return(damage_info, depth_info)

        except Exception as e:
            raise e from None