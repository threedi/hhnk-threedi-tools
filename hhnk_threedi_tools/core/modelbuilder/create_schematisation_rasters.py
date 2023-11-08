# %%
# Aanmaken rasters voor model
import hhnk_research_tools as hrt
from dataclasses import dataclass
import geopandas as gpd
import numpy as np
import os
import hhnk_research_tools.raster_functions as raster_functions
import hhnk_threedi_tools.core.folders_modelbuilder as folders_modelbuilder
import hhnk_threedi_tools as htt

if __name__ == "__main__":
    from pathlib import Path
    TEST_DIRECTORY = Path(__file__).parents[3].absolute() / "tests" / "data"

    PATH_TEST_MODEL = TEST_DIRECTORY / "model_test"
    TEMP_DIR = hrt.Folder(TEST_DIRECTORY/r"temp", create=True)
    TEMP_DIR.unlink_contents()

# %%
import importlib
importlib.reload(folders_modelbuilder)

raster_path = hrt.Folder(r"C:\Users\wiets\Documents\HHNK\07.Poldermodellen\10.tHoekje\02. Sqlite modeldatabase\rasters")

source_paths = folders_modelbuilder.SourcePaths(
    dem_path = raster_path.full_path("dem_thoekje.tif"),
    glg_path = raster_path.full_path("storage_glg_thoekje.tif"),
    ggg_path = raster_path.full_path("storage_ggg_thoekje.tif"),
    ghg_path = raster_path.full_path("storage_ghg_thoekje.tif" ),
    polder_path = r"C:\Users\wiets\Documents\GitHub\hhnk-threedi-tools\tests\data\model_test\01_source_data\polder_polygon.shp",
    watervlakken_path = r"C:\Users\wiets\Documents\GitHub\hhnk-threedi-tools\tests\data\model_test\01_source_data\modelbuilder_output\channel_surface_from_profiles.shp"
)

folder = folders_modelbuilder.FoldersModelbuilder(dst_path = TEMP_DIR.base,
    source_paths=source_paths
)


# %%
class ModelbuilderRasters:
    def __init__(self, 
        folder:htt.FoldersModelBuilder, 
        resolution:int = 0.5, 
        nodata:int = -9999,
        overwrite:bool = False,
    ):
        self.folder = folder
        self.resolution = resolution
        self.nodata = nodata
        self.overwrite = overwrite


    def prepare_input(self):
        """
        Rasterize polder and watervlakken
        Create vrt of source dem and gxg with bounds of polder raster.
        """
        #Rasterize polder polygon
        if not self.folder.dst.tmp.polder.exists():
            gdf_polder = gpd.read_file(self.folder.src.polder.path)
            metadata = hrt.create_meta_from_gdf(gdf_polder, 
                                                res=self.resolution)
            gdf_polder["value"] = 1
            hrt.gdf_to_raster(
                gdf=gdf_polder,
                value_field="value",
                raster_out=self.folder.dst.tmp.polder,
                nodata=self.nodata,
                metadata=metadata,
                read_array=False
            )

        #Rasterize watergangen
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
                overwrite=False
            )

        #Build vrt with correct extents of input rasters
        for key in ["dem", "glg", "ggg", "ghg"]:
            output_file = getattr(self.folder.dst.tmp, key)
            output_file.build_vrt(overwrite=False, 
                            bounds=eval(self.folder.dst.tmp.polder.metadata.bbox), 
                            input_files=getattr(self.folder.src, key), 
                            resolution=0.5,
            )


    def create_rasters(self):
        """Create output rasters dem and gxg"""

        #Dem creation
        def run_dem_window(block):
            """custom calc function on blocks in hrt.RasterCalculatorV2"""
            block_out = block.blocks['dem']

            #Watervlakken ophogen naar +10mNAP
            block_out[block.blocks['watervlakken']==1] = 10

            block_out[block.masks_all] = self.nodata
            return block_out


        # init calculator
        self.dem_calc = hrt.RasterCalculatorV2(
            raster_out=folder.dst.dem,
            raster_paths_dict={
                "dem" : folder.dst.tmp.dem,
                "polder" : folder.dst.tmp.polder,
                "watervlakken" : folder.dst.tmp.watervlakken,},
            nodata_keys=["polder"],
            mask_keys=["polder", "dem"],
            metadata_key="polder",
            custom_run_window_function=run_dem_window,
            output_nodata=self.nodata,
            min_block_size=4096,
            verbose=True,
        )
        #Run calculation of output raster
        self.dem_calc.run(overwrite=True)
    

        #gxg raster creation
        def run_gxg_window(block):
            """custom calc function on blocks in hrt.RasterCalculatorV2"""
            block_out = block.blocks['gxg']

            #Nodatamasks toepassen
            block_out[block.masks_all] = self.nodata
            return block_out


        for gxg in ["glg", "ggg", "ghg"]:
            # init calculator
            gxg_calc = hrt.RasterCalculatorV2(
                raster_out=getattr(self.folder.dst, gxg),
                raster_paths_dict={
                    "gxg" : getattr(self.folder.dst.tmp, gxg),
                    "polder" : self.folder.dst.tmp.polder,},
                nodata_keys=["polder"],
                mask_keys=["polder", "gxg"],
                metadata_key="polder",
                custom_run_window_function=run_gxg_window,
                output_nodata=self.nodata,
                min_block_size=4096,
                verbose=True,
            )
            #Run calculation of output raster
            gxg_calc.run(overwrite=False)



# %%
%timeit -r1 -n1 t()
# %%
%load_ext line_profiler
# %%
# %lprun -f hrt.Raster.write_array t()

# %%
%lprun -f t t()

# %%
import cProfile
import cProfile, pstats
profiler = cProfile.Profile()
profiler.enable()
t()
profiler.disable()
stats = pstats.Stats(profiler)
# stats.strip_dirs()
stats.sort_stats('tottime')
stats.print_stats()
#vrt van dem met juiste extent/resolutie
#watergangen rasterizen
#polder polygon rasterizen voor nodata doordruk


#vrt+

# %%

# 
#  rem De DEM wordt uitgeknipt uit de gebiedsdekkende DEM, de verrasterde watervlakken worden gebruikt om de DEM 
# rem dicht te smeren. 
# rem Een masker wordt gemaakt van de DEM om data/nodata pixels te onderscheiden. Gebiedsdekkende bodemberging 
# rem (ghg, glg, ggg), frictie en infiltratie raster worden op dit masker geprojecteerd en gecomprimeerd. 
# rem Rasters staat nu vast op 0.5 meter resolutie. 

# rem PS=$(python3 /code/modelbuilder/pixelsize.py "/code/tmp/rasters/tmp/channelsurface.tif")
# rem echo $PS
# rem PS=0.5

# echo INFO knip dem uit ahn met resolutie 0.5 m
# gdalwarp -cutline \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\polder.shp -tr 0.5 0.5 -tap -crop_to_cutline -srcnodata -9999 -dstnodata -9999 -co "COMPRESS=DEFLATE" "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\DEM\DEM_AHN4_int.vrt" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\raw_dem_clipped.tif

# echo INFO rasterize shapefile deelgebied.channelsurfacedem
# gdal_rasterize -a_nodata -9999 -a_srs EPSG:28992 -co "COMPRESS=DEFLATE" -tr 0.5 0.5  -burn 10.0 -l channelsurface \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\channelsurface.shp \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\channelsurface.tif

# echo INFO smeer watergangen dicht en comprimeer
# gdalwarp -ot Float32 -dstnodata -9999 -tr 0.5 0.5 -tap \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\raw_dem_clipped.tif \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\channelsurface.tif -ot Float32 -co "COMPRESS=DEFLATE" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif

# echo INFO Knip bodemberging, frictie en infiltratie uit gebiedsbrede rasters
# echo -----------------------------------
# echo INFO maak raster met enen voor extent
# rem dit werkt niet, iets met io.py dat hij niet kan vinden. Waarom python 37?
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif --calc="1" rem --quiet
# rem gdal niet gevonden: gdal_calc --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif --calc="1"
# rem DLL error: C:\PROGRA~1\3DIMOD~1.28\apps\Python39\python.exe C:\PROGRA~1\3DIMOD~1.28\apps\Python39\Scripts\gdal_calc.py
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif --calc="1" --quiet

# echo INFO maak rasters om te vullen
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulrasternul.tif --calc="0" --quiet
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulrasternul.tif --calc="0" --quiet

# copy \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulrasternul.tif \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_infiltration.tif 
# copy \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulrasternul.tif \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_berging.tif 

# echo INFO plak eenmalig bodemberging verhard in het vulraster
# gdalwarp "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\bodemberging\bodemberging_verhard_hhnk.tif" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_berging.tif 

# echo INFO maak drie vulrasters voor de berging
# copy \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_berging.tif \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ghg_ongec.tif 
# copy \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_berging.tif \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_glg_ongec.tif 
# copy \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_berging.tif \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ggg_ongec.tif 
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_friction.tif --calc="0.2" --quiet
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\dem_%2.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_friction.tif --calc="0.2" --quiet

# echo INFO plak de waarden voor bodemberging in de vulrasters
# gdalwarp "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\bodemberging\bodemberging_hhnk_ghg_m.tif" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ghg_ongec.tif
# gdalwarp "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\bodemberging\bodemberging_hhnk_ggg_m.tif" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ggg_ongec.tif
# gdalwarp "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\bodemberging\bodemberging_hhnk_glg_m.tif" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_glg_ongec.tif

# echo INFO vul rasters voor infiltratie en frictie
# gdalwarp "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\weerstand\friction_hhnk_2021.tif" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_friction.tif
# gdalwarp "\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\infiltratie\infiltratie_hhnk.tif" \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_infiltration.tif

# echo INFO pas extent toe op gevulde rasters
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ghg_ongec.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\storage_ghg_%2.tif --calc="A*B" --quiet
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ggg_ongec.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\storage_ggg_%2.tif --calc="A*B" --quiet
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_glg_ongec.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\storage_glg_%2.tif --calc="A*B" --quiet
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_friction.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\friction_%2.tif --calc="A*B" --quiet
# rem c:\OSGeo4W64\apps\Python37\python.exe c:\OSGeo4W64\apps\Python37\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_infiltration.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\infiltration_%2.tif --calc="A*B" --quiet

# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ghg_ongec.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\storage_ghg_%2.tif --calc="A*B" --quiet
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_ggg_ongec.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\storage_ggg_%2.tif --calc="A*B" --quiet
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_glg_ongec.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\storage_glg_%2.tif --calc="A*B" --quiet
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_friction.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\friction_%2.tif --calc="A*B" --quiet
# C:\ProgramData\Anaconda3\envs\threedipy\python.exe C:\ProgramData\Anaconda3\envs\threedipy\Scripts\gdal_calc.py --co="COMPRESS=DEFLATE" --NoDataValue -9999 -A \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\enenraster_ongec.tif -B \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\tmp\vulraster_infiltration.tif --outfile \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters\infiltration_%2.tif --calc="A*B" --quiet

# rem echo INFO comprimeer eindresultaat
# rem gdal_translate -co "COMPRESS=DEFLATE" /code/tmp/rasters/tmp/vulraster_ghg_ongec_ext.tif /code/tmp/rasters/storage_ghg_%2.tif
# rem gdal_translate -co "COMPRESS=DEFLATE" /code/tmp/rasters/tmp/vulraster_ggg_ongec_ext.tif /code/tmp/rasters/storage_ggg_%2.tif
# rem gdal_translate -co "COMPRESS=DEFLATE" /code/tmp/rasters/tmp/vulraster_glg_ongec_ext.tif /code/tmp/rasters/storage_glg_%2.tif
# rem gdal_translate -co "COMPRESS=DEFLATE" /code/tmp/rasters/tmp/vulraster_friction_ext.tif /code/tmp/rasters/friction_%2.tif
# rem gdal_translate -co "COMPRESS=DEFLATE" /code/tmp/rasters/tmp/vulraster_infiltration_ext.tif /code/tmp/rasters/infiltration_%2.tif

# echo INFO verwijder tijdelijke bestanden
# rem rmdir /s /q .\code\tmp\rasters\tmp
# xcopy /E /I /Y \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\tmp\rasters \\corp.hhnk.nl\data\Hydrologen_data\Data\modelbuilder\data\output\models\rasters
# rem rmdir /s /q .\code\tmp
# echo Klaar tmp_data