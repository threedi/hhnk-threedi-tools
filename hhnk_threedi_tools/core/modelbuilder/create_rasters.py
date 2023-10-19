# %%
# Aanmaken rasters voor model
import hhnk_research_tools as hrt
from dataclasses import dataclass
import geopandas as gpd
import numpy as np


@dataclass
class SourcePaths:
    path = hrt.Folder(r"C:\Users\wiets\Documents\HHNK\07.Poldermodellen\10.tHoekje\02. Sqlite modeldatabase\rasters")

    dem = path.full_path("dem_thoekje.tif")
    glg = path.full_path("storage_glg_thoekje.tif")
    ggg = path.full_path("storage_ggg_thoekje.tif")
    ghg = path.full_path("storage_ghg_thoekje.tif")    
    polder = hrt.File(r"C:\Users\wiets\Documents\GitHub\hhnk-threedi-tools\tests\data\model_test\01_source_data\polder_polygon.shp")
    watervlakken = hrt.File(r"C:\Users\wiets\Documents\GitHub\hhnk-threedi-tools\tests\data\model_test\01_source_data\modelbuilder_output\channel_surface_from_profiles.shp")

    def __init__(self):
        self.verify()

    def verify(self):
        for f in [self.dem, self.glg, self.ggg, 
                  self.ghg, self.polder, self.watervlakken]:
            if not f.exists():
                raise Exception(f"{f} not found")


class TempPaths(hrt.Folder):
    def __init__(self):
        super().__init__(base="tmp")

        self.dem = self.full_path("dem.vrt")
        self.glg = self.full_path("glg.vrt")
        self.ggg = self.full_path("ggg.vrt")
        self.ghg = self.full_path("ghg.vrt")    
        self.polder = self.full_path("polder.tif")
        self.watervlakken = self.full_path("watervlakken.tif")


class DestPaths:
    tmp = TempPaths()


class Folders:
    src = SourcePaths()
    dst = DestPaths()

folder = Folders()

resolution=0.5
nodata=-9999

#Rasterize polder polygon
if not folder.dst.tmp.polder.exists():
    gdf_polder = gpd.read_file(folder.src.polder.path)
    metadata = hrt.create_meta_from_gdf(gdf_polder, res=resolution)
    gdf_polder["value"] = 1
    hrt.gdf_to_raster(
        gdf=gdf_polder,
        value_field="value",
        raster_out=folder.dst.tmp.polder,
        nodata=nodata,
        metadata=metadata,
        read_array=False
    )
#Rasterize watergangen
if not folder.dst.tmp.watervlakken.exists():
    gdf_polder = gpd.read_file(folder.src.polder.path)

    gdf = gpd.read_file(folder.src.watervlakken.path)
    metadata = hrt.create_meta_from_gdf(gdf_polder, res=resolution)
    gdf["value"] = 10
    hrt.gdf_to_raster(
        gdf=gdf,
        value_field="value",
        raster_out=folder.dst.tmp.watervlakken,
        nodata=nodata,
        metadata=metadata,
        read_array=False,
        overwrite=False
    )


if not folder.dst.tmp.dem.exists():
    output_file = folder.dst.tmp.dem

    output_file.build_vrt(overwrite=False, 
                  bounds=eval(folder.dst.tmp.polder.metadata.bbox), 
                  input_files=folder.src.dem, 
                  resolution=0.5,
    )


output_file = folder.dst.tmp.dem

output_file.build_vrt(overwrite=False, 
                bounds=eval(folder.dst.tmp.polder.metadata.bbox), 
                input_files=folder.src.dem, 
                resolution=0.5,
)



# %%



blocks_df = folder.dst.tmp.polder.generate_blocks()

hist = {}

@dataclass
class Blocks:
    window: list
    dem_path = folder.dst.tmp.dem
    polder_path = folder.dst.tmp.polder
    watervlakken_path = folder.dst.tmp.watervlakken

    def __post_init__(self):
        self.cont = False
        self.polder = self.polder_path._read_array(window=self.window)

        self.masks = {}

        if not np.all(self.polder==self.polder_path.nodata):
            self.cont = True #Continue the calculation          
            self.dem = self.dem_path._read_array(window=self.window)
            self.watervlakken = self.watervlakken_path._read_array(window=self.window)

            self.masks["dem"] = self.dem==self.dem_path.nodata
            self.masks["polder"] = self.polder==self.polder_path.nodata

    @property
    def all_masks(self):
        """Combine masks"""
        return np.any([self.masks[i] for i in self.masks],0)


# %%
# def t():
if True:
    folder.dst.tmp.polder.min_block_size = 2048
    blocks_df = folder.dst.tmp.polder.generate_blocks()

    for idx, block_row in blocks_df.iterrows():
        block = Blocks(window=block_row['window_readarray'])

        if block.cont:
            mask_nodata = {}
            mask_nodata["dem"] = block



# %%
%timeit -n1 -r1 t()
# %%
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