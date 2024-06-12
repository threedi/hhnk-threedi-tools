"""
Create rasters required to do postprocessing.
"""

from typing import Union

import hhnk_research_tools as hrt

from hhnk_threedi_tools import Folders


def create_dem_50cm(folder: Folders, dem: Union[hrt.Raster, str]):
    """Init GridToRaster from a model folders. It will use the model-dem and source-data panden to create
    a dem elevated +10cm at panden.

    Parameters
    ----------
    folder : Folders
        Model folders structure, including 'source_data' and 'model' sub-folders. To run this method
        the following files should exist:
         - folder.model.schema_base.rasters.dem
         - folder.model.manipulated_rasters.panden

    dem: str
        path to the original dem.
        default dem = folder.model.schema_base.rasters.dem


    Returns
    -------
    GridToRaster
    """
    # check if damage dem needs to be updated
    highres_dem = folder.model.calculation_rasters.dem
    damage_dem = folder.model.calculation_rasters.damage_dem
    panden_gpkg = folder.source_data.panden
    panden_raster = folder.model.calculation_rasters.panden

    # check if we need to overwrite damage_dem because it doesn't exist or panden or model-dem are newer
    if not folder.model.calculation_rasters.exists():  # if the folders doesn't exist we are to create it
        folder.model.calculation_rasters.create()
        folder.model.calculation_rasters.create_readme()
        overwrite = True
    else:
        overwrite = hrt.check_create_new_file(
            output_file=damage_dem,
            input_files=[dem.base, panden_gpkg.base],
            overwrite=False,
        )

    # remove all files that do not have the correct pixel-size
    for file in [highres_dem, panden_raster, damage_dem]:
        if file.exists():
            if overwrite or (file.metadata.pixel_width != 0.5):
                file.path.unlink()

    # create panden_raster if it doesn't exist or is removed
    if not highres_dem.exists():
        hrt.reproject(src=dem, target_res=0.5, output_path=highres_dem.path)

    # create panden_raster if it doesn't exist or is removed
    if not panden_raster.exists():
        panden_gdf = panden_gpkg.load()
        panden_gdf["base_height"] = 0.1
        hrt.gdf_to_raster(
            gdf=panden_gdf,
            value_field="base_height",
            raster_out=panden_raster,
            nodata=0,
            metadata=highres_dem.metadata,
        )

    # create damage_dem if it doen't exist or is removed
    if not damage_dem.exists():

        def elevate_dem_block(block):
            block_out = block.blocks["dem"] + block.blocks["panden"]

            block_out[block.masks_all] = highres_dem.nodata
            return block_out

        calc = hrt.RasterCalculatorV2(
            raster_out=damage_dem,
            raster_paths_dict={
                "dem": highres_dem,
                "panden": panden_raster,
            },
            nodata_keys=["dem"],
            mask_keys=["dem"],
            metadata_key="dem",
            custom_run_window_function=elevate_dem_block,
            output_nodata=highres_dem.nodata,
            min_block_size=4096,
            verbose=True,
        )

        calc.run(overwrite=False)
