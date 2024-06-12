"""
Create rasters required to do postprocessing.
"""

from typing import Union

import hhnk_research_tools as hrt

from hhnk_threedi_tools import Folders


def create_damage_dem(folder: Folders, dem: hrt.Raster):
    """Create the damage dem. This dem differs from the original model dem in that the
    ground level of buildings are increased by an extra 10cm for a total of 15cm. This will ensure that
    no damage is calculated with low inundation depths. In the dem the floor
    level is already increased by 5cm (compared to the 75th percentile on the raw dem).

    This 15cm then accounts for door thresholds to hold a bit of water before it starts
    to enter buildings. In the inundation rasters there will still show inundation, but this
    is not reflected in the damage calculation unless the depth is higher than 10cm.

    Also makes sure the resolution is 50cm. Which is required for damage calculations.

    Parameters
    ----------
    folder : Folders
        Model folders structure, including 'source_data' and 'model' sub-folders. To run this method
        the following files should exist:
         - folder.model.schema_base.rasters.dem tif
         - folder.source_data.panden gpkg

    dem: hrt.Raster
        path to the original dem.
        default dem = folder.model.schema_base.rasters.dem
    """

    damage_dem = folder.model.calculation_rasters.full_path(f"damage{dem.stem}_50cm.tif")
    panden_gpkg = folder.source_data.panden
    panden_raster = folder.model.calculation_rasters.panden

    # Check if we should start creating the output
    overwrite = hrt.check_create_new_file(
        output_file=damage_dem,
        input_files=[dem.base, panden_gpkg.base],  # If any of these are newer than output, update them.
        overwrite=False,
    )

    if overwrite:
        # check if we need to overwrite damage_dem because it doesn't exist or panden or model-dem are newer
        if not folder.model.calculation_rasters.exists():  # if the folders doesn't exist we are to create it
            folder.model.calculation_rasters.create()
            folder.model.calculation_rasters.create_readme()

        # create highres dem if it doesn't exist
        if dem.metadata.x_res == 0.5:
            highres_dem = dem
        else:
            highres_dem = folder.model.calculation_rasters.full_path(f"{dem.stem}_50cm.tif")

        if not highres_dem.exists():
            hrt.reproject(src=dem, target_res=0.5, output_path=highres_dem.path)

        # create panden_raster if it doesn't exist
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

        # Create damage dem
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

        calc.run(overwrite=True)
    else:
        print(f"{damage_dem.view_name_with_parents(2)} already exists")
