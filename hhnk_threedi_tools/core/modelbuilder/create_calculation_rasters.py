# %%
"""Rasters required to do postprocessing."""

from dataclasses import dataclass
from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt

from hhnk_threedi_tools import Folders


def create_polder_tif(folder, overwrite=False):
    output_file = folder.model.calculation_rasters.polder
    create = hrt.check_create_new_file(
        output_file=output_file,
        input_files=[folder.source_data.polder_polygon],
        overwrite=overwrite,
    )
    if create:
        gdf_polder = folder.source_data.polder_polygon.load()
        metadata = hrt.RasterMetadataV2.from_gdf(gdf=gdf_polder, res=0.5)
        gdf_polder["value"] = 1
        hrt.gdf_to_raster(
            gdf=gdf_polder,
            value_field="value",
            raster_out=folder.model.calculation_rasters.polder,
            nodata=0,
            metadata=metadata,
            read_array=False,
        )


def create_waterdeel_tif(folder, overwrite=False):
    output_file = folder.model.calculation_rasters.waterdeel
    create = hrt.check_create_new_file(
        output_file=output_file,
        # input_files=[folder.source_data.polder_polygon], #TODO check edit time of layers in gpkg?
        overwrite=overwrite,
    )
    if create:
        gdf = folder.source_data.damo.load(layer="Waterdeel")
        metadata = folder.model.calculation_rasters.polder.metadata
        gdf["value"] = 1
        hrt.gdf_to_raster(
            gdf=gdf,
            value_field="value",
            raster_out=folder.model.calculation_rasters.waterdeel,
            nodata=0,
            metadata=metadata,
            read_array=False,
        )


@dataclass
class DamageDem:
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
    dem: hrt.Raster
        original dem, provide the path.
        default dem = folder.model.schema_base.rasters.dem
    """

    dem: hrt.Raster
    panden_gpkg: Union[hrt.SpatialDatabase, hrt.File, Path, str]
    panden_raster: hrt.Raster
    damage_dem: hrt.Raster

    def __post_init__(self):
        if self.dem.metadata.x_res == 0.5:
            self.highres_dem = self.dem
        else:
            self.highres_dem = hrt.Folder(self.damage_dem.parent).full_path(f"{self.dem.stem}_50cm.tif")

    @classmethod
    def from_folder(cls, folder: Folders, dem=None, panden_gpkg=None, panden_raster=None, damage_dem=None):
        """Call from folder, if any of input is None take it from folder otherwise use the input.

        Parameters
        ----------
        folder : Folders
            Model folders structure, including 'source_data' and 'model' sub-folders. To run this method
            the following files should exist:
                - folder.model.schema_base.rasters.dem tif
                - folder.source_data.panden gpkg
        """
        if dem is None:
            dem = folder.model.schema_base.rasters.dem
        if panden_gpkg is None:
            panden_gpkg = folder.source_data.panden
        if panden_raster is None:
            panden_raster = folder.model.calculation_rasters.panden
        if damage_dem is None:
            damage_dem = folder.model.calculation_rasters.full_path(f"damage{dem.stem}_50cm.tif")

        return cls(
            dem=dem,
            panden_gpkg=panden_gpkg,
            panden_raster=panden_raster,
            damage_dem=damage_dem,
        )

    def create(self, overwrite=False):
        # Check if we should start creating the output
        overwrite = hrt.check_create_new_file(
            output_file=self.damage_dem,
            input_files=[self.dem.base, self.panden_gpkg.base],  # If any of these are newer than output, update them.
            overwrite=overwrite,
        )

        if overwrite:
            # create highres dem if it doesn't exist
            if not self.highres_dem.exists():
                hrt.Raster.reproject(src=self.dem, dst=self.highres_dem, target_res=0.5)

            # create panden_raster if it doesn't exist
            if not self.panden_raster.exists():
                panden_gdf = self.panden_gpkg.load()
                panden_gdf["base_height"] = 0.1
                hrt.gdf_to_raster(
                    gdf=panden_gdf,
                    value_field="base_height",
                    raster_out=self.panden_raster,
                    nodata=0,
                    metadata=self.highres_dem.metadata,
                    overwrite=True,
                )

            # Create damage dem
            dem = self.highres_dem.open_rxr()
            pand = self.panden_raster.open_rxr()

            result = dem + pand.fillna(0)

            hrt.Raster.write(
                raster_out=self.damage_dem,
                result=result,
                nodata=self.highres_dem.nodata,
                dtype="float32",
                scale_factor=None,
                chunksize=self.damage_dem.chunksize,
            )

        else:
            print(f"{self.damage_dem.view_name_with_parents(2)} already exists")


# %%
if __name__ == "__main__":
    from tests.config import FOLDER_TEST, TEMP_DIR

    self = dmg_dem = DamageDem.from_folder(
        folder=FOLDER_TEST,
        panden_raster=hrt.Raster(TEMP_DIR.joinpath(f"panden_{hrt.get_uuid()}.tif")),
        damage_dem=hrt.Raster(TEMP_DIR.joinpath(f"damage_dem_50cm_{hrt.get_uuid()}.tif")),
    )
    dmg_dem.create()
