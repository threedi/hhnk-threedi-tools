# %%
#  gebaseerd op; r'G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens

from typing import Union

import hhnk_research_tools as hrt
import numpy as np
import xarray as xr

import hhnk_threedi_tools.core.raster_creation.storage_lookup as storage_lookup
from tests.config import FOLDER_TEST, TEMP_DIR

building_dh = 0.1  # m. Soil starts 0.1m under building footprint. #TODO Wordt dit wel gebruikt? En zou dat moeten?


class StorageRaster(hrt.RasterCalculatorRxr):
    def __init__(
        self,
        raster_out: hrt.Raster,
        raster_paths_dict: dict[str : hrt.Raster],
        metadata_key: str,
        nodata_keys: list[str],
        verbose: bool = False,
        tempdir: hrt.Folder = None,
    ):
        """Bereken beschikbaar bodembergingsraster

        Parameters
        ----------
        rasters : dict
            {
                "gwlvl":,
                "dem":,
                "soil":,
            }

            rasterpad naar gwlvl raster. Dit is het meetadataraster, op basis van deze
            extent maakt ie van de dem en soil een vrt. En output ook in deze extent.
        """

        super().__init__(
            raster_out=raster_out,
            raster_paths_dict=raster_paths_dict,
            nodata_keys=nodata_keys,
            metadata_key=metadata_key,
            verbose=verbose,
            tempdir=tempdir,
        )

        # Local vars
        self.storage_lookup_df = None  # Declared at .run
        self.soil_lookup_df = None  # Declared at .run

    def run(self, rootzone_thickness_cm: int, chunksize: Union[int, None] = None, overwrite: bool = False):
        # level block_calculator
        def calc_storage(da_storage, da_dewa, da_soil):
            """
            Calculate storage for a chunk.
            da_storage
            da_dewa (xr.DataArray): gwlvl - dem
            da_soil (xr.DataArray): soil
            mask_nodata (xr.DataArray): mask where output will be nodata
            mask_zero (xr.DataArray): mask where output will be zero
            nodata (int, float): output nodata
            """
            # Iterate over all soil types
            soil_lookup_df = self.soil_lookup_df
            for soil_type in np.unique(da_soil):
                if soil_type not in self.soil_lookup_df.index:
                    print(f"Soil type {soil_type} not found in soil_lookup_df")
                    continue

                soil_mask = xr.where(da_soil == soil_type, True, False)

                # Create list of dewateringdepths, corresponding total storage from capsim table.
                # ontwateringsdiepte
                xlist = soil_lookup_df.loc[soil_type, "Dewathering Depth (m)"]
                ylist = soil_lookup_df.loc[soil_type, "Total Available Storage (m)"]

                # Determine the storage coefficient per pixel using the actual dewatering depth (dewadepth_arr[soil_mask])
                # and the corresponding storage coefficient (ylist). Find values by interpolation.
                da_storage = xr.where(soil_mask, np.interp(x=da_dewa.where(soil_mask), xp=xlist, fp=ylist), da_storage)

            return da_storage

        cont = self.verify(overwrite=overwrite)

        if cont:
            print("lets go")
            # Create/load lookup table to find available storage using dewa depth
            self.storage_lookup_df, self.soil_lookup_df = storage_lookup.create_storage_lookup(
                rootzone_thickness_cm=rootzone_thickness_cm, storage_unsa_sim_path=None
            )

            # Load dataarrays
            da_dict = {}
            da_soil = da_dict["soil"] = self.raster_paths_same_bounds["soil"].open_rxr(chunksize)
            da_gwlvl = da_dict["gwlvl"] = self.raster_paths_same_bounds["gwlvl"].open_rxr(chunksize)
            da_dem = da_dict["dem"] = self.raster_paths_same_bounds["dem"].open_rxr(chunksize)

            nodata = da_dem.rio.nodata

            da_dewa = da_dem - da_gwlvl

            # Create global no data mask
            da_nodatamasks = self.get_nodatamasks(da_dict=da_dict)

            # Mask where out storage should be zero
            zeromasks = {}
            zeromasks["dem_water"] = da_dem == 10  # TODO watervlakken?
            zeromasks["negative_dewa"] = da_dewa < 0
            # Stack the conditions into a single DataArray
            da_zeromasks = self.concat_masks(zeromasks)
            da_storage = xr.full_like(da_dem, da_dem.rio.nodata)

            # create empty result array
            da_storage = xr.map_blocks(
                calc_storage,
                obj=da_storage,
                args=[da_dewa, da_soil],
                template=da_storage,
            )

            # Apply nodata and zero values
            da_storage = xr.where(da_zeromasks, 0, da_storage)
            da_storage = xr.where(da_nodatamasks, nodata, da_storage)

            self.raster_out = hrt.Raster.write(self.raster_out, result=da_storage, nodata=nodata, chunksize=chunksize)
            return self.raster_out
        else:
            print("Cont is False")


# %%
if __name__ == "__main__":
    folder = hrt.Folder(TEMP_DIR / f"storage_{hrt.get_uuid()}", create=True)
    folder.add_file("storage", "storage.tif")

    rootzone_thickness_cm = 20  # cm

    folder_schema = FOLDER_TEST

    overwrite = True
    nodata = -9999
    chunksize = None

    raster_out = folder.storage
    raster_paths_dict = {
        "gwlvl": folder_schema.model.schema_base.rasters.gwlvl_glg,
        "dem": folder_schema.model.schema_base.rasters.dem,
        "soil": folder_schema.model.schema_base.rasters.soil,
    }

    verbose = False
    tempdir = None
    metadata_key = "soil"
    nodata_keys = ["gwlvl", "dem", "soil"]

    self = StorageRaster(
        raster_out=raster_out,
        raster_paths_dict=raster_paths_dict,
        metadata_key=metadata_key,
        nodata_keys=nodata_keys,
        verbose=verbose,
        tempdir=tempdir,
    )

    o = self.run(rootzone_thickness_cm=rootzone_thickness_cm)
