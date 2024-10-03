# %%
#  gebaseerd op; r'G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens

from typing import Union

import hhnk_research_tools as hrt
import numpy as np
import xarray as xr

import hhnk_threedi_tools.core.raster_creation.storage_lookup as storage_lookup
from tests.config import FOLDER_TEST, TEMP_DIR

building_dh = 0.1  # m. Soil starts 0.1m under building footprint. #TODO Wordt dit wel gebruikt? En zou dat moeten?


class StorageRaster:
    def __init__(
        self,
        raster_out: hrt.Raster,
        raster_paths_dict: dict[str : hrt.Raster],
        metadata_key: str,
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
        self.raster_out = raster_out
        self.raster_paths_dict = raster_paths_dict
        self.metadata_key = metadata_key
        self.verbose = verbose

        # Local vars
        self.storage_lookup_df = None  # Declared at .run
        self.soil_lookup_df = None  # Declared at .run
        # If bounds of input rasters are not the same a temp vrt is created
        # The path to these files are stored here.
        self.raster_paths_same_bounds = self.raster_paths_dict.copy()
        self.tempdir = tempdir
        if tempdir is None:
            self.tempdir = raster_out.parent.full_path(f"temp_{hrt.current_time(date=True)}")

    @property
    def metadata_raster(self) -> hrt.Raster:
        """Raster of which metadata is used to create output."""
        return self.raster_paths_dict[self.metadata_key]

    def verify(self, overwrite: bool = False) -> bool:
        """Verify if all inputs can be accessed and if they have the same bounds."""
        cont = True

        # Check if all input rasters have the same bounds
        bounds = {}
        error = None
        for key, r in self.raster_paths_dict.items():
            if cont:
                if not isinstance(r, hrt.Raster):
                    raise TypeError(f"{key}:{r} in raster_paths_dict is not of type hrt.Raster")
                if not r.exists():
                    print(f"Missing input raster key: {key} @ {r}")
                    cont = False
                    error = f"{key}: {r} does not exist"
                    continue
                bounds[key] = r.metadata.bounds
        if error:
            raise FileNotFoundError(error)

        # Check resolution
        if cont:
            vrt_keys = []
            for key, r in self.raster_paths_dict.items():
                if r.metadata.pixelarea > self.metadata_raster.metadata.pixelarea:
                    print(f"Resolution of {key} is not the same as metadataraster {self.metadata_key}, creating vrt")
                    self.create_vrt(key)
                    vrt_keys.append(key)
                if r.metadata.pixelarea < self.metadata_raster.metadata.pixelarea:
                    cont = False
                    raise NotImplementedError(
                        f"Resolution of {key} is smaller than metadataraster {self.metadata_key}, \
this is not implemented or tested if it works."
                    )

        # Check bounds, if they are not the same as the metadata_raster, create a vrt
        if cont:
            for key, r in self.raster_paths_dict.items():
                if r.metadata.bounds != self.metadata_raster.metadata.bounds:
                    # Create vrt if it was not already created in resolution check
                    if key not in vrt_keys:
                        self.create_vrt(key)

                    if self.verbose:
                        print(f"{key} does not have same extent as {self.metadata_key}, creating vrt")

        # Check if we should create new file
        if cont:
            if self.raster_out is not None:
                cont = hrt.check_create_new_file(output_file=self.raster_out, overwrite=overwrite)
                if cont is False:
                    if self.verbose:
                        print(f"Output raster already exists: {self.raster_out.name} @ {self.raster_out.path}")

        return cont

    def create_vrt(self, raster_key: str):
        """Create vrt of input rasters with the extent of the metadata raster

        Parameters
        ----------
        raster_key (str) : key in self.raster_paths_dict to create vrt from.
        """
        input_raster = self.raster_paths_dict[raster_key]

        # Create temp output folder.
        self.tempdir.mkdir()
        output_raster = self.tempdir.full_path(f"{input_raster.stem}.vrt")
        print(f"Creating temporary vrt; {output_raster.name} @ {output_raster}")

        output_raster = hrt.Raster.build_vrt(
            vrt_out=output_raster,
            input_files=input_raster,
            overwrite=True,
            bounds=self.metadata_raster.metadata.bbox_gdal,
            resolution=self.metadata_raster.metadata.pixel_width,
        )

        self.raster_paths_same_bounds[raster_key] = output_raster

    def run(self, rootzone_thickness_cm: int, chunksize: Union[int, None] = None, overwrite: bool = False):
        # level block_calculator
        def calc_storage(da_storage, da_dewa, da_soil, mask_nodata, mask_zero, nodata):
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

            # Apply nodata and zero values
            da_storage = xr.where(mask_zero, 0, da_storage)
            da_storage = xr.where(mask_nodata, nodata, da_storage)

            return da_storage

        cont = self.verify(overwrite=overwrite)

        if cont:
            # Create/load lookup table to find available storage using dewa depth
            self.storage_lookup_df, self.soil_lookup_df = storage_lookup.create_storage_lookup(
                rootzone_thickness_cm=rootzone_thickness_cm, storage_unsa_sim_path=None
            )

            # Load dataarrays
            da_soil = self.raster_paths_same_bounds["soil"].open_rxr(chunksize)
            da_gwlvl = self.raster_paths_same_bounds["gwlvl"].open_rxr(chunksize)
            da_dem = self.raster_paths_same_bounds["dem"].open_rxr(chunksize)

            nodata = da_dem.rio.nodata

            da_dewa = da_dem - da_gwlvl

            # Create global no data masker
            masks = {}
            masks["dem"] = da_dem == da_dem.rio.nodata
            masks["soil"] = da_soil == da_soil.rio.nodata
            masks["gwlvl"] = da_gwlvl == da_gwlvl.rio.nodata
            mask_nodata = np.any([masks[i] for i in masks], 0)  # FIXME gaat dit goed met rxr?

            # mask where out storage should be zero
            zeromasks = {}
            zeromasks["dem_water"] = da_dem == 10  # TODO watervlakken?
            zeromasks["negative_dewa"] = da_dewa < 0
            mask_zero = np.any([zeromasks[i] for i in zeromasks], 0)

            da_storage = xr.full_like(da_dem, da_dem.rio.nodata)

            # create empty result array
            result = xr.map_blocks(
                calc_storage,
                obj=da_storage,
                args=[da_dewa, da_soil, mask_nodata, mask_zero, nodata],
                template=da_storage,
            )

            self.raster_out = hrt.Raster.write(self.raster_out, result=result, nodata=nodata, chunksize=chunksize)
            return self.raster_out


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
    metadata_key = "soil"
    verbose = False
    tempdir = None

    self = StorageRaster(
        raster_out=raster_out,
        raster_paths_dict=raster_paths_dict,
        metadata_key=metadata_key,
        verbose=verbose,
        tempdir=tempdir,
    )

    self.run(rootzone_thickness_cm=rootzone_thickness_cm)
