# %%
#  gebaseerd op; r'G:\02_Werkplaatsen\06_HYD\Projecten\HKC22014 Vergelijking GxG en bodemvochtproducten\02. Gegevens

from pathlib import Path
from typing import Union

import geopandas as gpd
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
        self.tempdir = tempdir
        if tempdir is None:
            self.tempdir = raster_out.parent.full_path(f"temp_{hrt.current_time(date=True)}")

        # If bounds of input rasters are not the same a temp vrt is created
        # The path to these files are stored here.
        self.raster_paths_same_bounds = self.raster_paths_dict.copy()

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

        # # nodata_keys and yesdata_dict are mutually exclusive.
        # if self.yesdata_dict is not None:
        #     for key in self.yesdata_dict:
        #         if key in self.nodata_keys:
        #             raise ValueError(f"Key:'{key}' not allowed to be passed to both yesdata_dict and nodata_keys.")

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
        def calc_storage(da_storage, dewa_da, soil_da, mask_nodata, mask_zero, soil_lookup_df):
            """
            Calculate storage for a single raster block.

            storage_lookup_df (pd.DataFrame: lookup table to find available storage using dewa depth
            block_dewa (np.array): gwlvl - dem
            block_soil (np.array): soil
            nodatamask (np.array): mask where output will be nodata
            nodatavalue (float): output nodata
            zeromask (np.array): mask where output will be zero
            """
            # Iterate over all soil types
            # for soil_type in np.unique(storage_lookup_df['Soil Type']):
            unique_soil_types = np.unique(soil_lookup_df.index)
            for soil_type in np.unique(soil_da):
                if soil_type not in unique_soil_types:
                    print(f"Soil type {soil_type} not found in soil_lookup_df")
                    continue

                soil_mask = soil_da == soil_type

                # Create list of dewateringdepths, corresponding total storage from capsim table.
                # ontwateringsdiepte
                # soil_idx = storage_lookup_df["Soil Type"] == soil_type
                # xlist = np.round(
                #     storage_lookup_df.loc[soil_idx, "Dewathering Depth (m)"].tolist(), 5
                # )  # x = ontwateringsdiepte
                # ylist = np.round(
                #     storage_lookup_df.loc[soil_idx, "Total Available Storage (m)"].tolist(), 5
                # )  # y = available storage #TODO dit 1x buiten de loop doen.

                xlist = soil_lookup_df.loc[soil_type, "Dewathering Depth (m)"]
                ylist = soil_lookup_df.loc[soil_type, "Total Available Storage (m)"]

                # Determine the storage coefficient per pixel using the actual dewatering depth (dewadepth_arr[soil_mask])
                # and the corresponding storage coefficient (ylist). Find values by interpolation.
                da_storage[soil_mask] = np.interp(x=dewa_da[soil_mask], xp=xlist, fp=ylist)

            # Apply nodata and zero values
            da_storage = xr.where(mask_zero, 0, da_storage)
            da_storage = xr.where(mask_nodata, nodata, da_storage)

            return da_storage

        cont = self.verify(overwrite=overwrite)

        if cont:
            # Create/load Storage lookup df
            self.storage_lookup_df, self.soil_lookup_df = storage_lookup.create_storage_lookup(
                rootzone_thickness_cm=rootzone_thickness_cm, storage_unsa_sim_path=None
            )

            # Load dataarrays
            da_soil = self.raster_paths_same_bounds["soil"].open_rxr(chunksize)
            da_gwlvl = self.raster_paths_same_bounds["gwlvl"].open_rxr(chunksize)
            da_dem = self.raster_paths_same_bounds["dem"].open_rxr(chunksize)

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
                args=[da_dewa, da_soil, mask_nodata, mask_zero, self.soil_lookup_df],
                template=da_storage,
            )

            self.raster_out = hrt.Raster.write(
                self.raster_out, result=result, nodata=da_dem.rio.nodata, chunksize=chunksize
            )


# %%
if __name__ == "__main__":
    folder = hrt.Folder(TEMP_DIR / f"storage_{hrt.get_uuid()}", create=True)
    folder.add_file("storage", "storage.tif")

    rootzone_thickness_cm = 20  # cm

    folder_schema = FOLDER_TEST

    output_raster = folder.storage
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


# %%
def calculate_storage_raster(
    output_raster,
    meta_raster,  # FIXME moet anders,
    groundwlvl_raster: hrt.Raster,
    dem_raster: hrt.Raster,
    soil_raster: hrt.Raster,
    storage_lookup_df,
    nodata=-9999,
    overwrite=False,
):
    """Berekening van beschikbare bodembergingsraster.

    Parameters
    ----------
    soil_raster : hrt.Raster

    """
    # Controle of we door moeten gaan met berekening.
    cont = hrt.check_create_new_file(output_file=output_raster, overwrite=overwrite)

    if cont:
        print(f"Creating {output_raster.name}")

        # Create output raster
        output_raster.create(metadata=meta_raster.metadata, nodata=nodata, verbose=False, overwrite=overwrite)

        # Rasters kunnen verschillende extent hebben, om dezelfde block in te laden
        # de verschillen berekenen.
        dx_min = {}
        dy_min = {}
        for rtype, rpath in zip(["soil", "dem", "gwlvl"], [soil_raster, dem_raster, groundwlvl_raster]):
            dx_min[rtype], dy_min[rtype], _, _ = hrt.dx_dy_between_rasters(
                meta_big=rpath.metadata, meta_small=meta_raster.metadata
            )

        # Load output raster so we can edit it.
        target_ds = output_raster.open_gdal_source_write()
        out_band = target_ds.GetRasterBand(1)

        blocks_df = meta_raster.generate_blocks()

        len_total = len(blocks_df)
        for idx, block_row in blocks_df.iterrows():
            # Create windows
            window = block_row["window_readarray"]

            windows = {}
            for rtype in ["soil", "dem", "gwlvl"]:
                windows[rtype] = window.copy()
                windows[rtype][0] += dx_min[rtype]
                windows[rtype][1] += dy_min[rtype]

            # Load blocks
            block_soil = soil_raster._read_array(window=windows["soil"])
            block_dem = dem_raster._read_array(window=windows["dem"])
            block_gwlvl = groundwlvl_raster._read_array(window=windows["gwlvl"])

            # Calc dewatering depth
            block_dewa = block_dem - block_gwlvl

            # create global no data masker
            masks = {}
            masks["dem"] = block_dem == dem_raster.nodata
            masks["soil"] = block_soil == soil_raster.nodata
            masks["gwlvl"] = block_gwlvl == groundwlvl_raster.nodata
            mask = np.any([masks[i] for i in masks], 0)

            # mask where out storage should be zero
            zeromasks = {}
            zeromasks["dem_water"] = block_dem == 10
            zeromasks["negative_dewa"] = block_dewa < 0
            zeromask = np.any([zeromasks[i] for i in zeromasks], 0)

            # Calculate storage
            block_storage = compute_storage_block(
                storage_lookup_df=storage_lookup_df,
                block_dewa=block_dewa,
                block_soil=block_soil,
                nodatamask=mask,
                nodatavalue=nodata,
                zeromask=zeromask,
            )

            # Write to file
            out_band.WriteArray(block_storage, xoff=window[0], yoff=window[1])

            print(f"{idx} / {len_total}", end="\r")

        out_band.FlushCache()  # close file after writing
        out_band = None
        target_ds = None

    def run(self, output_file, chunksize: Union[int, None] = None, overwrite: bool = False):
        # level block_calculator
        def calc_storage(_, dem_da, level_da):
            depth_da = level_da - dem_da

            depth_da = xr.where(depth_da < 0, 0, depth_da)

            return depth_da

        # get dem as xarray
        dem = self.dem_raster.open_rxr(chunksize)
        level = self.wlvl_raster.open_rxr(chunksize)

        # init result raster
        self.depth_raster = hrt.Raster(output_file, chunksize=chunksize)
        create = hrt.check_create_new_file(output_file=self.depth_raster.path, overwrite=overwrite)

        if create:
            # create empty result array
            result = xr.full_like(dem, 0)

            result = xr.map_blocks(calc_depth, obj=result, args=[dem, level], template=result)

            self.depth_raster.write(output_file, result=result, nodata=0, chunksize=chunksize)

        return self.depth_raster


# %%
if __name__ == "__main__":
    folder = hrt.Folder(TEMP_DIR / f"storage_{hrt.get_uuid()}", create=True)
    folder.add_file("storage", "storage.tif")

    rootzone_thickness_cm = 20  # cm

    # Create/load Storage lookup df
    storage_lookup_df = storage_lookup.create_storage_lookup(
        rootzone_thickness_cm=rootzone_thickness_cm, storage_unsa_sim_path=None
    )

    folder_schema = FOLDER_TEST

    output_raster = folder.storage
    overwrite = True
    nodata = -9999

    groundwlvl_raster = folder_schema.model.schema_base.rasters.gwlvl_glg
    dem_raster = folder_schema.model.schema_base.rasters.dem
    soil_raster = folder_schema.model.schema_base.rasters.soil
    meta_raster = dem_raster

    # %%

    calculate_storage_raster(
        output_raster=output_raster,
        meta_raster=meta_raster,
        groundwlvl_raster=groundwlvl_raster,
        dem_raster=dem_raster,
        soil_raster=soil_raster,
        storage_lookup_df=storage_lookup_df,
        nodata=nodata,
        overwrite=overwrite,
    )

    # %%

    assert output_raster.statistics() == {
        "min": 0.0,
        "max": 0.14029,
        "mean": 0.024702,
        "std": 0.031965,
    }
