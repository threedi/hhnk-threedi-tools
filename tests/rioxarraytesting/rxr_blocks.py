# %%
import shutil
import time

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import rioxarray as rxr
import xarray as xr
from shapely import geometry

from tests.config import FOLDER_TEST, TEMP_DIR

CHUNKSIZE = 64


dem = FOLDER_TEST.model.schema_base.rasters.dem
# dem = hrt.Raster(
#     rf"C:\Users\wiets\Documents\HHNK\07.Poldermodellen\LangeWeerenToekomstHHNK_1d2d_ghg\work in progress\schematisation\rasters\dem_ontsluitingsroute_ahn4_lw_v1.tif"
# )

wlvl = hrt.Raster(TEMP_DIR.joinpath(f"wlvl_{hrt.current_time(date=True)}.tif"))
depth = hrt.Raster(TEMP_DIR.joinpath(f"raster_out_{hrt.current_time(date=True)}.tif"))

if not wlvl.exists():
    shutil.copy(dem.base, wlvl.base)


class R(hrt.Raster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def generate_blocks(self, blocksize_from_source: bool = False) -> pd.DataFrame:
        """Generate blocks with the blocksize of the band.
        These blocks can be used as window to load the raster iteratively.
        blocksize_from_source (bool): read the blocksize from the source raster
            if its bigger than min_blocksize, use that.
        """

        if blocksize_from_source:
            gdal_src = self.open_gdal_source_read()
            band = gdal_src.GetRasterBand(1)
            block_height, block_width = band.GetBlockSize()
            band.FlushCache()  # close file after writing
            band = None
        else:
            block_height = 0
            block_width = 0

        if (block_height < self.min_block_size) or (block_width < self.min_block_size):
            block_height = self.min_block_size
            block_width = self.min_block_size

        ncols = int(np.floor(self.metadata.x_res / block_width))
        nrows = int(np.floor(self.metadata.y_res / block_height))

        # Create arrays with index of where windows end. These are square blocks.
        xparts = np.linspace(0, block_width * ncols, ncols + 1).astype(int)
        yparts = np.linspace(0, block_height * nrows, nrows + 1).astype(int)

        # If raster has some extra data that didnt fall within a block it is added to the parts here.
        # These blocks are not square.
        if block_width * ncols != self.shape[1]:
            xparts = np.append(xparts, self.shape[1])
            ncols += 1
        if block_height * nrows != self.shape[0]:
            yparts = np.append(yparts, self.shape[0])
            nrows += 1

        blocks_df = pd.DataFrame(index=np.arange(nrows * ncols) + 1, columns=["ix", "iy", "window"])
        i = 0
        for ix in range(ncols):
            for iy in range(nrows):
                i += 1
                blocks_df.loc[i, :] = np.array(
                    (ix, iy, [xparts[ix], yparts[iy], xparts[ix + 1], yparts[iy + 1]]), dtype=object
                )

        blocks_df["window_readarray"] = blocks_df["window"].apply(
            lambda x: [int(x[0]), int(x[1]), int(x[2] - x[0]), int(x[3] - x[1])]
        )

        self.blocks = blocks_df
        return blocks_df

    def _generate_blocks_geometry_row(self, window):
        window = window["window_readarray"]
        minx = self.metadata.x_min
        maxy = self.metadata.y_max

        # account for pixel size
        minx += window[0] * self.metadata.pixel_width
        maxy += window[1] * self.metadata.pixel_height
        maxx = minx + window[2] * self.metadata.pixel_width
        miny = maxy + window[3] * self.metadata.pixel_height

        # rxr gebruikt bij xy het midden van de cel.
        rxr_minx = minx + self.metadata.pixel_width * 0.5
        rxr_maxy = maxy + self.metadata.pixel_height * 0.5
        return rxr_minx, rxr_maxy, geometry.box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    def generate_blocks_geometry(self) -> gpd.GeoDataFrame:
        """Create blocks with shapely geometry"""
        self.blocks = self.generate_blocks()

        # return self.blocks.apply(
        #     self._generate_blocks_geometry_row, axis=1,
        # )

        self.blocks[["minx", "maxy", "geometry"]] = self.blocks.apply(
            self._generate_blocks_geometry_row,
            axis=1,
            result_type="expand",
        )
        self.blocks = gpd.GeoDataFrame(
            self.blocks,
            geometry="geometry",
            crs=self.metadata.projection,
        )
        return self.blocks


# %%
CHUNKSIZE = 64

if True:
    # demr = R(
    #     rf"C:\Users\wiets\Documents\HHNK\07.Poldermodellen\LangeWeerenToekomstHHNK_1d2d_ghg\work in progress\schematisation\rasters\dem_ontsluitingsroute_ahn4_lw_v1.tif"
    # )
    dem = R(dem)

    dem.min_block_size = CHUNKSIZE

    bounds = [129613.0, 134520.0, 497637.0, 503208.5]
    res = 0.5

    x_res = dem.metadata.x_res
    y_res = dem.metadata.y_res

    df = dem.generate_blocks_geometry()
df

# %%
df.drop(["window", "window_readarray"], axis=1, inplace=True)
df["geometry"] = df.apply(lambda x: geometry.Point(x["minx"], x["maxy"]), axis=1)
df.to_file("hrtpoints2.gpkg")
# %%
demr = rxr.open_rasterio(dem.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})


# %%


df["xy"] = df.apply(lambda x: f"{x['minx']},{x['maxy']}", axis=1)
df.loc[[1, 2, 3], "use"] = True  # set 3 block to 'true', only do calculation here.
df.set_index("xy", inplace=True)

# %%


def get_rasters(chunksize=CHUNKSIZE):
    # Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
    raster1 = rxr.open_rasterio(dem.base, chunks={"x": chunksize, "y": chunksize})
    raster2 = rxr.open_rasterio(wlvl.base, chunks={"x": chunksize, "y": chunksize})

    # Set NoData values (if needed, adjust to match your actual NoData value)
    # raster1 = raster1.where(raster1 != raster1.rio.nodata, np.nan)
    # raster2 = raster2.where(raster2 != raster2.rio.nodata, np.nan)
    return raster1, raster2


# TODO

"""
Testen of en op welke manier de xr.full_like return ipv berekening doen de
berekening sneller maakt. Vooral voor groetere rasters.


"""


def calc_zeros(d_out, da1, da2):
    return d_out


def calc_full_like(d_out, da1, da2):
    return xr.full_like(da1, da1.rio.nodata)


def calc_selected_blocks(d_out, da1, da2):
    if df.at[f"{da1.x.data[0]},{da1.y.data[0]}", "use"] is True:
        return da1 - da2
    else:
        return d_out
    # return xr.zeros_like(da1) - 9999

    # print(da)
    print(f"geometry.Point{(da1.x.data[0], da1.y.data[0])},")


def calc_long(d_out, da1, da2):
    return da1**da2**da1**da2**da2**da2**da1**10e6


def calc_all(d_out, da1, da2):
    r = da1 - da2
    r *= 1000  # scale_factor so we can save ints
    r = r.where(r >= 0, -9999)

    return r


def rxr_map_blocks(calc_func):
    now = time.time()

    raster1, raster2 = get_rasters(chunksize=CHUNKSIZE)

    result = xr.full_like(other=raster1, fill_value=raster1.rio.nodata)

    # result += 2
    result = xr.map_blocks(calc_func, obj=result, args=[raster1, raster2], template=result)

    result.rio.set_nodata(raster1.rio.nodata)

    result.rio.to_raster(
        depth.base,
        chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
        # lock=True, #Use dask multithread
        compress="ZSTD",
        tiled=True,
        PREDICTOR=2,  # from hrt, does it still work?
        ZSTD_LEVEL=1,  # from hrt, does it still work?
        # scale=0.01,
        # dtype="int16",
    )

    # Settings the scale_factor does not work with rioxarray. T
    # gdal_source = depth.open_gdal_source_write()
    # b = gdal_source.GetRasterBand(1)
    # b.SetScale(0.001)
    # gdal_source = None

    print(time.time() - now)


# %%

# calc_zeros
rxr_map_blocks(calc_zeros)

# %%

# calc_full_like
rxr_map_blocks(calc_full_like)

# %%

# calc_selected_blocks
rxr_map_blocks(calc_selected_blocks)

# %%

# calc_long
rxr_map_blocks(calc_long)

# %%

# calc_all
rxr_map_blocks(calc_all)
# %%

df["geometry"] = [
    geometry.Point(109981.0, 542085.0),
    geometry.Point(109581.0, 542085.0),
    geometry.Point(109981.0, 542485.0),
    geometry.Point(109581.0, 542485.0),
    geometry.Point(109981.0, 542885.0),
    geometry.Point(109581.0, 542885.0),
    geometry.Point(109981.0, 543285.0),
    geometry.Point(109581.0, 543285.0),
]
df.to_file("rxrpoints.gpkg")
