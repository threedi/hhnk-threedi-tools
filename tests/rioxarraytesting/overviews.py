# %%

import shutil
from pathlib import Path

import rasterio as rio
import rioxarray as rxr
from rasterio.enums import Resampling

PLAYGROUND_DIR = Path(r"C:\Users\Wietse\Documents\HHNK\playground")


dem = PLAYGROUND_DIR / "dem_schemer_hoog_zuid_compressed_v3.tif"
dem2 = PLAYGROUND_DIR / "dem_schemer_hoog_zuid_compressed_v4.tif"

if not dem2.exists():
    shutil.copy(dem, dem2)
factors = []


resampling = "nearest"

dst = rio.open(dem2, "r+")

# dst.build_overviews(factors, getattr(rio.enums.Resampling, resampling))
# dst.update_tags(ns="rio_overview", resampling=resampling)

# dst.close()

import hhnk_research_tools as hrt

r = hrt.Raster(dem2)
# %%


ds = r.open_gdal_source_write()
ds.BuildOverviews("AVERAGE", overviewlist=[8, 16, 32, 128])
ds = None

# %%


ds = r.open_gdal_source_write()
ds.BuildOverviews("NONE")
ds = None
# %%
src = rio.open(dem2, "r")

[src.overviews(i) for i in src.indexes]
# src.read().shape
# src.tags(ns='rio_overview').get('resampling')

# src.read(out_shape=(3, int(src.height / 16), int(src.width / 16))).shape

# %%
# rxr.open_rasterio(dem2, masked=True, overview_level=0)  # Open first overview level

!gdaladdo -ro -r nearest --config COMPRESS_OVERVIEW ZSTD --config PREDICTOR_OVERVIEW 2 --config ZSTD_LEVEL_OVERVIEW 1 "C:\Users\Wietse\Documents\HHNK\playground\dem_schemer_hoog_zuid_compressed_v4.tif" 8 32


# %%

# %%
import rasterio as rio

src = rio.open(dem2, "r")

[src.overviews(i) for i in src.indexes]