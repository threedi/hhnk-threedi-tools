# %%
r"""
Bijwerken van E:\01.basisgegevens\rasters\

Waterdelen staan in de BGT -> BGT.HHNK_MV_WTD
Ondersteunede waterdelen staan in de BGT -> BGT.HHNK_MV_OWT
"D:\oracle\product\19.0.0\client_64\network\admin\tnsnames.ora"
"""

# %% Watervlakken rasterizen
import hhnk_research_tools as hrt
from osgeo import gdal

folder = hrt.Folder(r"E:\01.basisgegevens\rasters\watervlakken")
DATE = 20240824

folder.add_file("wtd_gpkg", f"bgt_waterdelen_{DATE}.gpkg")
folder.add_file("ond_wtd_gpkg", f"bgt_ondersteunende_waterdelen_{DATE}.gpkg")
folder.add_file("wtd_tif", f"bgt_waterdelen_{DATE}.tif")
folder.add_file("ond_wtd_tif", f"bgt_ondersteunende_waterdelen_{DATE}.tif")

# Waterdeel
if not folder.wtd_tif.exists():
    watervlak_gdf = folder.wtd_gpkg.load()
    watervlak_gdf["value"] = 1

    metadata = hrt.RasterMetadataV2.from_gdf(watervlak_gdf, res=0.5)

    hrt.gdf_to_raster(
        watervlak_gdf,
        value_field="value",
        raster_out=folder.wtd_tif,
        metadata=metadata,
        nodata=0,
        datatype=gdal.GDT_Byte,
        read_array=False,
    )

# Ondersteunend waterdeel
if not folder.ond_wtd_tif.exists():
    watervlak_gdf = folder.ond_wtd_gpkg.load()
    watervlak_gdf["value"] = 1

    metadata = hrt.RasterMetadataV2.from_gdf(watervlak_gdf, res=0.5)

    hrt.gdf_to_raster(
        watervlak_gdf,
        value_field="value",
        raster_out=folder.ond_wtd_tif,
        metadata=metadata,
        nodata=0,
        datatype=gdal.GDT_Byte,
        read_array=False,
    )

# %%

wd_gdf = folder.wtd_gpkg.load()
owd_gdf = folder.ond_wtd_gpkg.load()

import pandas as pd

df = pd.concat([wd_gdf, owd_gdf])
