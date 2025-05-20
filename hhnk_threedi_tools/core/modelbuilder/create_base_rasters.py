# %%
r"""
Bijwerken van E:\01.basisgegevens\rasters\

Waterdelen staan in de BGT -> BGT.HHNK_MV_WTD
Ondersteunede waterdelen staan in de BGT -> BGT.HHNK_MV_OWT
"D:\oracle\product\19.0.0\client_64\network\admin\tnsnames.ora"
"""

# %% Watervlakken rasterizen
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt
from osgeo import gdal


def create_base_rasters(waterdeel_path: Union[str, Path], date: int):
    """_summary_

    Parameters
    ----------
    watervlak_path : Union[str, Path]
        Dir where waterdelen exports are stored.
    date : int
        Export date in filename, e.g. 20240824
    """
    folder = hrt.Folder(waterdeel_path)
    folder.add_file("wtd_gpkg", f"bgt_waterdelen_{date}.gpkg")
    folder.add_file("ond_wtd_gpkg", f"bgt_ondersteunende_waterdelen_{date}.gpkg")
    folder.add_file("wtd_tif", f"bgt_waterdelen_{date}.tif")
    folder.add_file("ond_wtd_tif", f"bgt_ondersteunende_waterdelen_{date}.tif")

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


if __name__ == "__main__":
    waterdeel_path = r"E:\01.basisgegevens\rasters\watervlakken"
    date = 20240824

    create_base_rasters(waterdeel_path=waterdeel_path, date=date)

# %%

# wd_gdf = folder.wtd_gpkg.load()
# owd_gdf = folder.ond_wtd_gpkg.load()

# import pandas as pd

# df = pd.concat([wd_gdf, owd_gdf])
