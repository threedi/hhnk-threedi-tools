# %%
import tempfile
from typing import Union

import hhnk_threedi_tools as htt
import rioxarray as rxr

import hhnk_research_tools as hrt

CHUNKSIZE = 4096


def create_landuse_polder_clip(
    dem: hrt.Raster,
    landuse_name: str,
    output_dir: Union[hrt.Folder, str] = None,
    landuse_hhnk: Union[
        hrt.Raster, str
    ] = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
    overwrite: bool = False,
) -> tuple[hrt.Raster, bool]:
    """Clip landuse raster on the model extent using the dem.
    Also burn dem nodata into landuse.

    Parameters
    ----------
    dem : hrt.Raster
        default: folder.model.schema_base.rasters.dem
    landuse_name : str
        f"landuse_{landuse_name}.tif" -> name to use in the output. 'landuse_' will be prepended.
    output_dir : hrt.Folder, str
        Default to the dem dir. Otherwise provide something.
    landuse_hhnk : hrt.Raster, str
        The 'big raster' that should cover the whole dem area.
        default at hhnk provided.
    overwrite : bool

    Returns
    -------
    hrt.Raster -> the output raster.
    bool -> created or not.
    """
    if output_dir is None:
        output_dir = dem.parent

    landuse_tif = hrt.Folder(output_dir).full_path(f"landuse_{landuse_name}.tif")
    landuse_hhnk = hrt.Raster(landuse_hhnk)
    create = hrt.check_create_new_file(output_file=landuse_tif, overwrite=overwrite)

    if create:
        # Create tempdir with landuse vrt then rasterize it.
        with tempfile.TemporaryDirectory() as tmpdirname:
            # print("created temporary directory", tmpdirname)

            landuse_vrt = hrt.Folder(tmpdirname).full_path(f"landuse_{landuse_name}.vrt")

            # Build landuse vrt
            landuse_vrt.build_vrt(overwrite=False, bounds=dem.metadata.bbox_gdal, input_files=[landuse_hhnk])

            # lazy load rasters
            dem_rxr = rxr.open_rasterio(dem.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})
            lu_rxr = rxr.open_rasterio(landuse_vrt.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})

            # Burn dem nodata into landuse
            result = lu_rxr.where(dem_rxr != dem_rxr.rio.nodata, lu_rxr.rio.nodata)

            # Write to file
            result.rio.to_raster(
                landuse_tif.base,
                chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
                compress="ZSTD",
                tiled=True,
                PREDICTOR=2,
                ZSTD_LEVEL=1,
                dtype="int16",
            )
        print(f"{landuse_tif.name} created")
        return landuse_tif, True
    else:
        print(f"{landuse_tif.name} already on system")
        return landuse_tif, False


# %%
if __name__ == "__main__":
    folder = htt.Folders(r"E:\02.modellen\Z0084_-_Lange_Weeren_-_bestaande_situatie")

    dem = folder.model.schema_base.rasters.dem
    landuse_name = "lange_weeren_ref"
    output_dir = None
    landuse_hhnk = hrt.Raster(
        r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt"
    )
    overwrite = False

    create_landuse_polder_clip(
        dem=dem, landuse_name=landuse_name, output_dir=output_dir, landuse_hhnk=landuse_hhnk, overwrite=overwrite
    )
