# %%

import hhnk_research_tools as hrt
import numpy as np
from osgeo import gdal

src = r"E:\02.modellen\23_Katvoed\02_schematisation\00_basis\rasters\dem_katvoed_ahn4.tif"
dst = r"E:\02.modellen\23_Katvoed\03_3di_results\1d2d_results\testdem.tif"

r = hrt.Raster(src)

scaled=False


# compressions = ["None", "LZW", "DEFLATE", "PACKBITS"]
# compressions = ["LERC_DEFLATE"]
compressions = ["ZSTD"]
for scaled in [True, False]:
    # if scaled:
    #     continue
    for compression in compressions:
        if not scaled:
            dst = fr"E:\02.modellen\23_Katvoed\03_3di_results\1d2d_results\testdem_normal_{compression}.tif"
            arr = r._read_array()
            datatype=gdal.GDT_Float32
            nodata=r.nodata
            
        else:
            dst = fr"E:\02.modellen\23_Katvoed\03_3di_results\1d2d_results\testdem_scaled_{compression}.tif"
            arr = r._read_array()
            mask = arr==r.nodata
            arr*=100
            nodata= np.iinfo("int16").min
            arr[mask] = nodata
            datatype=gdal.GDT_Int16


        tiled="YES"
        driver = GEOTIFF = "GTiff"

        def save_raster_array_to_tiff(
            output_file,
            raster_array,
            nodata,
            metadata,
            datatype,
            num_bands=1,
        ):
            """
            ONLY FOR SINGLE BAND

            input:
            output_file (filepath)
            raster_array (values to be converted to tif)
            nodata (nodata value)
            metadata (dictionary)
            datatype -> gdal.GDT_Float32
            compression -> 'DEFLATE'
            num_bands -> 1
            """
            try:
                target_ds = hrt.create_new_raster_file(
                    file_name=output_file,
                    nodata=nodata,
                    meta=metadata,
                    datatype=datatype,
                )  # create new raster
                for i in range(1, num_bands + 1):
                    target_ds.GetRasterBand(i).WriteArray(raster_array)  # fill file with data
                target_ds = None
            except Exception as e:
                raise e


        save_raster_array_to_tiff(output_file=dst,
                                    raster_array=arr,
                                    nodata=nodata,
                                    metadata=r.metadata,
                                    datatype=datatype,
                                    compression=compression)

        if scaled:
            d=hrt.Raster(dst)
            b = d._source.GetRasterBand(1)
            b.SetScale(0.01)
            d=None
            b=None

# %%

