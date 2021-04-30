import gdal
from ..variables.types import GEOTIFF, GDAL_DATATYPE

def set_band_data(data_source, num_bands, nodata):
    try:
        for i in range(1, num_bands + 1):
            band = data_source.GetRasterBand(i)
            band.SetNoDataValue(nodata)
            band.Fill(nodata)
    except Exception as e:
        raise e from None

def create_new_raster_file(file_name, nodata, meta, driver=GEOTIFF,
                           datatype=GDAL_DATATYPE, compression='DEFLATE',
                           num_bands=1, tiled='YES'):
    """
    ONLY FOR SINGLE BAND
    Create new empty gdal raster using metadata from raster from sqlite (dem)
    driver='GTiff'
    driver='MEM'
    Compression:
    LZW - highest compression ratio, highest processing power
    DEFLATE
    PACKBITS - lowest compression ratio, lowest processing power
    """
    try:
        target_ds = gdal.GetDriverByName(driver).Create(file_name,
                                                        meta['x_res'],
                                                        meta['y_res'],
                                                        num_bands,
                                                        datatype,
                                                        options=[f'COMPRESS={compression}',
                                                                 f'TILED={tiled}'])
        target_ds.SetGeoTransform(meta['georef'])
        set_band_data(target_ds, num_bands, nodata)
        target_ds.SetProjection(meta['proj'])
        return target_ds
    except Exception as e:
        raise e from None

def save_raster_array_to_tiff(output_file, raster_array, nodata, metadata, datatype=GDAL_DATATYPE,
                              compression='DEFLATE', num_bands=1):
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
        target_ds = create_new_raster_file(output_file, nodata,
                                           metadata, datatype=datatype,
                                           compression=compression)  # create new raster
        for i in range(1, num_bands + 1):
            target_ds.GetRasterBand(i).WriteArray(raster_array)  # fill file with data
    except Exception as e:
        raise e from None
