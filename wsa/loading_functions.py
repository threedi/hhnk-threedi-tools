import gdal
import numpy as np

def get_array_from_bands(gdal_file, band_count, window, raster_source):
    try:
        if band_count == 1:
            band = gdal_file.GetRasterBand(1)
            if window is not None:
                raster_array = band.ReadAsArray(xoff=window[0], yoff=window[1],
                                                win_xsize=window[2] - window[0],
                                                win_ysize=window[3] - window[1])
            else:
                raster_array = band.ReadAsArray()
            return raster_array
        elif band_count == 3:
            if window is not None:
                red_arr = gdal_file.GetRasterBand(1).ReadAsArray(xoff=window[0], yoff=window[1],
                                                                 win_xsize=window[2] - window[0],
                                                                 win_ysize=window[3] - window[1])
                green_arr = gdal_file.GetRasterBand(2).ReadAsArray(xoff=window[0], yoff=window[1],
                                                                   win_xsize=window[2] - window[0],
                                                                   win_ysize=window[3] - window[1])
                blue_arr = gdal_file.GetRasterBand(3).ReadAsArray(xoff=window[0], yoff=window[1],
                                                                  win_xsize=window[2] - window[0],
                                                                  win_ysize=window[3] - window[1])
            else:
                red_arr = gdal_file.GetRasterBand(1).ReadAsArray()
                green_arr = gdal_file.GetRasterBand(2).ReadAsArray()
                blue_arr = gdal_file.GetRasterBand(3).ReadAsArray()
            raster_arr = np.dstack((red_arr, green_arr, blue_arr))
            return raster_arr
        else:
            raise ValueError(f"Unexpected number of bands in raster {raster_source} (expect 1 or 3)")
    except Exception as e:
        raise e from None

def get_gdal_metadata(gdal_file):
    try:
        meta = {}
        meta['proj'] = gdal_file.GetProjection()
        meta['georef'] = gdal_file.GetGeoTransform()
        meta['pixel_width'] = meta['georef'][1]
        meta['x_min'] = meta['georef'][0]
        meta['y_max'] = meta['georef'][3]
        meta['x_max'] = meta['x_min'] + meta['georef'][1] * gdal_file.RasterXSize
        meta['y_min'] = meta['y_max'] + meta['georef'][5] * gdal_file.RasterYSize
        meta['bounds'] = [meta['x_min'], meta['x_max'], meta['y_min'], meta['y_max']]
        # for use in threedi_scenario_downloader
        meta['bounds_dl'] = {'west': meta['x_min'], 'south': meta['y_min'], 'east': meta['x_max'],
                             'north': meta['y_max']}
        meta['x_res'] = gdal_file.RasterXSize
        meta['y_res'] = gdal_file.RasterYSize
        meta['shape'] = [meta['y_res'], meta['x_res']]
        return meta
    except Exception as e:
        raise e from None

def load_gdal_raster(raster_source, window=None):
    """
    Loads a raster (tif) and returns an array of its values, its no_data value and
    dict containing associated metadata
    """
    try:
        gdal_file = gdal.Open(raster_source)
        if gdal_file:
            band_count = gdal_file.RasterCount
            raster_array = get_array_from_bands(gdal_file, band_count, window, raster_source)
            # are they always same even if more bands?
            no_data = gdal_file.GetRasterBand(1).GetNoDataValue()
            metadata = get_gdal_metadata(gdal_file)
            return raster_array, no_data, metadata
    except Exception as e:
        raise e from None
