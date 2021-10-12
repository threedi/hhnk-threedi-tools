
import hhnk_research_tools as hrt
import gdal
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy import ndimage

#TODO moet naar hhnk-research-tools
class Raster():
    def __init__(self, source_path):
        self.source_path = source_path
        self.source = source_path #calls self.source.setter(source_path)
        self.band_count = self.source.RasterCount
        self._array = None
        self.nodata = self.source.GetRasterBand(1).GetNoDataValue()
        self.metadata = None


    @property
    def array(self):
        if self._array  is not None:
            print('from memory')
            return self._array
        else:
            print('Array not loaded. Call Raster.get_array(window) first')
            return self._array

    @array.setter
    def array(self, raster_array):
        self._array = raster_array

    def _read_array(self, band=None, window=None):
        """window=[x0, y0, x1, y1]"""
        if band == None:
            band = self.source.GetRasterBand(1) #TODO band is not closed properly

        if window is not None:
            raster_array = band.ReadAsArray(
                xoff=int(window[0]),
                yoff=int(window[1]),
                win_xsize=int(window[2] - window[0]),
                win_ysize=int(window[3] - window[1]))
        else:
            raster_array = band.ReadAsArray()

        band.FlushCache()  # close file after writing
        band = None

        return raster_array

    def get_array(self, window=None):
        try:
            if self.band_count == 1:
                raster_array = self._read_array(band=self.source.GetRasterBand(1), 
                                                window=window)

            elif self.band_count == 3:
                red_array = self._read_array(band=self.source.GetRasterBand(1), 
                                                window=window)
                green_array = self._read_array(band=self.source.GetRasterBand(2), 
                                                window=window)   
                blue_array = self._read_array(band=self.source.GetRasterBand(3), 
                                window=window)                                                                            

                raster_array = np.dstack((red_array, green_array, blue_array))
            else:
                raise ValueError(
                    f"Unexpected number of bands in raster {self.source_path} (expect 1 or 3)"
                )
            self._array = raster_array
            return raster_array

        except Exception as e:
            raise e from None


    @property
    def source(self):
        return self._source


    @source.setter
    def source(self, value):
        self._source=gdal.Open(value)


    @property
    def metadata(self):
        return self._metadata


    @metadata.setter
    def metadata(self, val) -> dict:
        meta = {}
        meta["proj"] = self.source.GetProjection()
        meta["georef"] = self.source.GetGeoTransform()
        meta["pixel_width"] = meta["georef"][1]
        meta["x_min"] = meta["georef"][0]
        meta["y_max"] = meta["georef"][3]
        meta["x_max"] = meta["x_min"] + meta["georef"][1] * self.source.RasterXSize
        meta["y_min"] = meta["y_max"] + meta["georef"][5] * self.source.RasterYSize
        meta["bounds"] = [meta["x_min"], meta["x_max"], meta["y_min"], meta["y_max"]]
        # for use in threedi_scenario_downloader
        meta["bounds_dl"] = {
            "west": meta["x_min"],
            "south": meta["y_min"],
            "east": meta["x_max"],
            "north": meta["y_max"],
        }
        meta["x_res"] = self.source.RasterXSize
        meta["y_res"] = self.source.RasterYSize
        meta["shape"] = [meta["y_res"], meta["x_res"]]

        self._metadata = meta

    def plot(self):
        plt.imshow(self._array)


    @property
    def shape(self):
        return self.metadata['shape']

    def _iter_block_row(self, band, ncols, offset_y, block_height, block_width, no_data_value, return_array=True):
        """For given row in a raster, iterate over columns"""
        for i in range(ncols):
            print(f"i={i}")
            window = [i * block_width, offset_y,
                                (i + 1) * block_width, offset_y + block_height]
            if return_array:
                arr=self._read_array(band=band, window=window)

                if no_data_value is not None:
                    arr[arr == no_data_value] = 0.
            else: 
                arr=None
            yield window, arr

        # possible leftover block
        width = band.XSize - (ncols * block_width)
        if width > 0:
            window=[ncols * block_width, offset_y,
                ncols * block_width + width, offset_y + block_height]
            if return_array:
                arr=self._read_array(band=band, window=window)

                if no_data_value is not None:
                    arr[arr == no_data_value] = 0.
            else:
                arr=None

            yield window, arr


    def iter_blocks(self, band_nr=1, min_blocksize=0, return_array=True):
        """ Iterate over native blocks in a GDal raster data band.
        Optionally, provide a minimum block dimension.
        Returns a tuple of bbox (x1, y1, x2, y2) and the data as ndarray. """

        band = self.source.GetRasterBand(band_nr)

        block_height, block_width = band.GetBlockSize()
        block_height = max(min_blocksize, block_height)
        block_width = max(min_blocksize, block_width)


        nrows = int(band.YSize / block_height)
        ncols = int(band.XSize / block_width)
        no_data_value = band.GetNoDataValue()

        #Iterate over a whole row. Call function to iterate within that row
        for j in range(nrows):
            #Iterate within row over columns to yield block
            for bbox, block in self._iter_block_row(band=band, 
                                            ncols=ncols,
                                            offset_y=j * block_height, 
                                            block_height=block_height,
                                            block_width=block_width, 
                                            no_data_value=no_data_value,
                                            return_array=return_array):
                yield bbox, block

        # possible leftover row
        height = band.YSize - (nrows * block_height)
        if height > 0:
            for bbox, block in self._iter_block_row(band=band, 
                                            ncols=ncols,
                                            offset_y=nrows * block_height, 
                                            block_height=height,
                                            block_width=block_width, 
                                            no_data_value=no_data_value,
                                            return_array=return_array):
                yield bbox, block
        band.FlushCache()  # close file after writing
        band = None
        print('Done')


    def generate_blocks(self, min_blocksize=0):
        #TODO blocksize
        band = self.source.GetRasterBand(1)

        block_height, block_width = band.GetBlockSize()
        block_height = max(min_blocksize, block_height)
        block_width = max(min_blocksize, block_width)


        ncols = int(max(1, np.floor(band.XSize / block_width)))
        nrows = int(max(1, np.floor(band.YSize / block_height)))

        xparts = np.linspace(0, band.XSize, ncols+1).astype(int)
        yparts = np.linspace(0, band.YSize, nrows+1).astype(int)

        parts = pd.DataFrame(index=np.arange(nrows*ncols)+1, columns=['ix', 'iy', 'window'])
        i = 0
        for ix in range(ncols):
            for iy in range(nrows):
                i += 1
                parts.loc[i, :] = np.array((ix, iy, [xparts[ix], yparts[iy], xparts[ix+1], yparts[iy+1]]), dtype=object)


        band.FlushCache()  # close file after writing
        band = None
        return parts




    def sum_labels(self, labels_raster, labels_index, min_blocksize):
        parts = self.generate_blocks(min_blocksize=min_blocksize)
        accum = None

        if labels_raster.shape != self.shape:
            raise Exception(f'label raster shape {labels_raster.shape} does not match the raster shape {self.shape}')

        for idx, part in parts.iterrows():
            window=part['window']
            block = self._read_array(window=window)
            block_label = labels_raster._read_array(window=window)


            #Calculate sum per label (region)
            result = ndimage.sum_labels(input=block,
                                    labels=block_label,
                                    index=labels_index) #Which values in labels to take into account.

            if accum is None:
                accum = result
            else:
                accum += result
        return accum

    def to_file():
        pass

    def __iter__(self):
        blocks = self.generate_blocks()
        for idx, part in blocks.iterrows():
            window=part['window']
            block = self._read_array(window=window)
            yield window, block
            

    def __repr__(self):
        return f"""{self.__class__}
Source: {self.source_path}
Shape: {self.metadata['shape']}
Pixelsize: {self.metadata['pixel_width']}"""
