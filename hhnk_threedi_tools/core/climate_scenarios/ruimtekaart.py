# %% debug

"""
Compute the sum of 12 rasterfiles in polygons given by a shapefile and compute
the "ruimte-indicator". Optionally, a mask shapefile can be provided.
"""
from osgeo import gdal
import argparse
import logging
import os
import sys

import numpy as np
from scipy import ndimage

import hhnk_research_tools as hrt
import geopandas as gpd

logger = logging.getLogger(__name__)


DAMAGE_TIFF_NAMES = (
    'blok_GGG_T10.tif',
    'blok_GHG_T10.tif',
    'blok_GGG_T100.tif',
    'blok_GHG_T100.tif',
    'blok_GGG_T1000.tif',
    'blok_GHG_T1000.tif',
)
MAXDEPTH_TIFF_NAMES = DAMAGE_TIFF_NAMES

ATTRS = {
    'damage_0': 'dggt10',
    'damage_1': 'dght10',
    'damage_2': 'dggt100',
    'damage_3': 'dght100',
    'damage_4': 'dggt1000',
    'damage_5': 'dght1000',
    'volume_0': 'mggt10',
    'volume_1': 'mght10',
    'volume_2': 'mggt100',
    'volume_3': 'mght100',
    'volume_4': 'mggt1000',
    'volume_5': 'mght1000',
    'deuro_per_dm3_0': 'ekght10',
    'deuro_per_dm3_1': 'ekggt100',
    'deuro_per_dm3_2': 'ekght100',
    'deuro_per_dm3_3': 'ekggt1000',
    'deuro_per_dm3_4': 'ekght1000',
    'deuro_per_dm3_norm_0': 'nekght10',
    'deuro_per_dm3_norm_1': 'nekggt100',
    'deuro_per_dm3_norm_2': 'nekght100',
    'deuro_per_dm3_norm_3': 'nekggt1000',
    'deuro_per_dm3_norm_4': 'nekght1000',
    'indicator': 'indicator',
}
MIN_BLOCKSIZE = 1024  # process rasters using at least 1024x1024 blocks
MAX_NUM_REGIONS = 2 ** 16 - 1  # uint16 dtype + nodata value


def _iter_block_row(band, offset_y, block_height, block_width, no_data_value):

    ncols = int(band.XSize / block_width)

    for i in range(ncols):
        arr = band.ReadAsArray(i * block_width, offset_y,
                               block_width, block_height)
        if no_data_value is not None:
            arr[arr == no_data_value] = 0.
        yield (i * block_width, offset_y,
               (i + 1) * block_width, offset_y + block_height), arr

    # possible leftover block
    width = band.XSize - (ncols * block_width)
    if width > 0:
        arr = band.ReadAsArray(ncols * block_width, offset_y,
                               width, block_height)
        if no_data_value is not None:
            arr[arr == no_data_value] = 0.
        yield (ncols * block_width, offset_y,
               ncols * block_width + width, offset_y + block_height), arr


def iter_blocks(band, min_blocksize=0):
    """ Iterate over native blocks in a GDal raster data band.
    Optionally, provide a minimum block dimension.
    Returns a tuple of bbox (x1, y1, x2, y2) and the data as ndarray. """
    block_height, block_width = band.GetBlockSize()
    block_height = max(min_blocksize, block_height)
    block_width = max(min_blocksize, block_width)

    nrows = int(band.YSize / block_height)
    no_data_value = band.GetNoDataValue()
    for j in range(nrows):
        for block in _iter_block_row(band, j * block_height, block_height,
                                     block_width, no_data_value):
            yield block

    # possible leftover row
    height = band.YSize - (nrows * block_height)
    if height > 0:
        for block in _iter_block_row(band, nrows * block_height,
                                     height, block_width,
                                     no_data_value):
            yield block


def rasterize(pgb_gdf, raster_path, mask_path=None):
    """Rasterize the shapefile with a unique id per region."""
    _, nodata, metadata = hrt.load_gdal_raster(raster_source=raster_path, return_array=False)

    #Rasterize areas, giving each region a unique id. 
    labels_array = hrt.gdf_to_raster(gdf=pgb_gdf,
        value_field='index',
        raster_out='',
        nodata=nodata,
        metadata=metadata,
        driver='MEM')

    #Rasterize mask file to remove any areas that are not within the mask
    mask_gdf = gpd.read_file(mask_path)
    mask_gdf['val']= 1
    mask = hrt.gdf_to_raster(gdf=mask_gdf,
        value_field='val',
        raster_out='',
        nodata=0,
        metadata=metadata,
        driver='MEM',
        datatype=gdal.GDT_Byte)

    labels_array[mask!=1] = nodata

    #Overview of available regions within mask. 
    unique_labels = np.unique(labels_array).tolist()
    unique_labels.remove(nodata)
    return labels_array, metadata, unique_labels


def aggregate(raster_path, labels_array, pgb_gdf, min_blocksize=1024):
    """Calculate sum of raster per region"""
    #Rasterize regions giving each region unique id. 

    source = gdal.Open(raster_path, gdal.GA_ReadOnly)
    band = source.GetRasterBand(1)

    #Loop over raster by generating blocks. Then calculate the sum of values of the raster for each region 
    accum = None
    for bbox, block in iter_blocks(band, min_blocksize=min_blocksize):
        #Calculate sum per label (region)
        result = ndimage.sum_labels(input=block,
                                labels=labels_array[bbox[1]:bbox[3], bbox[0]:bbox[2]],
                                index=pgb_gdf['index'].values) #Which values in labels to take into account.

        if accum is None:
            accum = result
        else:
            accum += result


    return accum


def command(shapefile_path, output_path, maxdepth_prefix, damage_prefix,
            mask_path):
    """Calcualtion of ruimtekaart. Calculates the total volume 
    and damage per region for multiple calculations and calculates
    an indicator whether it is relatively cheap the store extra
    water in that region."""
    peilgebieden_df = gpd.read_file(shapefile_path)
    num_regions = len(peilgebieden_df)


    pgb_gdf = peilgebieden_df[['peil_id','code','geometry']].copy()
    pgb_gdf.reset_index(inplace=True,drop=False)
    # aggregate the 12 input rasters
    damages_euro = np.zeros((num_regions, 6), dtype=float)
    volumes_m3 = np.zeros((num_regions, 6), dtype=float)

    #Aggregate sum per region for each result for both the depth and damage rasters

    #rasterize regions
    labels_depth_array, metadata, unique_labels = rasterize(pgb_gdf=pgb_gdf, raster_path=maxdepth_prefix+MAXDEPTH_TIFF_NAMES[0], mask_path=mask_path)

    for i, fn in enumerate(MAXDEPTH_TIFF_NAMES):
        raster_path = maxdepth_prefix + fn
        logger.info("Aggregating '{}'".format(raster_path))

        #calculate sum per region.
        volumes_m3[:, i] = aggregate(raster_path=raster_path,
                                                labels_array=labels_depth_array, 
                                                pgb_gdf=pgb_gdf, 
                                                min_blocksize=MIN_BLOCKSIZE)
        volumes_m3[:, i] *= abs(metadata['georef'][1] * metadata['georef'][5]) #take pixelsize into account. 

    #rasterize regions (damage may have other resolution)
    labels_damage_array, metadata, unique_labels = rasterize(pgb_gdf=pgb_gdf, raster_path=damage_prefix+DAMAGE_TIFF_NAMES[0], mask_path=mask_path)

    for i, fn in enumerate(DAMAGE_TIFF_NAMES):
        raster_path = damage_prefix + fn
        logger.info("Aggregating '{}'".format(raster_path))
        #calculate sum per region.
        damages_euro[:, i] = aggregate(raster_path=raster_path, 
                                                labels_array=labels_damage_array,
                                                pgb_gdf=pgb_gdf, 
                                                min_blocksize=MIN_BLOCKSIZE)
            

    # add the total sum per raster to the last row
    m3 = np.concatenate([volumes_m3,
                        np.sum(volumes_m3, axis=0)[np.newaxis]], axis=0)
    euro = np.concatenate([damages_euro,
                        np.sum(damages_euro, axis=0)[np.newaxis]], axis=0)

    # take the forward differential
    d_euro = np.diff(euro, axis=1)
    d_m3 = np.diff(m3, axis=1)

    # how many euros extra per m3 extra?
    mask = (d_m3 != 0)#.all(axis=1)
    d_euro_per_m3 = np.full_like(d_m3, np.nan)
    d_euro_per_m3[mask] = np.clip(d_euro[mask] / d_m3[mask], 0, 1E100)

    # normalize on the total sum
    d_euro_per_m3_norm = \
        (d_euro_per_m3[-1] - d_euro_per_m3) / d_euro_per_m3[-1]

    # compute the indicator, ignoring NaNs
    weights = np.tile([[1., 2., 2., 5., 5.]], (d_euro_per_m3_norm.shape[0], 1))
    weights[np.isnan(d_euro_per_m3_norm)] = 0

    #toevoeging 2021-03-02 Als er wel toename in m3 maar maar afname in schade of andersom over de som van alle peilgebieden gaat
    #gaat de hele berekening plat. Daarom worden deze scenarios niet mee genomen in de berekening van de indicator. Voor zijpe noord
    #zijpe zuid was dit het geval.
    weights[np.isinf(d_euro_per_m3_norm)] = 0 
    weights_total = weights.sum(1)
    weights_total[weights_total == 0] = np.nan
    indicator = np.nansum(d_euro_per_m3_norm * weights, axis=1) / weights_total

    # add the results to the output file
    # set the total damages
    for j in range(6):
        pgb_gdf[ATTRS['damage_{}'.format(j)]] = euro[:-1, j]

    # set the total volumes
    for j in range(6):
        pgb_gdf[ATTRS['volume_{}'.format(j)]] = m3[:-1, j]

    # set the incremental euro/m3
    for j in range(5):
        pgb_gdf[ATTRS['deuro_per_dm3_{}'.format(j)]] = d_euro_per_m3[:-1, j]

    # set the normalized incremental euro/m3
    for j in range(5):
        pgb_gdf[ATTRS['deuro_per_dm3_norm_{}'.format(j)]] = \
            d_euro_per_m3_norm[:-1, j]

    # set the indicator
    pgb_gdf[ATTRS['indicator']] = indicator[:-1]
    
    pgb_gdf = pgb_gdf.loc[unique_labels] #Drop regions that do that fall within the rasters. 
    pgb_gdf['relmmt1000'] = np.round(pgb_gdf["mght1000"]*1000 / sum(pgb_gdf.area),2)


    pgb_gdf.to_file(output_path)


def get_parser():
    """
    Compute the sum of 12 rasterfiles in region given by polygons in a
    shapefile. The 12 raster files are suffixed by shapefile and compute
    the "ruimte-indicator". Optionally, a mask shapefile can be provided.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'shapefile_path',
        help='Path to a shapefile that contains the region polygons.',
    )
    parser.add_argument(
        'output_dir',
        help='Directory name to use as output path. The directory should not '
             'exist.',
    )
    parser.add_argument(
        '-x', '--maxdepth_prefix',
        default='rasters/maxdepth_',
        dest='maxdepth_prefix',
        help=('Prefix of 6 maxdepth tiffiles (in meters). '
              'The filenames ' + ', '.join(MAXDEPTH_TIFF_NAMES) +
              ' will be prefixed by this. Default: "rasters/maxdepth_".'),
    )
    parser.add_argument(
        '-d', '--damage_prefix',
        default='rasters/damage_',
        dest='damage_prefix',
        help=('Prefix of  6 damage tiffiles (in euros). '
              'The filenames ' + ', '.join(DAMAGE_TIFF_NAMES) +
              ' will be prefixed by this. Default: "rasters/damage_".'),
    )
    parser.add_argument(
        '-m', '--mask',
        default='',
        dest='mask_path',
        help='Optional path to a mask shapefile containing polygons of areas '
             'to include in this analysis.',
    )
    return parser


def main():
    """ Call command with args from parser. """
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(message)s')

    command(**vars(get_parser().parse_args()))
# %%
