

import hhnk_threedi_tools.utils.notebooks.rasterclass as rasterclass #TODO to hrt.
import hhnk_research_tools as hrt
import geopandas as gpd
from osgeo import gdal
import numpy as np





def rasterize_peilgebieden(input_raster, output_file, input_peilgebieden, output_peilgebieden, mask_path, overwrite=False):
    #Rasterize regions
    if not output_file.exists:
        create=True
    else:
        create=False
        #Check edit times. To see if raster needs to be updated.
        raster_mtime = output_file.pl.stat().st_mtime
        shape_mtime = input_peilgebieden.pl.stat().st_mtime
        
        if shape_mtime>raster_mtime:
            create=True
    if overwrite:
        create=True    
    
    if create:
        #TODO dit is niet goed voor het geheugen... Is het wel nodig?
        pgb_gdf = gpd.read_file(input_peilgebieden.path)
        pgb_gdf.reset_index(drop=False, inplace=True)

        #Rasterize areas, giving each region a unique id. 
        labels_array = hrt.gdf_to_raster(gdf=pgb_gdf,
            value_field='index',
            raster_out='',
            nodata=input_raster.nodata,
            metadata=input_raster.metadata,
            driver='MEM')


        mask_gdf = gpd.read_file(mask_path)
        mask_gdf['val']=1
        mask_array = hrt.gdf_to_raster(gdf=mask_gdf,
            value_field='val',
            raster_out='',
            nodata=input_raster.nodata,
            metadata=input_raster.metadata,
            driver='MEM')

        #Apply mask to labels. Use DEM.
        labels_array[mask_array!=1] = input_raster.nodata

        hrt.save_raster_array_to_tiff(output_file=output_file.path, 
                                        raster_array=labels_array, 
                                        nodata=input_raster.nodata, 
                                        metadata=input_raster.metadata)


        print(f'{output_file.name} created')

        unique_labels = np.unique(labels_array[labels_array!=input_raster.nodata])

        pgb_masked = pgb_gdf.loc[unique_labels][['index', 'peil_id','code', 'name','geometry']]

        # pgb_masked.drop('level_0', axis=1, inplace=True)
        pgb_masked.reset_index(drop=True, inplace=True)
        pgb_masked.to_file(output_peilgebieden.path)

    else:
        print(f'{output_file.name} already exists')

