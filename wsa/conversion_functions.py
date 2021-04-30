import json
import gdal
import ogr
from shapely.geometry import LineString
from ..variables.default_variables import DEF_TRGT_CRS
from ..variables.types import GDAL_DATATYPE, GEOTIFF
from ..read_write_functions.write_to_file_functions import ensure_file_path
from .saving_functions import create_new_raster_file

def line_geometries_to_coords(lines):
    """
    Coordinates read from threedi results netcdf can't be used as is in geodataframe
    Usage: lines = results.lines.channels.line_geometries where results = GridH5ResultAdmin object
    """
    coords = []
    for line in lines:
        if len(line) >= 4:
            x_coords = line[:int(line.size / 2)].tolist()
            y_coords = line[int(line.size / 2):].tolist()
        else:
            # Fill in dummy coords
            x_coords = [0.0, 25000]
            y_coords = [0.0, 25000]
        line_list = []
        for x, y in zip(x_coords, y_coords):
            line_list.append([x, y])
        # Creates set of ([x1, y1], [x2, y2] ...., [xn, yn])
        coords.append(LineString(line_list))
    return coords

def gdf_to_json(gdf, epsg=DEF_TRGT_CRS):
    try:
        gdf_json = json.loads(gdf.to_json())
        gdf_json["crs"] = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::{}".format(epsg)}}
        gdf_json_str = json.dumps(gdf_json)
        return gdf_json_str
    except Exception as e:
        raise e from None

def gdf_to_ogr(gdf, epsg=DEF_TRGT_CRS):
    try:
        gdf_json = gdf_to_json(gdf, epsg)
        ogr_ds = ogr.Open(gdf_json)
        polygon = ogr_ds.GetLayer()
        return ogr_ds, polygon
    except Exception as e:
        raise e from None

def gdf_to_raster(gdf, value_field, raster_out, nodata, metadata, epsg=DEF_TRGT_CRS, driver=GEOTIFF,
                      datatype=GDAL_DATATYPE, compression="DEFLATE", tiled="YES"):
    """Dem is used as format raster. The new raster gets meta data from the DEM. A gdf is turned into ogr layer and is
    then rasterized.
    wsa.polygon_to_raster(polygon_gdf=mask_gdf[mask_type], valuefield='val', raster_output_path=mask_path[mask_type],
    nodata=0, meta=meta, epsg=28992, driver='GTiff')
    """
    try:
        ogr_ds, polygon = gdf_to_ogr(gdf, epsg)
        # make sure folders exist
        ensure_file_path(raster_out)
        new_raster = create_new_raster_file(file_name=raster_out,
                                            nodata=nodata,
                                            meta=metadata,
                                            driver=driver,
                                            datatype=datatype)
        gdal.RasterizeLayer(new_raster, [1], polygon,
                            options=[f"ATTRIBUTE={value_field}",
                                     f"COMPRESS={compression}",
                                     f"TILED={tiled}"])
        raster_array = new_raster.ReadAsArray()
        return raster_array
    except Exception as e:
        raise e from None
