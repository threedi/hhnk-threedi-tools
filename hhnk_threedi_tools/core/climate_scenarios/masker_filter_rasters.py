import geopandas as gpd
import pandas as pd
from osgeo import gdal
import numpy as np
from shapely.geometry import MultiPolygon, Polygon
import os
import hhnk_research_tools as hrt

# import PIL.Image, PIL.ImageDraw #maskerkaart
# def geometry_to_raster_mask(geometry, extent, cellsize, shape):
#     """Convert input SINGLE polygon not gdf (geometry) in a mask with given boundaries.
#     output is a mask list (equal size to the raster_array), with True if the polygon is on the cell"""

#     def get_mask(linestring, extent, cellsize, shape):
#         # Create array from coordinate sequence
#         path = np.vstack(linestring.coords[:])

#         # Convert to (0,0) and step size 1
#         path[:, 0] -= extent[0]
#         path[:, 1] -= extent[2]
#         path /= cellsize
#         # Convert from array to tuple list
#         path = list(zip(*zip(*path)))

#         # Create mask
#         maskIm = PIL.Image.new('L', (shape[1], shape[0]), 0)
#         PIL.ImageDraw.Draw(maskIm).polygon(path, outline=None, fill=1)
#         mask = np.array(maskIm)[::-1]
#         return mask

#     # Initialize mask
#     mask = np.zeros(shape)

#     # Collect polygons
#     polygons = []
#     if isinstance(geometry, Polygon):
#         polygons.append(geometry)
#     elif isinstance(geometry, MultiPolygon):
#         for p in geometry:
#             polygons.append(p)

#     for polygon in polygons:
#         # Create from exterior
#         mask += get_mask(polygon.exterior, extent, cellsize, shape)
#         # Subtract interiors
#         for interior in polygon.interiors:
#             mask -= get_mask(interior, extent, cellsize, shape)

#     mask = mask > 0
#     return mask


# def maskerkaart_inladen(input_file, meta):
#     """Laden van de masker rasters, die gemaakt worden vanuit de maskerkaart.
#     mask=maskerkaart_inladen(input_file=batch_fd['02_output_rasters']['maskerkaart'], meta=damage_meta)"""
#     # Maak polygoon van watersysteemgerelateerde inundatie
#     maskerkaart = gpd.read_file(input_file).dropna(how='any')

#     #let op hoe kolom heet: case, case_final af final_case
#     overlast = maskerkaart.buffer(0.1).loc[maskerkaart.case_final == 'overlast'].unary_union.buffer(-0.1)
#     plas = maskerkaart.buffer(0.1).loc[maskerkaart.case_final == 'plas'].unary_union.buffer(-0.1)

#     mask = {
#         'overlast' : geometry_to_raster_mask(overlast, meta['bounds'], meta['pixel_width'], shape=meta['shape']),
#         'plas' : geometry_to_raster_mask(plas, meta['bounds'], meta['pixel_width'], shape=meta['shape'])
#     }
#     return mask


def rasterize_maskerkaart(input_file, mask_plas_path, mask_overlast_path, meta):
    """Aanmaken van de masker rasters, die gemaakt worden vanuit de maskerkaart, voor plasvorminge en wateroverlast.
    rasterize_maskerkaart(input_file=batch_fd['02_output_rasters']['maskerkaart'],
                      mask_plas_path=batch_fd['02_output_rasters']['mask_plas'],
                      mask_overlast_path=batch_fd['02_output_rasters']['mask_overlast'],
                      meta=depth_meta)"""
    # Maak polygoon van watersysteemgerelateerde inundatie
    maskerkaart_gdf = gpd.read_file(input_file).dropna(how="any")

    mask_gdf = {}
    mask = {}

    mask_path = {}
    mask_path["plas"] = mask_plas_path
    mask_path["overlast"] = mask_overlast_path

    for mask_type in ["plas", "overlast"]:
        if not os.path.exists(mask_path[mask_type]):
            # Repareer geometry
            temp_geom = (
                maskerkaart_gdf.buffer(0.1)
                .loc[maskerkaart_gdf.case_final == mask_type]
                .unary_union.buffer(-0.1)
            )
            mask_gdf[mask_type] = gpd.GeoDataFrame(geometry=[temp_geom])
            # Voeg kolom toe aan gdf, deze waarden worden in het raster gezet.
            mask_gdf[mask_type]["val"] = 1

            mask[mask_type] = hrt.gdf_to_raster(
                gdf=mask_gdf[mask_type],
                value_field="val",
                raster_out=mask_path[mask_type],
                nodata=0,
                metadata=meta,
                datatype=gdal.GDT_Byte,
            )
            print("{} created".format(mask_path[mask_type]))
        else:
            print("{} already exists".format(mask_path[mask_type]))
    return mask


def rasterize_maskerkaart_lizard(input_file, mask_all_path, meta, nodata, datatype):
    """
    Input maskerkaart for lizard.
    input_file=batch_fd['02_output_rasters']['maskerkaart']
    mask_all_path=batch_fd['02_output_rasters']['maskerkaart_tif']
    meta=damage_meta
    nodata=damage_nodata"""
    # Maak polygoon van watersysteemgerelateerde inundatie
    if not os.path.exists(mask_all_path):
        maskerkaart_gdf = gpd.read_file(input_file).dropna(how="any")

        mask_gdf = {}
        mask = {}

        for mask_type in maskerkaart_gdf.case_final.unique():
            value = {"overlast": 1, "plas": 2, "modelfout": 3}

            # Repareer geometry
            temp_geom = (
                maskerkaart_gdf.buffer(0.1)
                .loc[maskerkaart_gdf.case_final == mask_type]
                .unary_union.buffer(-0.1)
            )
            mask_gdf[mask_type] = gpd.GeoDataFrame(geometry=[temp_geom])
            # Voeg kolom toe aan gdf, deze waarden worden in het raster gezet.
            mask_gdf[mask_type]["val"] = value[mask_type]

        mask_gdf_all = pd.concat([mask_gdf[x] for x in mask_gdf], ignore_index=True)
        mask_gdf_all = gpd.GeoDataFrame(
            mask_gdf_all, geometry="geometry", crs=maskerkaart_gdf.crs
        )
        hrt.gdf_to_raster(
            gdf=mask_gdf_all,
            value_field="val",
            raster_out=mask_all_path,
            nodata=nodata,
            metadata=meta,
            datatype=datatype,
        )
        print("{} created".format(mask_all_path))
    else:
        print("{} already exists".format(mask_all_path))


def rasterize_geen_schade_lizard(input_file, output_file, meta, nodata, datatype):
    """Create raster from geen_schade.shp
    input_file=batch_fd['02_output_rasters']['maskerkaart']
    output_file=batch_fd['02_output_rasters']['geen_schade_tif']
    meta=damage_meta
    nodata=0
    datatype=gdal.GDT_Byte)"""
    if not os.path.exists(output_file):
        # make one geometry from shapefile.
        filter_gdf_base = gpd.read_file(input_file)
        filter_union = filter_gdf_base.buffer(0.1).unary_union.buffer(-0.1)
        filter_gdf = gpd.GeoDataFrame(geometry=[filter_union], crs=filter_gdf_base.crs)

        filter_gdf["val"] = 1
        hrt.gdf_to_raster(
            gdf=filter_gdf,
            value_field="val",
            raster_out=output_file,
            nodata=nodata,
            metadata=meta,
            datatype=datatype,
        )
        print("{} created".format(output_file))
    else:
        print("{} already exists".format(output_file))


def apply_mask_to_raster(mask, raster_file, output_files):
    """Gegeven de input raster; maak een filtering op basis van het masker tussen plasvorming en wateroverlast
    output_files={}
    output_files['plas']='...tif'
    output_files['overlast']='...tif'"""

    def apply_mask_to_raster_array(
        mask_array, mask_type, raster_array_local, output_file, meta, nodata
    ):
        """Maskeer deel van een raster en sla deze op."""
        raster_array_local[~(mask_array == 1)] = nodata  # Maskeer
        hrt.save_raster_array_to_tiff(
            output_file=output_file,
            raster_array=raster_array_local,
            nodata=nodata,
            metadata=meta,
        )

    # laad ongefilterd raster
    raster_array, nodata, meta = hrt.load_gdal_raster(raster_file)

    # Creeer masker voor plas en overlast
    for mask_type in ["plas", "overlast"]:
        if not os.path.exists(output_files[mask_type]):
            apply_mask_to_raster_array(
                mask_array=mask[mask_type],
                mask_type=mask_type,
                raster_array_local=raster_array.copy(),
                output_file=output_files[mask_type],
                meta=meta,
                nodata=nodata,
            )
        else:
            print("{} bestond al".format(output_files[mask_type]))


def remove_mask_from_raster(mask, input_raster, output_file):
    """Bij niet realistische schades kan je een shapefile aanmaken die uit de schaderasters wordt geknipt. Deze shape is:
    ./01. DAMO HDB en Datachecker\peilgebieden\geen_schade.shp"""
    raster_array, nodata, meta = hrt.load_gdal_raster(input_raster)  # load raster

    # Remove polygon from raster
    raster_array[mask] = nodata

    # Save to new tif
    hrt.save_raster_array_to_tiff(
        output_file=output_file, raster_array=raster_array, nodata=nodata, metadata=meta
    )
