import geopandas as gpd
from osgeo import gdal
import hhnk_research_tools as hrt
import os

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