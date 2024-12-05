"""DAMO exporter based on model extent"""
import geopandas as gpd
from shapely.geometry import box
import pytest
from local_settings import DATABASES
from hhnk_research_tools.sql_functions import (
    database_to_gdf,
    sql_builder_select_by_location,
)

def DAMO_exporter(model_extent, table_name="HYDROOBJECT", EPSG_CODE = "28992"):
    # if instead of a model_extent, polder_name is given as input.
    # make it possible to select geometry of polder from polderclusters.gpkg
    db_dicts = {
        "aquaprd": DATABASES.get("aquaprd_lezen", None),
        "bgt": DATABASES.get("bgt_lezen", None),
    }
    
    schema = "DAMO_W"
    
    # make bbox --> to simply string request to DAMO db
    min_x, min_y, max_x, max_y = model_extent.bounds
    bbox_model = box(min_x, min_y, max_x, max_y)

    # Build sql to select by input polygon
    sql = sql_builder_select_by_location(
        schema=schema, table_name=table_name, epsg_code=EPSG_CODE, polygon_wkt=bbox_model, simplify=True
    )
    db_dict = db_dicts["aquaprd"]
    columns = None
        
    bbox_gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)

    # clip gdf to model_extent again
    gdf_model = gpd.clip(bbox_gdf, model_extent)

    return gdf_model

#Test
POLDERS_PATH = r'E:\01.basisgegevens\Polders\polderclusters.gpkg'
POLDERS =  gpd.read_file(POLDERS_PATH, engine="pyogrio")
model_extent = POLDERS['geometry'][15] 
output_DAMO = DAMO_exporter(model_extent)
