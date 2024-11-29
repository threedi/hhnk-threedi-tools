# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 09:17:01 2024

@author: esther.vanderlaan
"""
# %%
import geopandas as gpd
import pytest
from local_settings import DATABASES
# %%
from hhnk_research_tools.sql_functions import (
    database_to_gdf,
    sql_builder_select_by_location,
)

# Temporary: hard coded model extent selection
POLDERS =  gpd.read_file(r'E:\01.basisgegevens\Polders\polderclusters.gpkg', engine="pyogrio")
model_extent = POLDERS['geometry'][54] # this polder, because others are too large
#polygon_wkt = POLDERS['geometry'][54]

EPSG_CODE = "28992"


def DAMO_exporter(model_extent, table_name="HYDROOBJECT"):

    db_dicts = {
        "aquaprd": DATABASES.get("aquaprd_lezen", None),
        "bgt": DATABASES.get("bgt_lezen", None),
    }
    
    schema = "DAMO_W"
    
    # Load area for selection
    model_extent_gdf = gpd.read_file(model_extent, engine="pyogrio")
    polygon_wkt = model_extent_gdf.iloc[0]["geometry"]
    
    # Build sql to select by input polygon
    sql = sql_builder_select_by_location(
        schema=schema, table_name=table_name, epsg_code=EPSG_CODE, polygon_wkt=polygon_wkt, simplify=True
    )
    db_dict = db_dicts["aquaprd"]
    columns = None
    
    
    gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)

    return gdf
# %%

# afhankelijk van hoe dit gebruikt gaat worden, __main__ toevoegen om functie direct met dit script te runnen
