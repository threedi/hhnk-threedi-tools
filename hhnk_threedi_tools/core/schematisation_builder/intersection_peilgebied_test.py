#%%
import geopandas as gpd
# from hydamo_validation.datamodel import HyDAMO
from pathlib import Path
import pandas as pd

hydamo_path = Path(r"E:\09.modellen_speeltuin\test_jk1\01_source_data\HyDAMO.gpkg")
# schemas_path = Path(r"E:\github\jacostabarragan\HyDAMOValidatieModule\hydamo_validation\schemas\hydamo")
polder_polygon = hydamo_path.parent / "polder_polygon.shp"

#check if code_1 and code_2 are the same
def intersectie_zelfde_peilgebied(hydamo_path, polder_polygon):
    combinatiepeilgebied_gdf = gpd.read_file(hydamo_path, layer= 'combinatiepeilgebied')

    #make a copy of the gdf
    combinatiepeilgebied_copy = combinatiepeilgebied_gdf.copy()
    polder_polygon_gdf = gpd.read_file(polder_polygon)

    #clip peilgebieden op polder polygon
    combinatiepeilgebied_copy = gpd.clip(combinatiepeilgebied_copy, polder_polygon_gdf)

    #intersect combinatiepeilgebied with itself to find overlapping areas
    intersection = gpd.overlay(combinatiepeilgebied_copy, combinatiepeilgebied_copy, how='intersection')
    
    #export only intersection with area > 0
    intersection = intersection[intersection.geometry.area > 0]

    #add column to combinatiepeilgebied_gdf 'intersectie_codes' with code_1 and code_2 save it as list separated by commas

    combinatiepeilgebied_gdf['intersectie_codes'] = intersection['code_1'].astype(str) + ', ' + intersection['code_2'].astype(str)
    
    number_of_intersected = 

    #make a column number of intersecties
    combinatiepeilgebied_gdf['aantal_intersecties'] = combinatiepeilgebied_gdf['intersectie_codes'].apply(lambda x: len(x.split(', ')) if pd.notnull(x) else 0) 
    intersection.to_file(hydamo_path.parent / "intersection.shp")
    
    return 