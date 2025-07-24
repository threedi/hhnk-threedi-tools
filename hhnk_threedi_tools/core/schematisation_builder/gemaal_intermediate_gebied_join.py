# %%
from pathlib import Path

import fiona
import geopandas as gpd
import matplotlib.pyplot as plt

damo_path = Path(r"E:\09.modellen_speeltuin\test_with_pomp_table_juan\01_source_data\DAMO.gpkg")
polder_polygon = Path(
    r"\\corp.hhnk.nl\data\Hydrologen_data\Data\09.modellen_speeltuin\test_with_pomp_table_juan\01_source_data\polder_polygon.shp"
)
output_path = Path(
    r"E:\09.modellen_speeltuin\test_with_pomp_table_juan\01_source_data\peilgebiedpraktijk_linestring.gpkg"
)


# %%
# make function function to retrieve de gemaal that are intersected in the WS_PEILAFWIJKING_VW and PEILGEBIEDPRAKTIJK_EVW
def gemaal_in_peilgebiden(damo_path, polder_polygon):
    polder_polygon_gdf = gpd.read_file(polder_polygon)

    # get the layer gemaal een pielgebiedpraktijk
    def get_layer_gdf(layer_name):
        return gpd.read_file(damo_path, layer=layer_name)

    gdf_gemaal = get_layer_gdf("GEMAAL")
    gdf_peilgebiedpraktijk = get_layer_gdf("peilgebiedpraktijk")

    # transform MULTIPOLYGON  to LINESTRING
    gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk.copy()
    gdf_peilgebiedpraktijk_linestring["geometry"] = gdf_peilgebiedpraktijk_linestring["geometry"].apply(
        lambda geom: geom.boundary if geom.type == "MultiPolygon" else geom
    )
    gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.explode(index_parts=True)
    gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.reset_index(drop=True)

    # clip linestrings to the polder_polygon
    gdf_peilgebiedpraktijk_linestring = gpd.clip(gdf_peilgebiedpraktijk_linestring, polder_polygon_gdf)

    # make spatial join between gdf_gemaal and gdf_peilgebiedpraktijk_linestring distance 10 cm
    gemaal_spatial_join = gpd.sjoin_nearest(
        gdf_gemaal,
        gdf_peilgebiedpraktijk_linestring,
        how="inner",
        max_distance=0.01,
        distance_col="distance_to_peilgebied",
    )

    print(gemaal_spatial_join.columns)
    # Join the column 'distance_validation_rule' from gemaal_spatial_join into gdf_gemaal based on the 'code' column
    gdf_gemaal = gdf_gemaal.merge(gemaal_spatial_join[["code", "distance_to_peilgebied"]], on="code", how="left")

    # Loop over gdf_gemaal and print rows where 'distance_validation_rule' is lower than 0.01
    for idx, row in gdf_gemaal.iterrows():
        if row["distance_to_peilgebied"] < 0.01:
            print(row)

    # save the layer in DAMO TOFIX """"SHOULD I SAVE IT IN DAMO OR HYDAMO"""
    gdf_gemaal.to_file(damo_path, layer="GEMAAL", driver="GPKG")
    # save the clipped linestring to a new file
    gdf_peilgebiedpraktijk_linestring.to_file(damo_path, layer="peil_boundary", driver="GPKG")

    # %%
    # plot the geometry of gdf_gemaal, gdf_peilgebiedpraktijk_linestring and a
    # ax = gdf_peilgebiedpraktijk_linestring.plot(
    #     color="blue", edgecolor="black", figsize=(10, 10), label="Peilgebiedpraktijk Linestring"
    # )

    # gdf_gemaal.plot(ax=ax, color="red", markersize=5, label="Gemaal")
    # plt.legend()
    # plt.show()


# %%
