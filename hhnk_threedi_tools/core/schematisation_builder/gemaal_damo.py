# %%
import os
import geopandas as gpd
import pandas as pd

# %%  Paths
base_path = r"E:\personen\jacosta\HYDAMO"
damo_gemaal_path = os.path.join(base_path, "gemaal_v2.gpkg")
cso_gemaal_path = os.path.join(base_path, "pomp_cso.csv")
gemaal_join_output = os.path.join(base_path, "gemaal_join_cso.gpkg")
final_output_path = os.path.join(base_path, "DAMO.gpkg")

def export_gemaal (damo_gemaal_path, ):
    # %% load data
    df_cso = pd.read_csv(cso_gemaal_path, cso_gemaal_path, gemaal_join_output, final_output_path)
    df_cso.columns = df_cso.columns.str.lower()

    gdf_gemaal = gpd.read_file(damo_gemaal_path, engine="pyogrio")
    gdf_gemaal = gdf_gemaal.set_crs("EPSG:28992")

    # %% merge datbases
    gemaal_join_cso = gdf_gemaal.merge(
        df_cso,
        how="inner",
        left_on="code",
        right_on="codebeheerobject",
        suffixes=("_DAMO", "_CSO")
    )

    # save merge
    gemaal_join_cso.to_file(gemaal_join_output, driver="GPKG", engine="pyogrio")

    # %% read data to select columns 
    gdf_gemaal = gpd.read_file(gemaal_join_output, engine="pyogrio")

    # Copy database 
    gdf_gemaal_damo = gdf_gemaal.copy()
    gdf_gemaal_damo["OBJECTID"] = gdf_gemaal["objectid_cso"]
    gdf_gemaal_damo["code"] = gdf_gemaal["code_cso"]
    gdf_gemaal_damo["maximalecapaciteit"] = gdf_gemaal["maximalecapaciteit_cso"]
    gdf_gemaal_damo["geamaalID"] = gdf_gemaal["codebeheerobject"]
    gdf_gemaal_damo["globalid"] = gdf_gemaal["muid"]

    # delete Columns 
    columns_to_drop = [col for col in gdf_gemaal.columns if col.lower() not in {
        "geometry", "objectid_cso", "code_cso", "maximalecapaciteit_cso", "codebeheerobject", "muid"
    }]
    gdf_gemaal_damo.drop(columns=columns_to_drop, axis=1, inplace=True)

    # Save data
    gdf_gemaal_damo.to_file(final_output_path, driver="GPKG", layer="pomp")

    # %%
