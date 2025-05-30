# %%
import os

import geopandas as gpd
import pandas as pd

# %%
damo_gemaal = r"E:\personen\jacosta\HYDAMO\gemaal_v2.gpkg"
cso_gemaal = r"E:\personen\jacosta\HYDAMO\pomp_cso.csv"
gemaal_join_cso_path = r"E:\personen\jacosta\HYDAMO\gemaal_join_cso_v2.gpkg"

df_cso = pd.read_csv(cso_gemaal, sep=",")

df_cso.columns = [col.lower() for col in df_cso.columns]

gdf_gemaal = gpd.read_file(damo_gemaal, engine="pyogrio")

gdf_gemaal.set_crs("EPSG:28992")

# gemaal_damo_codes = gdf_gemaal['code'].to_list()
# cso_codes = df_cso['CODEBEHEEROBJECT'].to_list()
# code_same = []
# for cso_code in cso_codes:
#     if cso_code in gemaal_damo_codes:
#         code_same.append(cso_code)

gemaal_join_cso = gdf_gemaal.merge(
    df_cso, how="inner", left_on="code", right_on="codebeheerobject", suffixes=("_DAMO", "_CSO")
)


gemaal_join_cso.to_file(r"E:\personen\jacosta\HYDAMO\gemaal_join_cso.gpkg", driver="GPKG", engine="pyogrio")

# gemaal_join_cso.to_file(gemaal_join_cso_path, driver='GPKG')
# %%

gdf_gemaal = gpd.read_file(r"E:\personen\jacosta\HYDAMO\gemaal_join_cso.gpkg", engine="pyogrio")
damo_schema = gpd.read_file(r"E:\personen\jacosta\HYDAMO\DAMO.gpkg", engine="pyogrio")

gdf_gemaal_damo = gdf_gemaal.copy()
gdf_gemaal_damo["OBJECTID"] = gdf_gemaal["objectid_CSO"]
gdf_gemaal_damo["code"] = gdf_gemaal["code_CSO"]
gdf_gemaal_damo["maximalecapaciteit"] = gdf_gemaal["maximalecapaciteit_CSO"]
gdf_gemaal_damo["geamaalID"] = gdf_gemaal["codebeheerobject"]
gdf_gemaal_damo["globalid"] = gdf_gemaal["muid"]

gdf_gemaal_damo.drop(
    [
        "objectid_DAMO",
        "se_anno_cad_data",
        "code_DAMO",
        "naam_DAMO",
        "statusleggerwatersysteem",
        "statusleggerwaterveiligheid",
        "statusobject",
        "indicatiewaterkerend",
        "richting",
        "typewaterkerendeconstructie",
        "drempelpeil",
        "functiegemaal",
        "kerendehoogte",
        "signaleringspeil",
        "maximalecapaciteit_DAMO",
        "sluitpeil",
        "openkeerpeil",
        "categorie",
        "openingspeil",
        "filteruitstroming",
        "ontwerpbuitenwaterstand",
        "breedteopening",
        "afvoercoefficient",
        "aantalopeningen",
        "namespace",
        "detailniveaugeometrie",
        "lvpublicatiedatum",
        "ws_categorie",
        "ws_op_afstand_beheerd",
        "ws_bron",
        "ws_inwinningswijze",
        "ws_inwinningsdatum",
        "ws_beheercode",
        "ws_inwinningswijze_admin",
        "ws_inwinningsdatum_admin",
        "ws_volledigheid_dataset",
        "ws_kwaliteitsscore",
        "ws_inlaatfunctie",
        "created_user",
        "created_date",
        "last_edited_user",
        "last_edited_date",
        "ws_typefundering",
        "ws_ohplicht_kwk",
        "ws_inforcode",
        "afslagpeil",
        "globalid",
        "waterkeringid",
        "hyperlink",
        "opmerking",
        "functiegebiedgemaal",
        "soortregelbaarheid",
        "datuminwinning",
        "inwinnendeinstantie",
        "metendeinstantie",
        "inwinningsmethode",
        "dimensie",
        "nauwkeurigheidxy",
        "nauwkeurigheidz",
        "objectid_CSO",
        "code_CSO",
        "naam_CSO",
        "codebeheerobject",
        "bouwjaar",
        "fabrikant",
        "type",
        "maximalecapaciteit_CSO",
        "vermogenkw",
        "i_nominaal",
        "toerental",
        "opvoerhoogte",
        "waaier",
        "gewicht",
        "opstelling",
        "diameter",
        "obsgemuteerddoor",
        "obsmutatiedatum",
        "muid",
        "cso_laaddatum",
    ],
    axis=1,
)

gdf_gemaal_damo.to_file(r"E:\personen\jacosta\HYDAMO\DAMO.gpkg", driver="GPKG", layer="pomp")


# %%
