import hhnk_research_tools as hrt

try:
    from local_settings_htt import DATABASES, DB_LAYERS_MAPPING
except ImportError as e:
    raise ImportError(
        "The 'local_settings_htt' module is missing. Get it from D:\github\evanderlaan\local_settings_htt.py and place it in \hhnk_threedi_tools\resources\schematisation_builder"
    ) from e

db_dicts = {
    "aquaprd": DATABASES.get("aquaprd_lezen", None),
    "bgt": DATABASES.get("bgt_lezen", None),
    "csoprd": DATABASES.get("csoprd_lezen", None),
}

sql_gemaal = """select * from CS_OBJECTEN.GEMAAL"""
gdf_gemaal, sql2_gemaal = hrt.database_to_gdf(
    db_dict=db_dicts["csoprd"], sql=sql_gemaal, columns=None, lower_cols=False
)

sql_pomp = """select * from CS_OBJECTEN.POMP"""
gdf_pomp, sql2_pomp = hrt.database_to_gdf(db_dict=db_dicts["csoprd"], sql=sql_pomp, columns=None, lower_cols=False)


print(gdf_gemaal.head())
print(gdf_pomp.head())
print(gdf_gemaal.columns)
print(gdf_pomp.columns)


gdf_gemaal.to_file("gemaal_cso.gpkg", driver="GPKG", layer="gemaal")
gdf_pomp.to_csv("pomp_cso.csv", index=False)
