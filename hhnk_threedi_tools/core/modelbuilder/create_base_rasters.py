# %%
""" Bijwerken van E:\01.basisgegevens\rasters\ 

Waterdelen staan in de BGT -> BGT.HHNK_MV_WTD
Ondersteunede waterdelen staan in de BGT -> BGT.HHNK_MV_OWT
"D:\oracle\product\19.0.0\client_64\network\admin\tnsnames.ora"
"""

# %% Watervlakken rasterizen
import hhnk_research_tools as hrt
from osgeo import gdal

folder = hrt.Folder(r"E:\01.basisgegevens\rasters\watervlakken")
DATE = 20240824

folder.add_file("wtd_gpkg", f"bgt_waterdelen_{DATE}.gpkg")
folder.add_file("ond_wtd_gpkg", f"bgt_ondersteunende_waterdelen_{DATE}.gpkg")
folder.add_file("wtd_tif", f"bgt_waterdelen_{DATE}.tif")
folder.add_file("ond_wtd_tif", f"bgt_ondersteunende_waterdelen_{DATE}.tif")

# Waterdeel
if not folder.wtd_tif.exists():
    watervlak_gdf = folder.wtd_gpkg.load()
    watervlak_gdf["value"] = 1

    metadata = hrt.RasterMetadataV2.from_gdf(watervlak_gdf, res=0.5)

    hrt.gdf_to_raster(
        watervlak_gdf,
        value_field="value",
        raster_out=folder.wtd_tif,
        metadata=metadata,
        nodata=0,
        datatype=gdal.GDT_Byte,
        read_array=False,
    )

# Ondersteunend waterdeel
if not folder.ond_wtd_tif.exists():
    watervlak_gdf = folder.ond_wtd_gpkg.load()
    watervlak_gdf["value"] = 1

    metadata = hrt.RasterMetadataV2.from_gdf(watervlak_gdf, res=0.5)

    hrt.gdf_to_raster(
        watervlak_gdf,
        value_field="value",
        raster_out=folder.ond_wtd_tif,
        metadata=metadata,
        nodata=0,
        datatype=gdal.GDT_Byte,
        read_array=False,
    )

# %%

wd_gdf = folder.wtd_gpkg.load()
owd_gdf = folder.ond_wtd_gpkg.load()

import pandas as pd
df = pd.concat([wd_gdf,owd_gdf])

# %% ---------------------------------------------------------------------
# Hieronder is met oracledb een poging gedaan.
# Niet gelukt omdat arcpolygonen naar wkt niet goed gaat.
# De route van FME export gaat wel goed.
if False:
    import geopandas as gpd
    import hhnk_research_tools as hrt
    import pandas as pd
    from osgeo import gdal

    DATABASES = {
        # Deze staan in SPOC
    }

    # Translate dict so we can connect with oracledb
    ORACLEDB_KEYS_TRANSLATE = {
        "NAME": "service_name",
        "USER": "user",
        "PASSWORD": "password",
        "HOST": "host",
        "PORT": "port",
    }

    def create_oracledb_db(db):
        """Create a connection database dict that can be used with oracledb
        Example use:
        create_oracle_db(DATABASES["fews"])
        """
        return {ORACLEDB_KEYS_TRANSLATE[key]: db[key] for key in ORACLEDB_KEYS_TRANSLATE.keys() if key in db}

    ORACLEDB = {}
    for key, item in DATABASES.items():
        ORACLEDB[key] = create_oracledb_db(item)

    db_dicts = {
        "aquaprd": ORACLEDB.get("aquaprd_lezen", None),
        "bgt": ORACLEDB.get("bgt_lezen", None),
    }

    sql = """SELECT * FROM BGT.HHNK_MV_WTD"""
    db_dict = db_dicts["bgt"]

    import json

    # %%
    import re

    import geojson
    import oracledb
    from shapely import wkb, wkt

    columns = None
    crs = "EPSG:28992"

    con = None
    cur = None
    sql = """SELECT GUID, SDO_UTIL.TO_WKTGEOMETRY(GEOMETRIE) FROM BGT.HHNK_MV_WTD WHERE SDO_UTIL.VALIDATE_WKTGEOMETRY(SDO_UTIL.TO_WKTGEOMETRY(GEOMETRIE)) = 'TRUE' FETCH FIRST 10 ROWS ONLY"""
    sql = """SELECT * FROM BGT.HHNK_MV_WTD FETCH FIRST 10 ROWS ONLY"""
    sql = """SELECT * FROM BGT.HHNK_MV_WTD"""
    with oracledb.connect(**db_dict) as con:
        cur = oracledb.Cursor(con)

        cur.execute(sql)

        # Get column names from external table when names are not provided
        if columns is None:
            # When selecting all columns, retrieve the geometry as text.
            # For this we need to recreate the sql
            if "SELECT *" in sql:
                col_names = [i[0] for i in cur.description]

                col_select = ", ".join(col_names)
                if "SHAPE" in col_names:
                    pattern1 = "SDO_UTIL.TO_WKTGEOMETRY"
                    col_select = col_select.replace("SHAPE", f"{pattern1}(SHAPE)")
                elif "GEOMETRIE" in col_names:
                    # pattern1 = "SDO_UTIL.TO_WKTGEOMETRY"
                    pattern1 = "SDO_UTIL.TO_JSON_VARCHAR"
                    col_select = col_select.replace("GEOMETRIE", f"{pattern1}(GEOMETRIE)")

                sql = sql.replace("SELECT *", f"SELECT {col_select}")
                cur.execute(sql)

            # Take column names from cursor
            columns = [i[0] for i in cur.description]

        df = pd.DataFrame(cur.fetchall(), columns=columns)

        pattern = rf"{pattern1}\([^)]*\)"
        cols = []
        for col in df.columns:
            if re.findall(pattern=pattern, string=col):
                cols.append("geometry")
            else:
                cols.append(col)
        df.columns = cols

        # if "geometry" in df.columns:
        #     df = df.set_geometry(gpd.GeoSeries(df["geometry"].apply(lambda x: wkt.loads(str(x)))), crs=crs)

        print(df)
    from shapely.geometry import Polygon

    # g = df['geometry'].iloc[0]

    # shape(geojson.loads(json.dumps(g)))

    def gett(x):
        x = json.loads(x)
        print(x["polygon"]["boundary"][0])
        if "line" in x["polygon"]["boundary"][0]:
            return Polygon(x["polygon"]["boundary"][0]["line"]["datapoints"])
        else:
            return False

    a = df["geometry"].apply(gett)
    sum(a == False)

    # %%
    gdf = hrt.database_to_gdf(db_dict=db_dicts["bgt"], sql=sql, columns=None)
