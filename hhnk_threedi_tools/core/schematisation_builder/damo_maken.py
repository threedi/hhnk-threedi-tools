import pandas as pd
import geopandas as gpd
import sqlite3


damo_data = gpd.read_file("damo.gpkg", layer="POMP")

damo_pomp = damo_data[['OBJECTID', 'code', 'maximalecapaciteit', 'geamaalID', 'muid']]

#rename column maxamalecapaciteit to maximaleCapaciteit
damo_pomp.rename(columns={'maximalecapaciteit': 'maximaleCapaciteit'}, inplace=True)
damo_pomp.rename(columns={'MUID': 'globalID'}, inplace=True)
damo_pomp.rename(columns={'geamaalID': 'gemaalID'}, inplace=True)

#add columns minimaleCapaciteit, typeAandrijving, typePompOpstelling, typePomp, typePompschakeling, pompcurve, pomprichting, globalID
damo_pomp['minimaleCapaciteit'] = None
damo_pomp['typeAandrijving'] = None 
damo_pomp['typePompOpstelling'] = None
damo_pomp['typePomp'] = None
damo_pomp['typePompschakeling'] = None
damo_pomp['pompcurve'] = None
damo_pomp['pomprichting'] = None


#add layer to geopackage
damo_pomp.to_file("damo.gpkg", layer="POMP", driver="GPKG", index=False)

with sqlite3.connect("DAMO.gpkg") as conn:
    damo_pomp.to_sql('POMP', conn, if_exists='replace', index=False)


with sqlite3.connect("DAMO.gpkg") as conn:
    conn.execute("DELETE FROM gpkg_contents WHERE table_name = 'POMP';")
    conn.commit()

import pyogrio

pyogrio.write_dataframe(
    damo_pomp,
    "DAMO.gpkg",
    layer="POMP",
    driver="GPKG"
)


conn = sqlite3.connect("DAMO.gpkg")
damo_pomp.to_sql("POMP", conn, if_exists="replace", index=False)

conn.close()


conn = sqlite3.connect("damo.gpkg")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS pomp;")
conn.commit()
conn.close()


from DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter

converter = DAMO_to_HyDAMO_Converter(
    damo_file_path='DAMO.gpkg', hydamo_file_path='HyDAMO.gpkg', layers=["GEMAAL" , "POMP","HYDROOBJECT", "DUIKERSIFONHEVEL"], overwrite=True
)
converter.run()