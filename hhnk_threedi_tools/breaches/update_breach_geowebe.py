# %%
from pathlib import Path

import geopandas as gpd

gdb_path = Path(
    r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC25007 Ontsluiten Overstromingsbeelden\geoweb_gdb\bressen.gpkg"
)

bressen_schade_gdf = gpd.read_file(
    r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC25007 Ontsluiten Overstromingsbeelden\geoweb_gdb\bressen_schade.gpkg"
)
bressen = gpd.read_file(gdb_path, engine="pyogrio")


bressen_schade_gdf["Scenario_Identificatie"] = bressen_schade_gdf["Scenario_I"]
bressen_copy = bressen.copy()
# %%
bressen_copy = bressen_copy.merge(
    bressen_schade_gdf[["Scenario_Identificatie", "Totale_Kos"]], on="Scenario_Identificatie", how="left"
)

bressen_copy["Total_Schade_Kost"] = bressen_copy["Totale_Kos"]

# CHANGE COLUMN 'Varianttype' FOR Category_waterkereing
bressen_copy["CATEGORIE_WATERKERING"] = bressen_copy["Varianttype"]
# if Variant_type == Primaire Kereingen, Category_waterkering is 1 else 2
bressen_copy["CATEGORIE_WATERKERING"] = bressen_copy["CATEGORIE_WATERKERING"].apply(
    lambda x: 1 if x == "Primaire Keringen" else 2
)


bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "CATEGORIE_WATERKERING"] = 1
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "CATEGORIE_WATERKERING"] = 2

# CHANGE COLUMN 'Varianttype' FOR Category_waterkereing
bressen_copy["DIJKGEBIED_NAAM"] = bressen_copy["Gebiedsnaam"]

bressen_copy["Naam_buitenwater_text"] = bressen_copy["Naam_buitenwater_Code"]


bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Amstelmeer", "Naam_buitenwater_Code"] = 1
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Amstelmeerkanaal", "Naam_buitenwater_Code"] = 2
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Boezemwater", "Naam_buitenwater_Code"] = 3
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "IJsselmeer", "Naam_buitenwater_Code"] = 4
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Markermeer", "Naam_buitenwater_Code"] = 5
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Noordzee", "Naam_buitenwater_Code"] = 6
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Noordzeekanaal", "Naam_buitenwater_Code"] = 7
bressen_copy.loc[bressen_copy["Naam_buitenwater_text"] == "Waddenzee", "Naam_buitenwater_Code"] = 8


bressen_copy["SIMULATIEDUUR"] = bressen_copy["Simulatietijd"]
bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "SIMULATIEDUUR"] = 10
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "SIMULATIEDUUR"] = 5
bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "CATEGORIE_WATERKERING"] = 1
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "CATEGORIE_WATERKERING"] = 2


bressen_copy.loc[bressen_copy["Overschrijdingsfrequentie"] == "10", "Overschrijdingsfrequentie_code"] = 1
bressen_copy.loc[bressen_copy["Overschrijdingsfrequentie"] == "100", "Overschrijdingsfrequentie_code"] = 2
bressen_copy.loc[bressen_copy["Overschrijdingsfrequentie"] == "1000", "Overschrijdingsfrequentie_code"] = 4
bressen_copy.loc[bressen_copy["Overschrijdingsfrequentie"] == "3000", "Overschrijdingsfrequentie_code"] = 5
bressen_copy.loc[bressen_copy["Overschrijdingsfrequentie"] == "10000", "Overschrijdingsfrequentie_code"] = 6
bressen_copy.loc[bressen_copy["Overschrijdingsfrequentie"] == "100000", "Overschrijdingsfrequentie_code"] = 8

# rename
bressen_copy.rename(columns={"Overschrijdingsfrequentie": "Overschrijdingsfrequentie_value"}, inplace=True)
bressen_copy.rename(columns={"Overschrijdingsfrequentie_code": "Overschrijdingsfrequentie"}, inplace=True)
bressen_copy.rename(columns={"Naam_buitenwater_text": "Naam_buitenwater_value"}, inplace=True)


# %%
bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "DOORBRAAK_SCENARIO"] = 1
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "DOORBRAAK_SCENARIO"] = 2

bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "DOORBRAAK_AFSLUITBAAR"] = "j"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "DOORBRAAK_AFSLUITBAAR"] = "n"

bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "COMPARTIMENTERING"] = "n"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "COMPARTIMENTERING"] = "n"

bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "COMPARTIMENTERING"] = "n"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "COMPARTIMENTERING"] = "n"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "COMPARTIMENTERING"] = "n"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "COMPARTIMENTERING"] = "n"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "COMPARTIMENTERING"] = "n"
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "COMPARTIMENTERING"] = "n"


bressen_copy.loc[bressen_copy["Varianttype"] == "Primaire Keringen", "RASTER_RESOLUTIE_M"] = float(0.5)
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "RASTER_RESOLUTIE_M"] = float(0.5)
bressen_copy.loc[bressen_copy["Scenario_Identificatie"].str.contains("IPO_WL1D"), "RASTER_RESOLUTIE_M"] = 2
bressen_copy.loc[bressen_copy["Scenario_Identificatie"].str.contains("IPO_NZK"), "RASTER_RESOLUTIE_M"] = 1
bressen_copy.loc[bressen_copy["Scenario_Identificatie"].str.contains("IPO_SKB_TP"), "RASTER_RESOLUTIE_M"] = 1

bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "OPMERKINGEN"] = (
    "Schade berekening met SSM (HHNK). Bres dicht na 48 uur."
)
bressen_copy.loc[bressen_copy["Varianttype"] != "Primaire Keringen", "OPMERKINGEN"] = (
    "Schade berekening nog niet berend"
)
# %%
cols = [
    "OBJECTID",
    "Scenario_Identificatie",
    "Scenarionaam",
    "Naam_waterkering",
    "CATEGORIE_WATERKERING",
    "DIJKGEBIED_NAAM",
    "Overschrijdingsfrequentie",
    "Naam_buitenwater_Code",
    "png_path",
    "png_schade_path",
    "Totale_Kost_Schade",
    "SIMULATIEDUUR",
    "DOORBRAAK_SCENARIO",
    "DOORBRAAK_AFSLUITBAAR",
    "COMPARTIMENTERING",
    "RASTER_RESOLUTIE_M",
    "OPMERKINGEN",
    "Varianttype",
    "Gebiedsnaam",
    "Overschrijdingsfrequentie_value",
    "Gebidsnaam_Code",
    "Opmerking",
    "Simulatietijd",
    "Totale_Kos",
    "Total_Schade_Kost",
    "Naam_buitenwater_value",
    "geometry",
]
bressen_copy.to_file(
    r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC25007 Ontsluiten Overstromingsbeelden\geoweb_gdb\bressen_copy.gpkg",
    driver="GPKG",
)
print("done")
# %%
