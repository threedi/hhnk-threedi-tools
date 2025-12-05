from typing import List

# SQLite layers expected in 3Di schema
SQLITE_LAYERS: List[str] = [
    "connection_node",
    "obstacle",
    "channel",
    "culvert",
    "cross_section_location",
    "orifice",
    "pump",
    "weir",
]

# Name of the table containing cross-section definitions
SQLITE_LAYER_CROSS_SECTION_DEFINITION: str = "v2_cross_section_definition"

# DAMO layer names (source dataset)
DAMO_LAYERS: List[str] = [
    "AfvoergebiedAanvoergebied",
    # "AquaductLijn",  # Niet in beide datasets
    # "Bergingsgebied",  # Niet in beide datasets
    "Brug",
    # "Doorstroomopening", not a gdf
    "DuikerSifonHevel",
    "Gemaal",
    # "GW_PBP", no gdf
    # "GW_PRO",
    # "GW_PRW", no gdf
    "HydroObject",
    #'IWS_GEO_BESCHR_PROFIELPUNTEN',
    "PeilafwijkingGebied",
    "PeilgebiedPraktijk",
    "REF_BEHEERGEBIEDSGRENS_HHNK",
    "Sluis",
    "Stuw",
    "VasteDam",
    "Vispassage",
    "Waterdeel",
    # "Waterdeel", it does not contains code should be done in a different way
]

# HDB layer names (HDB exports)
HDB_LAYERS: List[str] = [
    "gemalen_op_peilgrens",
    "stuwen_op_peilgrens",
    "hydro_deelgebieden",
    "Levee_overstromingsmodel",
    "polderclusters",
    # "randvoorwaarden",  # geen geom
    "Sturing_3Di",
    "duikers_op_peilgrens",
]

THREEDI_STRUCTURE_LAYERS: List[str] = [
    "culvert",
    "pump",
    "weir",
    "orifice",
    "channel",
]

DAMO_HDB_STRUCTURE_LAYERS: List[str] = [
    "gemalen_op_peilgrens",
    "stuwen_op_peilgrens",
    "brug",
    "gemaal",
    "stuw",
    "vastedam",
    "duikersifonhevel",
    "hydroobject",
]

STRUCTURE_CODES: List[str] = [
    "KDU",
    "KST",
    "KGM",
    "KVD",
    "OAF",
    "KBR",
    # "KSY",
    # @TODO: introduce 'other' category, so we don't miss any that don't have a nice code
]

# Layers that require geometrical (polygon) comparison logic
GEOMETRICAL_COMPARISON_LAYERS: List[str] = ["PeilafwijkingGebied", "PeilgebiedPraktijk", "Waterdeel"]

# Default numeric threshold for numeric comparisons
COMPARISON_GENERAL_THRESHOLD: float = 0.00001
