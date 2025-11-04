SQLITE_LAYERS = [
    "connection_node",
    "obstacle",
    "channel",
    "culvert",
    "cross_section_location",
    "orifice",
    "pump",
    "weir",
]

SQLITE_LAYER_CROSS_SECTION_DEFINITION = "v2_cross_section_definition"

DAMO_LAYERS = [
    "AfvoergebiedAanvoergebied",
    # "AquaductLijn",  # Niet in beide datasets
    # "Bergingsgebied",  # Niet in beide datasets
    "Brug",
    # "Doorstroomopening", not a gdf
    "DuikerSifonHevel",
    "Gemaal",
    # "GW_PBP", no gdf
    "GW_PRO",
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
    # "Waterdeel", it does not contains code should be done in a different way
]

HDB_LAYERS = [
    "gemalen_op_peilgrens",
    "stuwen_op_peilgrens",
    "hydro_deelgebieden",
    "Levee_overstromingsmodel",
    "polderclusters",
    # "randvoorwaarden",  # geen geom
    "Sturing_3Di",
    "duikers_op_peilgrens",
]

THREEDI_STRUCTURE_LAYERS = [
    "culvert",
    "pump",
    "weir",
    "orifice",
    "channel",
]

DAMO_HDB_STRUCTURE_LAYERS = [
    "gemalen_op_peilgrens",
    "stuwen_op_peilgrens",
    "brug",
    "gemaal",
    "stuw",
    "vastedam",
    "duikersifonhevel",
    "hydroobject",
]

# DAMO_HDB_STRUCTURE_LAYERS = [
#     'duikersifonhevel',
#     'stuw',
# ]

STRUCTURE_CODES = [
    "KDU",
    "KST",
    "KGM",
    "KSY",
    "KVD",
    "KBR",
    "OAF",
    # @TODO: introduce 'other' category, so we don't miss any that don't have a nice code
]

GEOMETRICAL_COMPARISON_LAYERS = [
    "PeilafwijkingGebied",
    "PeilgebiedPraktijk",
]
# "Waterdeel"
COMPARISON_GENERAL_THRESHOLD = 0.00001
