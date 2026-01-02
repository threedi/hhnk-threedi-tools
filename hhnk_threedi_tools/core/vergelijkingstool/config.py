SQLITE_LAYERS = [
    "v2_connection_nodes",
    "v2_cross_section_definition",
    "v2_obstacle",
    "v2_channel",
    "v2_culvert",
    "v2_cross_section_location",
    "v2_orifice",
    "v2_pumpstation",
    "v2_weir",
]

SQLITE_LAYER_CROSS_SECTION_DEFINITION = "v2_cross_section_definition"

DAMO_LAYERS = [
    "AfvoergebiedAanvoergebied",
    "AquaductLijn",  # Niet in beide datasets
    "Bergingsgebied",  # Niet in beide datasets
    "Brug",
    "Doorstroomopening",
    "DuikerSifonHevel",
    "Gemaal",
    "GW_PBP",
    "GW_PRO",
    "GW_PRW",
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
]

HDB_LAYERS = [
    "gemalen_op_peilgrens",
    "stuwen_op_peilgrens",
    "hydro_deelgebieden",
    "Levee_overstromingsmodel",
    "polderclusters",
    "randvoorwaarden",  # geen geom
    "Sturing_3Di",
]

THREEDI_STRUCTURE_LAYERS = [
    "v2_culvert",
    "v2_pumpstation",
    "v2_weir",
    "v2_orifice",
]

DAMO_HDB_STRUCTURE_LAYERS = [
    "gemalen_op_peilgrens",
    "stuwen_op_peilgrens",
    "brug",
    "gemaal",
    "duikersifonhevel",
    "stuw",
]

STRUCTURE_CODES = [
    "KDU",
    "KST",
    "KGM",
    "KSY",
    "KBR",
    # @TODO: introduce 'other' category, so we don't miss any that don't have a nice code
]

GEOMETRICAL_COMPARISON_LAYERS = ["PeilafwijkingGebied", "PeilgebiedPraktijk", "Waterdeel"]

COMPARISON_GENERAL_THRESHOLD = 0.00001
