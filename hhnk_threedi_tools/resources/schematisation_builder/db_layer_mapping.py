"""This script contains the mapping of database layers to their respective source tables and
schemas. It is used to define which data to export from which database for the schematisation
builder. It includes information about the source, schema, columns, and any required sub-tables
or links to other tables. This mapping is the default set of data used in the `db_exporter` function
that exports everything needed for model generation (available in the databases of HHNK).
"""

DB_LAYER_MAPPING = {
    # All non geometry tables should listed as sub tables
    # CS_OBJECTEN
    "GEMAAL": {
        "source": "csoprd_lezen",
        "schema": "CS_OBJECTEN",
        "columns": None,
        "required_sub_table": "POMP",
        "id_link_column": "CODE",
        "sub_id_column": "CODEBEHEEROBJECT",
    },
    "SLUIS": {
        "source": "csoprd_lezen",
        "schema": "CS_OBJECTEN",
        "columns": None,
        "required_sub_table": None,
    },
    "DUIKERSIFONHEVEL": {  # TODO afsluitmiddel nodig?
        "source": "csoprd_lezen",
        "schema": "CS_OBJECTEN",
        "columns": None,
        "required_sub_table": None,
    },
    "STUW": {  # TODO afsluitmiddel nodig?
        "source": "csoprd_lezen",
        "schema": "CS_OBJECTEN",
        "columns": None,
        "required_sub_table": None,
    },
    "VISPASSAGE": {  # TODO afsluitmiddel nodig?
        "source": "csoprd_lezen",
        "schema": "CS_OBJECTEN",
        "columns": None,
        "required_sub_table": None,
    },
    "COMBINATIEPEILGEBIED": {
        "source": "csoprd_lezen",
        "schema": "CS_OBJECTEN",
        "columns": None,
    },
    # BGT
    "HHNK_MV_WTD": {
        "layername": "Waterdeel",
        "source": "bgt_lezen",
        "schema": "BGT",
        "columns": None,
    },
    "HHNK_MV_OWT": {
        "layername": "Ondersteunend waterdeel",
        "source": "bgt_lezen",
        "schema": "BGT",
        "columns": None,
    },
    # DAMO_W
    "BRUG": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
        "required_sub_table": "DOORSTROOMOPENING",  # TODO check
        "id_link_column": "GLOBALID",
        "sub_id_column": "BRUGID",
    },
    "PUT": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "GEMAAL_DAMO": {
        "table_name": "GEMAAL",
        "layername": "GEMAAL_DAMO",
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "AQUADUCTLIJN": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
        "required_sub_table": None,
    },
    "AQUADUCT": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "BODEMVAL": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "VASTEDAM": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "BERGINGSGEBIED": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "HYDROOBJECT": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "IWS_GEO_BESCHR_PROFIELPUNTEN": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "geomcolumn": "GEOMETRIE",
        "columns": ["PBP_PBP_ID"],
        "required_sub_table": None,
    },
    "GW_PRO": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "geomcolumn": "GEOMETRIE",
        "columns": [
            "PRO_ID",
            "PROIDENT",
            "OVK_OVK_ID",
            "OPRDATOP",
            "OSMOMSCH",
        ],
        "required_sub_table": "GW_PRW",
        "id_link_column": "PRO_ID",
        "sub_id_column": "PRO_PRO_ID",
        "sub_columns": [
            "PRO_PRO_ID",
            "PRW_ID",
            "OSMOMSCH",
        ],
        "required_sub2_table": "GW_PBP",
        "sub_id_link_column": "PRW_ID",
        "sub2_id_column": "PRW_PRW_ID",
        "sub2_columns": [
            "PBP_ID",
            "PRW_PRW_ID",
            "PBPIDENT",
            "PBPSOORT",
            "IWS_VOLGNR",
            "IWS_HOOGTE",
            "IWS_AFSTAND",
        ],
    },
    "PEILGEBIEDPRAKTIJK": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    "WATERKERING": {
        "source": "aquaprd_lezen",
        "schema": "DAMO_W",
        "columns": None,
    },
    # ingevulde kolommen zijn CODE, NAAM, STATUSOBJECT, CATEGORIE, TYPEWATERKERING, SOORTREFERENTIELIJN, WS_DWK_SOORT, WS_BEHEERCODE
}
