# Default settings for all rain events used in the water system analysis.

RAIN_SETTINGS = {}

# blok= 2 days of continuous rain
RAIN_SETTINGS["blok"] = {}
RAIN_SETTINGS["blok"]["days_dry_start"] = 0
RAIN_SETTINGS["blok"]["hours_dry_start"] = 0
RAIN_SETTINGS["blok"]["days_rain"] = 2
RAIN_SETTINGS["blok"]["hours_rain"] = 0
RAIN_SETTINGS["blok"]["days_dry_end"] = 0
RAIN_SETTINGS["blok"]["hours_dry_end"] = 0

# piek=2 hours of rain in a 2 day simulation
RAIN_SETTINGS["piek"] = {}
RAIN_SETTINGS["piek"]["days_dry_start"] = 0
RAIN_SETTINGS["piek"]["hours_dry_start"] = 0
RAIN_SETTINGS["piek"]["days_rain"] = 0
RAIN_SETTINGS["piek"]["hours_rain"] = 2
RAIN_SETTINGS["piek"]["days_dry_end"] = 1
RAIN_SETTINGS["piek"]["hours_dry_end"] = 22

# Rain intensity per scenario
RAIN_INTENSITY = {}
RAIN_INTENSITY["blok"] = {}
RAIN_INTENSITY["blok"]["T10"] = 71.2 / 48  # mm/hour
RAIN_INTENSITY["blok"]["T100"] = 100.9 / 48  # mm/hour
RAIN_INTENSITY["blok"]["T1000"] = 134.6 / 48  # mm/hour
RAIN_INTENSITY["piek"] = {}
RAIN_INTENSITY["piek"]["T10"] = 35.5 / 2  # mm/hour
RAIN_INTENSITY["piek"]["T100"] = 55.7 / 2  # mm/hour
RAIN_INTENSITY["piek"]["T1000"] = 80.5 / 2  # mm/hour

RAIN_TYPES = ["piek", "blok"]
GROUNDWATER = ["GLG", "GGG", "GHG"]
RAIN_SCENARIOS = ["T10", "T100", "T1000"]

# Dict with uuids for the organisation. Organisation names are equal to the
API_SETTINGS = {}
API_SETTINGS["org_uuid"] = {}
API_SETTINGS["org_uuid"]["BWN HHNK"] = "48dac75bef8a42ebbb52e8f89bbdb9f2"
API_SETTINGS["org_uuid"][
    "Hoogheemraadschap Hollands Noorderkwartier"
] = "474afd212f2e4b4f82615142f1d67acb"
API_SETTINGS["store_results"] = {
    "process_basic_results": True,
    "arrival_time": False,
    "damage_estimation": {
        "cost_type": 2,
        "flood_month": 9,
        "inundation_period": 48,
        "repair_time_infrastructure": 120,
        "repair_time_buildings": 240,
    },
}

# Emails for api call
USERNAME_EMAIL = {}
USERNAME_EMAIL["wietse.vangerwen"] = "w.vangerwen@hhnk.nl"
USERNAME_EMAIL["jeroen.hermans"] = "j.hermans@hhnk.nl"
USERNAME_EMAIL["matthijs.hekstra"] = "m.hekstra@hhnk.nl"
USERNAME_EMAIL["niel.dejong"] = "n.dejong@hhnk.nl"
USERNAME_EMAIL["w.vanesse"] = "w.vanesse@hhnk.nl"
USERNAME_EMAIL["a.grootkormelink"] = "a.grootkormelink@hhnk.nl"


# Externen
USERNAME_EMAIL["michael.pieterson"] = "m.pieterson@hhnk.nl"
USERNAME_EMAIL["emiel.verstegen"] = "E.Verstegen@hhnk.nl"
USERNAME_EMAIL["rebecca.vanweesep"] = "R.vanWesep@hhnk.nl"
USERNAME_EMAIL["h.hagedooren"] = "H.Hagedooren@hhnk.nl"
USERNAME_EMAIL["nils.vandervliet"] = "vliet@hkv.nl"
USERNAME_EMAIL["ralf.linneman"] = "Ralf.Linneman@hydrologic.com"
USERNAME_EMAIL["ruben.boelens"] = "Ruben.Boelens@hydrologic.com"
USERNAME_EMAIL["ivar.lokhorst"] = "ivar.lokhorst@nelen-schuurmans.nl"
USERNAME_EMAIL["j.keja"] = "j.keja@hhnk.nl"


MODEL_TYPES = ["0d1d_test", "1d2d_test", "1d2d_glg", "1d2d_ggg", "1d2d_ghg"]

# Batch calculation postprocessing settings
RAW_DOWNLOADS = ["piek_GHG_T1000", "blok_GHG_T1000"]  # Needed for mask/filter map
RUIMTEKAART_SCENARIOS = [
    "blok_GGG_T10",
    "blok_GGG_T100",
    "blok_GGG_T1000",
    "blok_GHG_T10",
    "blok_GHG_T100",
    "blok_GHG_T1000",
]
