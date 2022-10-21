# Default settings for all rain events used in the water system analysis.

RAIN_SETTINGS = {}

# blok= 2 days of continuous rain
RAIN_SETTINGS["blok"] = {}
RAIN_SETTINGS["blok"]["simulation_duration"] = "48*3600"
RAIN_SETTINGS["blok"]["rain_offset"] = "0"
RAIN_SETTINGS["blok"]["rain_duration"] = "48*3600"

# piek=2 hours of rain in a 2 day simulation
RAIN_SETTINGS["piek"] = {}
RAIN_SETTINGS["piek"]["simulation_duration"] = "48*3600"
RAIN_SETTINGS["piek"]["rain_offset"] = "0"
RAIN_SETTINGS["piek"]["rain_duration"] = "2*3600"

# Rain intensity per scenario
RAIN_INTENSITY = {}
RAIN_INTENSITY["blok"] = {}
RAIN_INTENSITY["blok"]["T10"] = "71.2 / 48"  # mm/hour
RAIN_INTENSITY["blok"]["T100"] = "100.9 / 48"  # mm/hour
RAIN_INTENSITY["blok"]["T1000"] = "134.6 / 48"  # mm/hour
RAIN_INTENSITY["piek"] = {}
RAIN_INTENSITY["piek"]["T10"] = "35.5 / 2"  # mm/hour
RAIN_INTENSITY["piek"]["T100"] = "55.7 / 2"  # mm/hour
RAIN_INTENSITY["piek"]["T1000"] = "80.5 / 2"  # mm/hour

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
}
API_SETTINGS["basic_processing"] = {
    "process_basic_results": True,
}
API_SETTINGS["damage_processing"] = {
    "basic_post_processing": True,
    "cost_type": "avg",  # 2,
    "flood_month": "sep",  # 9,
    "inundation_period": 48,
    "repair_time_infrastructure": 120,
    "repair_time_buildings": 240,
}
# TODO verwijderen als oude settings werken.
# damage_processing_data = {
#     "basic_post_processing": True,
#     "cost_type": "avg",
#     "flood_month": "sep",
#     "inundation_period": 1,
#     "repair_time_infrastructure": 6,
#     "repair_time_buildings": 24,
# }

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
