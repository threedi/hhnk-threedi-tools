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
RAIN_INTENSITY["blok"]["T10"] = "88 / 48"  # mm/hour
RAIN_INTENSITY["blok"]["T100"] = "130.8 / 48"  # mm/hour
RAIN_INTENSITY["blok"]["T1000"] = "184.9 / 48"  # mm/hour
RAIN_INTENSITY["piek"] = {}
RAIN_INTENSITY["piek"]["T10"] = "45.4 / 2"  # mm/hour
RAIN_INTENSITY["piek"]["T100"] = "84.5 / 2"  # mm/hour
RAIN_INTENSITY["piek"]["T1000"] = "157.5.4 / 2"  # mm/hour

RAIN_TYPES = ["piek", "blok"]
GROUNDWATER = ["glg", "ggg", "ghg"]
RAIN_SCENARIOS = ["T10", "T100", "T1000"]

# Dict with uuids for the organisation. Organisation names are equal to the
API_SETTINGS = {}
API_SETTINGS["org_uuid"] = {}
API_SETTINGS["org_uuid"]["BWN HHNK"] = "48dac75bef8a42ebbb52e8f89bbdb9f2"
API_SETTINGS["org_uuid"]["Hoogheemraadschap Hollands Noorderkwartier"] = "474afd212f2e4b4f82615142f1d67acb"
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
