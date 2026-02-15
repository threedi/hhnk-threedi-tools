# %%
"""
Schematisation settings were originally stored in 02_schematisation/model_settings.xlsx.
This script migrates those settings to a new JSON format, which is used since 2026.03
"""

import csv
import json
from pathlib import Path

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.folders import Folders
from tests.config import FOLDER_TEST

logger = hrt.logging.get_logger(__name__)
INPUT_CSV = Path(r"hhnk_threedi_tools/core/schema_scenario_builder/resources/schematisation_settings.csv")
OUTPUT_JSON = Path(r"hhnk_threedi_tools/core/schema_scenario_builder/resources/scenarios1.json")

# col renames for model_settings.xlsx to schemation_scenarios.json
MODEL_SETTINGS_RENAMES = {
    "schematisation_name": "schematisation_name",
    "name": "simulation_template_settings.name",
    "use_0d_inflow": "simulation_template_settings.use_0d_inflow",
    "control_group_id": "simulation_template_settings.use_structure_control",
    "initial_waterlevel_file": "initial_conditions.initial_waterlevel_file",
    "water_level_ini_type": "initial_conditions.initial_water_level_aggregation",
    "output_time_step": "time_step_settings.output_time_step",
    "kmax": "model_settings.nr_grid_levels",
    "dem_file": "model_settings.dem_file",
    "frict_coef_file": "model_settings.friction_coefficient_file",
    "use_2d_rain": "model_settings.use_2d_rain",
    "use_2d_flow": "model_settings.use_2d_flow",
    "simple_infiltration_settings_id": "model_settings.use_simple_infiltration",
    "infiltration_rate": "simple_infiltration.infiltration_rate",
    "infiltration_rate_file": "simple_infiltration.infiltration_rate_file",
    "infiltration_surface_option": "simple_infiltration.infiltration_surface_option",
    "max_infiltration_capacity_file": "simple_infiltration.max_infiltration_volume_file",
    "\ufeffid": "REMOVED",
    "display_name": "REMOVED",
}

type_conversions = {"control_group_id": bool, "simple_infiltration_settings_id": bool}


def convert_value(key: str, value: str):
    """Convert CSV string values to proper Python types."""

    logger.info(f"Converting key: {key}, value: {value}")
    if value == "":
        return None

    if "_file" in key:
        value = value.replace("rasters/", "")

    if key == "name":
        value = value.replace("_test", "_check")

    # Int
    if value.isdigit():
        return int(value)

    return value


def set_nested_dict(d, layer_name, value):
    """Set a value in a nested dictionary using dot notation path."""
    keys = layer_name.split(".")
    current = d
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


# %%
def migration_xlsx_to_json(folder: Folders):
    input_xlsx = folder.model.settings.path
    output_json = folder.model.joinpath("schematisation_scenarios.json")
    scenarios = {}

    with input_xlsx.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            clean_row = {key: convert_value(key, value.strip()) for key, value in row.items()}

            # Apply column renames
            renamed_row = {}
            for old_key in MODEL_SETTINGS_RENAMES.keys():
                new_key = MODEL_SETTINGS_RENAMES[old_key]
                if new_key != "REMOVED":
                    set_nested_dict(renamed_row, new_key, clean_row.get(old_key))

            scenario_name = renamed_row["simulation_template_settings"]["name"]
            scenario_name = scenario_name.replace("_test", "_check")
            scenarios[scenario_name] = renamed_row

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(scenarios, f, indent=4)

    print(f"Written {output_json}")


if __name__ == "__main__":
    from tests.config import FOLDER_NEW, FOLDER_TEST

    folder = FOLDER_TEST
    migration_xlsx_to_json(folder)

# %%
