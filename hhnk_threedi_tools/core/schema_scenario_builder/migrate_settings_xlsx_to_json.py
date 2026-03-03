# %%
"""
Schematisation settings were originally stored in 02_schematisation/model_settings.xlsx.
This script migrates those settings to a new JSON format, which is used since 2026.03
"""

import json
from typing import Optional

import hhnk_research_tools as hrt
import pandas as pd
import threedi_schema
from threedi_schema import ModelSchema, ThreediDatabase

from hhnk_threedi_tools.core.folders import Folders
from tests.config import FOLDER_TEST

logger = hrt.logging.get_logger(__name__)

# col renames for model_settings.xlsx to schemation_scenarios.json
MODEL_SETTINGS_RENAMES = {
    "schematisation_name": "schematisation_name",
    "name": "layers.simulation_template_settings.name",
    "use_0d_inflow": "layers.simulation_template_settings.use_0d_inflow",
    "control_group_id": "layers.simulation_template_settings.use_structure_control",
    "initial_waterlevel_file": "layers.initial_conditions.initial_water_level_file",
    "water_level_ini_type": "layers.initial_conditions.initial_water_level_aggregation",
    "output_time_step": "layers.time_step_settings.output_time_step",
    "dem_file": "layers.model_settings.dem_file",
    "frict_coef_file": "layers.model_settings.friction_coefficient_file",
    "use_2d_rain": "layers.model_settings.use_2d_rain",
    "use_2d_flow": "layers.model_settings.use_2d_flow",
    "simple_infiltration_settings_id": "layers.model_settings.use_simple_infiltration",
    "infiltration_rate": "layers.simple_infiltration.infiltration_rate",
    "infiltration_rate_file": "layers.simple_infiltration.infiltration_rate_file",
    "infiltration_surface_option": "layers.simple_infiltration.infiltration_surface_option",
    "max_infiltration_capacity_file": "layers.simple_infiltration.max_infiltration_volume_file",
    "kmax": "REMOVED",
    "\ufeffid": "REMOVED",
    "display_name": "REMOVED",
}


def _convert_values(key: str, value) -> Optional[object]:
    """Convert CSV string values to proper Python types."""
    # Handle NaN and None first
    if pd.isna(value) or value is None or value == "" or str(value).strip() == "":
        value = None

    if key in [
        "simple_infiltration_settings_id",
        "control_group_id",
        "use_2d_rain",
        "use_2d_flow",
        "use_0d_inflow",
    ]:
        if value is None:
            return False
        return bool(value)

    if value is None:
        return value

    if "_file" in key:
        value = value.replace("rasters/", "")

    if key == "name":
        value = value.replace("_test", "_check")

    if str(value).isdigit():
        return int(value)

    try:
        return float(value)
    except ValueError:
        pass

    return value


def _set_nested_dict(nested_dict, layer_name, value) -> None:
    """Set a value in a nested dictionary using dot notation path.
    Mutates inplace
    """
    keys = layer_name.split(".")
    for key in keys[:-1]:
        if key not in nested_dict:
            nested_dict[key] = {}
        nested_dict = nested_dict[key]
    nested_dict[keys[-1]] = value


def migrate_scenario_xlsx_to_json(folder: Folders) -> None:
    """Convert model_settings.xlsx to schematisation_scenarios.json format."""
    input_xlsx = folder.model.settings.path
    output_json = folder.model.schematisation_scenarios
    scenarios: dict[str, dict] = {}

    df = pd.read_excel(input_xlsx)

    # Iterate through sceenario rows
    for _, row in df.iterrows():
        clean_row = {}
        for key, value in row.items():
            clean_row[key] = _convert_values(key, value)

        # Apply column renames
        renamed_row = {}
        for old_key in MODEL_SETTINGS_RENAMES.keys():
            new_key = MODEL_SETTINGS_RENAMES[old_key]
            if new_key != "REMOVED":
                _set_nested_dict(renamed_row, new_key, clean_row.get(old_key))

        scenario_name = renamed_row["layers"]["simulation_template_settings"]["name"]
        scenario_name = scenario_name.replace("_test", "_check")

        # Remove all null simple_infiltration
        if renamed_row["layers"]["model_settings"]["use_simple_infiltration"] is False:
            renamed_row["layers"].pop("simple_infiltration", None)

        scenarios[scenario_name] = renamed_row

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(scenarios, f, indent=4)

    logger.info(f"Written {output_json}")


def migrate_schematisation_to_latest_version(folder: Folders) -> None:
    """Migrate the GPKG schematisation to the latest version, including upgrading the SpatiaLite version if needed.
    Will transform the sqlite into gpkg, without deleting the sqlite.
    """

    gpkg_path = folder.model.schema_base.database.path

    logger.info(f"Migrating {gpkg_path} to latest version ({threedi_schema.__version__})...")
    threedi_db: ThreediDatabase = ThreediDatabase(path=gpkg_path)
    model_schema: ModelSchema = threedi_db.schema
    model_schema.upgrade(upgrade_spatialite_version=True, keep_spatialite=True)


# %%
if __name__ == "__main__":
    from tests.config import FOLDER_NEW, FOLDER_TEST

    folder = FOLDER_TEST
    migrate_scenario_xlsx_to_json(folder)

# %%
