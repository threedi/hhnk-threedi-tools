# %%
import json
import os
import shutil
from functools import cached_property
from pathlib import Path
from typing import Tuple

import hhnk_research_tools as hrt
import pandas as pd
from core.folders import Folders
from core.schema_scenario_builder.models import LAYER_MAP
from core.schematisation import upload
from core.schematisation.threedi_schematisation import ThreediSchematisation

logger = hrt.logging.get_logger(__name__)


class RanaSchematisationApiService:
    """Service for uploading schematisations to 3Di and requesting information on revisions."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api = upload.threedi
        self.api.set_api_key(api_key)

    def upload(self, scenario_folder: Path, name: str, commit_message: str) -> None:
        """Upload scenario to 3Di."""
        pass

    def get_revision_info(self, name: str) -> str:
        return ""


class ScenarioBuilder:
    def __init__(self, folder: Folders):
        self.folder = folder
        self.folder.model.add_scenarios_from_json()  # loads scenarios and adds them as attributes to folder.model

    @cached_property
    def scenarios(self) -> dict:
        with self.folder.model.schematisation_scenarios.open(encoding="utf-8") as f:
            scenarios = json.load(f)
        return scenarios

    def build_scenario(self, scenario_name: str) -> Path:
        """Build schematisation of scenario.

        Steps:
        1. Copy required files from the default scenario, including the base schematisation.
        2. Edit the schematisation based on the scenario settings.
        3. Apply additional changes for specific scenarios (e.g. 0d1d_check)
        4. Apply sql changes as defined in folder.model.model_sql
        """

        scenario_folder = Path(f"./{scenario_name}")
        return scenario_folder

    def copy_base_schematisation(self, scenario_name: str) -> None:
        """Copy the base schematisation to a new folder for the scenario."""
        schema_base = self.folder.model.schema_base
        schema_new: ThreediSchematisation = getattr(self.folder.model, f"schema_{scenario_name}")
        schema_new.mkdir()
        schema_new.rasters.mkdir()

        # Copy schematisation

        dst = schema_new.full_path(schema_base.database.name)
        shutil.copyfile(src=schema_base.database.base, dst=dst.base)

        # Copy rasters that are defined in the settings JSON
        scenario_settings: dict = self.scenarios[scenario_name]

        def _iter_raster_files(settings: dict):
            """Yield (key, filename) for every nested '*_file' entry with a non-empty value."""
            for layer_vals in settings["layers"].values():
                if not isinstance(layer_vals, dict):
                    continue
                for k, v in layer_vals.items():
                    if k.endswith("_file") and v:
                        if v.endswith(".tif") or v.endswith(".tiff"):
                            yield k, v

        for _key, raster_name in _iter_raster_files(scenario_settings):
            if raster_name is not None:
                src = schema_base.rasters.full_path(raster_name)

                if src.exists():
                    dst = schema_new.rasters.full_path(raster_name)
                    logger.info(f"{scenario_name}: Copying '{src.name}' to scenario folder for '{scenario_name}'")
                    shutil.copyfile(src=src.base, dst=dst.base)
                else:
                    raise FileNotFoundError(
                        f"The '{raster_name}' used in run-name '{scenario_name}' is missing in the base-schematization. "
                        f"It is expected at {src}. Please provide the file or change your schematisation-settings file."
                    )

    def update_scenario_from_json(self, scenario_name: str, gpkg_path: Path) -> None:
        """Update the layers in the schematisation GPKG based on the scenario settings defined in the JSON file."""
        scenario_settings: dict = self.scenarios[scenario_name]

        for layer_name, values in scenario_settings["layers"].items():
            # This cls variable is used to determine which pydantic model to use for loading and updating the layer
            cls = LAYER_MAP.get(layer_name)
            if not cls:
                continue

            # Load current values from gpkg
            model = cls.load(gpkg_path)
            current: dict[str, any] = model.model_dump()

            # Update the fields present in the JSON, validating the values using pydantic
            for k, v in values.items():
                if k in current:
                    if current[k] != v:
                        logger.info(f"{scenario_name}: Updating {layer_name}.{k} from {current[k]} to {v}")
                        setattr(model, k, v)
                else:
                    raise KeyError(
                        f"{scenario_name}: Attempting to update {layer_name}.{k} which is not a valid field in the layer according to the pydantic model. Fix the schematisation_scenarios.json"
                    )

            model.write_to_gpkg(gpkg_path)  # write back to gpkg


class ScenarioService:
    def __init__(self, folder: Folders):
        self.folder = folder
        self.rana_service = RanaSchematisationApiService(api_key=os.environ["THREEDI_API_KEY"])

    def run(self, scenario_name, commit_message):
        """Run the scenario building and uploading process.

        Steps:
        1. Build the scenario using the ScenarioBuilder.
        2. Upload the scenario to 3Di using the RanaSchematisationService.
        """

    def _create_backup_revisions(self, commit_message: str) -> None:
        """Create backup revisions for all scenarios before building new ones."""

    def build_scenarios(self, scenario_names: list[str]) -> None:
        self._create_backup_revisions(commit_message="backup before building new scenarios")
        scenario_builder = ScenarioBuilder(folder=self.folder)
        for scenario_name in scenario_names:
            scenario_builder.build_scenario(scenario_name)

    def upload_scenarios(self, scenario_names: list[str], commit_message: str) -> None:
        # validatie met 3di of model goed is
        for scenario_name in scenario_names:
            self.rana_service.upload(scenario_name=scenario_name, commit_message=commit_message)
