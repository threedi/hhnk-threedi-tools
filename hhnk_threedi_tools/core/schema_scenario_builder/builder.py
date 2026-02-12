# %%
import os
from pathlib import Path
from typing import Tuple

import hhnk_research_tools as hrt
import pandas as pd
from core.folders import Folders
from core.schematisation import upload
from git_model_repo.hooks import commit_msg

logger = hrt.get_logger(__name__)


class RanaSchematisationService:
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


class ScenarioSettings:
    """Read and validate settings for scenario building.

    Settings are stored in two csv files
    - schematisation_settings_default.csv
        Contains default settings for all scenarios. This file should not be edited.
    - schematisation_settings.csv
        Each row represents a scenario. The specific settings are stored in the columns.
    """

    def __init__(self, folder: Folders):
        self.settings_file = folder.model_settings
        self.settings_default_file = folder.model_settings_default

    def read_settings(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Read settings from file."""
        settings_df = pd.read_csv(self.settings_file, sep=";")
        settings_default_df = pd.read_csv(self.settings_default_file, sep=";")

        return settings_df, settings_default_df

    def validate(self, settings_df: pd.DataFrame, settings_default_df: pd.DataFrame) -> None:
        """Sanity check settings tables"""
        intersect = settings_df.keys().intersection(settings_default_df.keys())
        if len(intersect) > 0:
            logger.warning(
                f"""Er staan kolommen zowel in de defaut als in de andere modelsettings.
        Dat lijkt me een slecht plan. Kolommen: {intersect.values}"""
            )

    def get(self, scenario_name: str) -> pd.DataFrame:
        """Get settings for scenario."""
        settings_df, settings_default_df = self.read_settings()
        self.validate(settings_df, settings_default_df)

        return settings_df.loc[scenario_name]


class ScenarioBuilder:
    def __init__(self, folder: Folders, scenario_name: str):
        self.folder = folder
        self.scenario_name = scenario_name

    def build_scenario(self, scenario_name: str) -> Path:
        """Build schematisation of scenario.

        Steps:
        1. Copy required files from the default scenario, including the base schematisation.
        2. Edit the schematisation based on the scenario settings.
        (apart from global_settings, also check the tables, they may need to be empty)
        3. Apply additional changes for specific scenarios (e.g. 0d1d_check)
        4. Apply sql changes as defined in folder.model.model_sql
        """

        scenario_folder = Path(f"./{scenario_name}")
        return scenario_folder


class ScenarioService:
    def __init__(self, folder: Folders):
        self.rana_service = RanaSchematisationService(api_key=os.environ["THREEDI_API_KEY"])
        self.settings = ScenarioSettings(folder=folder)

    def run(self, scenario_name, commit_message):
        """Run the scenario building and uploading process.

        Steps:
        1. Build the scenario using the ScenarioBuilder.
        2. Upload the scenario to 3Di using the RanaSchematisationService.
        """

    def _create_backup_revisions(self, commit_message: str) -> None:
        """Create backup revisions for all scenarios before building new ones."""

    def build_scenarios(self, scenario_names: list[str]) -> None:
        self._create_backup_revisions(commit_msg)
        for scenario_name in scenario_names:
            scenario_builder = ScenarioBuilder(scenario_name)
            scenario_builder.build_scenario(scenario_name)

    def upload_scenarios(self, scenario_names: list[str], commit_message: str) -> None:
        # validatie met 3di of model goed is
        for scenario_name in scenario_names:
            self.rana_service.upload(scenario_name=scenario_name, commit_message=commit_message)
