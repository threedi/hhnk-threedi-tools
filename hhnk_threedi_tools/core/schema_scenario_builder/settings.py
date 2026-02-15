# %%


import json
from pathlib import Path
from typing import Tuple

import hhnk_research_tools as hrt
import pandas as pd
from core.schema_scenario_builder.models import ScenarioDefaults

logger = hrt.logging.get_logger(__name__)


class ScenarioSettings:
    """Read and validate settings for scenario building.

    Settings are stored in two csv files
    - schematisation_settings_default.csv
        Contains default settings for all scenarios. This file should not be edited.
    - schematisation_settings.csv
        Each row represents a scenario. The specific settings are stored in the columns.
    """

    def __init__(self, settings_file: Path, settings_default_file: Path):
        self.settings_file = settings_file
        self.settings_default_file = settings_default_file

    def read_settings(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Read settings from file."""
        settings_df = pd.read_csv(self.settings_file)

        with open(self.settings_default_file) as f:
            data = json.load(f)

        defaults = ScenarioDefaults(**data)

        return settings_df, defaults

    def validate(self, settings_df: pd.DataFrame, settings_default: ScenarioDefaults) -> None:
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
