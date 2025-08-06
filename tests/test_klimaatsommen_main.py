# %%
# First-party imports
import shutil

import hhnk_research_tools as hrt
import pandas as pd
import pytest

# Local imports
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_main import (
    KlimaatsommenMain,
)
from hhnk_threedi_tools.core.folder_helpers import ClimateResult
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

folder = FOLDER_TEST
