# %%
import shutil
from pathlib import Path

import geopandas as gpd

from hhnk_threedi_tools.core.schema_scenario_builder.builder import ScenarioBuilder
from tests.config import FOLDER_NEW, FOLDER_TEST

shutil.copytree(FOLDER_TEST.model.schema_base.base, FOLDER_NEW.model.schema_base.base, dirs_exist_ok=True)
shutil.copy(FOLDER_TEST.model.schematisation_scenarios, FOLDER_NEW.model.schematisation_scenarios)


# %%
if __name__ == "__main__":
    folder = FOLDER_NEW
    scenario_name = "0d1d_check"

    self = builder = ScenarioBuilder(folder=folder)
    builder.copy_base_schematisation(scenario_name=scenario_name)

    gpkg_path = folder.model.schema_0d1d_check.database
    assert gpkg_path.exists()

    builder.update_scenario_from_json(scenario_name=scenario_name, gpkg_path=gpkg_path)


# %%

# %%

m = Path(r"E:\02.modellen\23_Katvoed\02_schematisation\00_basis\bwn_katvoed.gpkg")

df = gpd.read_file(m, layer="global_settings").T
# import json

# row_dict = dict(settings_default_df.iloc[0])
# with open("default.json", "w") as f:
#     json.dump(row_dict, f, indent=4)
# %%

import json

with open(settings_default_file) as f:
    data = json.load(f)

defaults = ScenarioDefaults(**data)
