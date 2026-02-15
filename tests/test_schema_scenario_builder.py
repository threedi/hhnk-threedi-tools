# %%
from pathlib import Path

import geopandas as gpd
from core.schema_scenario_builder.settings import ScenarioSettings

import hhnk_threedi_tools as htt
from tests.config import FOLDER_NEW, FOLDER_TEST

MODULE_DIR = Path(htt.__file__).parent

if __name__ == "__main__":
    # settings_file = Path(f"{Path(__file__).parent}/resources/model_settings.xlsx")
    # settings_default_file = Path(f"{Path(__file__).parent}/resources/model_settings_default.xlsx")
    settings_file = FOLDER_TEST.model.settings.path

    self = ScenarioSettings(settings_file=settings_file, settings_default_file=settings_default_file)

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
