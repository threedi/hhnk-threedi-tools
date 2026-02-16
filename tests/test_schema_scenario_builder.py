# %%
import json
import shutil
import warnings

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.schema_scenario_builder.builder import ScenarioBuilder
from tests.config import FOLDER_NEW, FOLDER_TEST

warnings.filterwarnings("error", category=FutureWarning)
shutil.copytree(FOLDER_TEST.model.schema_base.base, FOLDER_NEW.model.schema_base.base, dirs_exist_ok=True)
shutil.copy(FOLDER_TEST.model.schematisation_scenarios, FOLDER_NEW.model.schematisation_scenarios)


# %%
if __name__ == "__main__":
    folder = FOLDER_NEW
    scenario_name = "0d1d_check"

    self = builder = ScenarioBuilder(folder=folder)

    with folder.model.schematisation_scenarios.open(encoding="utf-8") as f:
        scenarios = json.load(f)

    for scenario_name in scenarios:
        scenario = scenarios[scenario_name]

        builder.copy_base_schematisation(scenario_name=scenario_name)

        gpkg_path = folder.model.schema_0d1d_check.database.path
        assert gpkg_path.exists()

        builder.update_scenario_from_json(scenario_name=scenario_name, gpkg_path=gpkg_path)

        for layer_name, values in scenario["layers"].items():
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
            for k, v in values.items():
                cell_value = gdf[k].to_numpy()[0]
                # treat NaN and None as equivalent (don't raise when both are missing)
                if pd.isna(cell_value) and v is None:
                    continue
                assert cell_value == v


# %%

# %%


# import json

# row_dict = dict(settings_default_df.iloc[0])
# with open("default.json", "w") as f:
#     json.dump(row_dict, f, indent=4)
# %%

import json

with open(settings_default_file) as f:
    data = json.load(f)

defaults = ScenarioDefaults(**data)
