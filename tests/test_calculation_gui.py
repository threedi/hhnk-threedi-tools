# %%
import shutil
from pathlib import Path
import pytest

import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.api.calculation import SimulationData
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER

     

def test_simulationdata():
    simdata = SimulationData(sqlite_path=FOLDER_TEST.model.schema_base.database.path, 
                                    sim_name="test_simdata", 
                                    sim_duration=900, 
                                    rain_data=[{}],
                                    threedi_api = None,
                                    model_id = None)

    #Numerical settings
    numerical_settings = simdata.numerical_settings
    assert numerical_settings["use_nested_newton"] == 1

    #Physical settings
    physical_settings = simdata.physical_settings
    assert physical_settings["use_advection_1d"] == 1

    #Timestep settings
    time_step_settings = simdata.time_step_settings
    assert time_step_settings["time_step"] == 15
    
    #Boundary data
    boundaries = simdata.boundaries
    assert boundaries == []

    #Aggregation settings
    aggregation = simdata.aggregation
    assert len(aggregation) == 11


# %%
if __name__ == "__main__":
    from threedi_api_client import ThreediApi
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
    notebook_data = setup_notebook()

    #Test if iniwlvl works. Needs api key so not on pytests (yet)
    api_keys = hrt.read_api_file(notebook_data["api_keys_path"])
    config = {
        "THREEDI_API_HOST": "https://api.3di.live",
        "THREEDI_API_PERSONAL_API_TOKEN": api_keys["threedi"],
    }
    threedi_api = ThreediApi(config=config)
    model_id = 58400
    FOLDER_TEST.model.set_modelsplitter_paths()
    simdata = SimulationData(sqlite_path=FOLDER_TEST.model.schema_1d2d_glg.database.path, 
                            sim_name="test_simdata", 
                            sim_duration=900, 
                            rain_data=[{}],
                            threedi_api = threedi_api,
                            model_id = model_id)

