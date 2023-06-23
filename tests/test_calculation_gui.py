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
            rain_data=[{}],)

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

    


if __name__ == "__main__":
    test_simulationdata()