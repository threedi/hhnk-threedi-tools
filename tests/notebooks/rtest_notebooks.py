# %%
# First-party imports
import os
import pathlib

from hhnk_threedi_tools.core.folders import Folders

# Local imports
from hhnk_threedi_tools.utils.notebooks.run import open_server

# Globals
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"

# FIXME open_notebook not available anymore. Replace test?
# def test_run_notebook():
#     open_notebook("02_calculation_gui.ipynb")


def test_open_server_mp():
    open_server(location="user")


if __name__ == "__main__":
    # test_open_server_mp()
    pass
# %%
