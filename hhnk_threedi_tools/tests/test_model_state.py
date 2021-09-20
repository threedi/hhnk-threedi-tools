# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 10:46:05 2021

@author: chris.kerklaan

Functional testing for zeroDoneD object
Note: the model needs to be in a

"""
# First-party imports
import pathlib

# Local imports
from hhnk_threedi_tools.core.folders import Folders

from hhnk_threedi_tools.variables.model_state import (
    hydraulic_test_state,
    one_d_two_d_state,
    undefined_state,
    invalid_path,
)

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tests/hhnk_threedi_tools/tests/test_model_state.py"
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_get_state():
    """test of de 0d1d test werkt"""
    folder = Folders(TEST_MODEL)
    assert folder.model.state == "Hydraulische toets"


def test_adjustments_global_settings():
    folder = Folders(TEST_MODEL)
    output = folder.model.proposed_adjustments("global_settings", one_d_two_d_state)[0]
    assert output["new_control_group_id"][0].values[0] == 1


def test_adjustments_channels():
    folder = Folders(TEST_MODEL)
    output = folder.model.proposed_adjustments("channels", one_d_two_d_state)
    assert output["code"][46] == "162_1"


def test_adjustments_weirs():
    folder = Folders(TEST_MODEL)
    output = folder.model.proposed_adjustments("weirs", one_d_two_d_state)
    assert output["weir_code"][0] == "KST-J-2349"


def test_adjustments_manholes():
    folder = Folders(TEST_MODEL)
    output = folder.model.proposed_adjustments("manholes", one_d_two_d_state)
    assert output.empty == True
