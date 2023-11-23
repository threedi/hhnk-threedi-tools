# %%
import importlib
import os

import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import pytest

import hhnk_threedi_tools.qgis.layer_structure as layer_structure
from tests.config import FOLDER_TEST, TEST_DIRECTORY

if __name__ == "__main__":
    importlib.reload(layer_structure)


# %%
def test_layer_structure():
    # %%
    # Check if we get exception with bogus input.
    with pytest.raises(Exception):
        ls = layer_structure.LayerStructure(layer_structure_path=r"givemeanexception.csv", subjects=None)
        ls.run()
    with pytest.raises(Exception):
        ls = layer_structure.LayerStructure(
            layer_structure_path=TEST_DIRECTORY.joinpath("layer_structure.csv"), subjects=["thisisatypo"]
        )
        ls.run()

    # Generate structure
    self = layer_structure.LayerStructure(
        layer_structure_path=TEST_DIRECTORY.joinpath("layer_structure.csv"), subjects=["test_0d1d"], folder=FOLDER_TEST
    )
    self.run()
    # self.groups = layer_structure.QgisAllGroupsSettings(layers=self.layers)
    # self.groups.groups = self.groups.generate_groups()

    assert len(self.df_full) == 7
    assert len(self.df) == 4
    assert "layer" in self.df_full.keys()
    assert self.themes.example2.layer_ids == [
        "Waterstand_T1_uur____07. Testprotocol 1d2d tests[]__Kaart 2: Waterstand"
    ]
    assert len([i for i in self.groups.groups.get_all_children()]) == 14

    assert self.layers["Luchtfoto 2017 HHNK WMS____Achtergrond"].qml_lst is not None


# %%

if __name__ == "__main__":
    self = layer_structure.LayerStructure(
        layer_structure_path=TEST_DIRECTORY.joinpath("layer_structure.csv"), subjects=["test_0d1d"], folder=FOLDER_TEST
    )
    self.run()

    l = self.df_full.loc[0, "layer"]


# %%
