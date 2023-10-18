# %%
import importlib
import hhnk_research_tools as hrt
import pandas as pd
import os
import hhnk_threedi_tools.qgis.layer_structure as layer_structure
import pytest
import numpy as np

if __name__ == "__main__":
    importlib.reload(layer_structure)

from tests.config import FOLDER_TEST, TEST_DIRECTORY, TEMP_DIR

LAYER_STRUCTURE_PATH = r"E:\github\wvangerwen\hhnk-threedi-tools\tests\data\layer_structure.csv"
# LAYER_STRUCTURE_PATH = r"\\corp.hhnk.nl\data\hydrologen_data\Data\github\wvangerwen\hhnk-threedi-plugin\hhnk_threedi_plugin\qgis_interaction\layer_structure\testprotocol.csv"

# %%
def test_layer_structure():
    # %%
    #Check if we get exception with bogus input.
    with pytest.raises(Exception):
        ls = layer_structure.LayerStructure(layer_structure_path=r"givemeanexception.csv",
                                        subjects=None)
        ls.run()
    with pytest.raises(Exception):
        ls = layer_structure.LayerStructure(layer_structure_path=LAYER_STRUCTURE_PATH,
                                        subjects=["thisisatypo"])
        ls.run()

    #Generate structure
    self = layer_structure.LayerStructure(layer_structure_path=LAYER_STRUCTURE_PATH,
                                        subjects=['test_0d1d'],
                                        folder=FOLDER_TEST)
    self.run()

    assert len(self.df_full) == 4
    assert len(self.df) == 1
    assert "layer" in self.df_full.keys()
    assert self.themes.example1.layer_ids == ["waterstand_einde_regen_vs_begin_regen____05. Hydraulische Toets en 0d1d tests[]__Kaart 3: Streefpeilhandhaving"]


# %%

if __name__=="__main__":
    self = layer_structure.LayerStructure(layer_structure_path=LAYER_STRUCTURE_PATH,
                                        subjects=['test_0d1d'],
                                        folder=FOLDER_TEST)
    self.run()
    
    l = self.df_full.loc[0, "layer"]

self.layers


# %%
