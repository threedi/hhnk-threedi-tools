# %%
import sys

import geopandas as gpd
import pandas as pd
import pytest

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.relations import ChannelRelations, StructureRelations
from tests.config import TEST_DIRECTORY


def test_structure_relations():
    folder = Folders(TEST_DIRECTORY / "model_test")

    # load orifices as structure
    self = StructureRelations(folder=folder, structure_table="orifice")
    orifice_gdf = self.gdf

    assert len(orifice_gdf) == 5
    assert "min_ref_level_end" in list(orifice_gdf.columns)
    assert round(orifice_gdf["min_ref_level_end"].iloc[0], 1) == -0.9

    # test wrong profiles
    wrong_profile_dict: dict[str, gpd.GeoDataFrame] = {}
    for side in ["start", "end"]:
        # Get wrong profiles on both sides of structure
        wrong_profiles_side = self.get_wrong_profile(side=side)
        wrong_profile_dict[f"{self}_{side}"] = wrong_profiles_side
    # Combine wrong profiles
    wrong_profiles_gdf = pd.concat(wrong_profile_dict.values(), ignore_index=True)

    assert wrong_profiles_gdf["proposed_reference_level"].iloc[0] == -125.0


def test_channel_relations():
    folder = Folders(TEST_DIRECTORY / "model_test")

    self = ChannelRelations(folder=folder)
    channel_gdf = self.gdf

    assert len(channel_gdf) == 49
    assert "width_at_wlvl_mean" in list(channel_gdf.columns)
    assert round(channel_gdf["depth_max"].iloc[0], 1) == 1.0
    assert sum(channel_gdf["is_primary"]) == 7


# %%
