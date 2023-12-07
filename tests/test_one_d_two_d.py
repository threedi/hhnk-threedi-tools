# %%
"""Functional testing for oneDtwoD object"""
import os
import pathlib

import pytest

from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.folders import Folders
from tests.config import FOLDER_NEW, FOLDER_TEST

# Globals
REVISION = "BWN bwn_test #6 1d2d_test"


class TestOneDTwoD:
    # Remove previous output
    FOLDER_TEST.output.one_d_two_d.unlink_contents(rmdirs=True)

    @pytest.fixture(scope="class")
    def check_1d2d(self):
        check_1d2d = OneDTwoDTest(folder=FOLDER_TEST, revision=REVISION)
        check_1d2d.output_fd.create(parents=True)
        return check_1d2d

    def test_run_flowline_stats(self, check_1d2d):
        """Test of de hydraulische testen werken"""
        output = check_1d2d.run_flowline_stats()

        assert output["pump_capacity_m3_s"][1094] == 0.00116666666666667

    def test_run_node_stats(self, check_1d2d):
        output = check_1d2d.run_node_stats()

        assert round(output["minimal_dem"][1], 3) == 1.54

    def test_run_depth_at_timesteps_test(self, check_1d2d):
        """Test of de 0d1d test werkt"""

        check_1d2d.run_wlvl_depth_at_timesteps(overwrite=True)

        assert "waterdiepte_T15.tif" in [
            i.name for i in check_1d2d.fenv.output.one_d_two_d[check_1d2d.revision].content
        ]
        assert check_1d2d.fenv.output.one_d_two_d[check_1d2d.revision].waterdiepte_T1.shape == [787, 242]
        assert check_1d2d.fenv.output.one_d_two_d[check_1d2d.revision].waterdiepte_T15.sum() == 1576.087158203125


# %%
if __name__ == "__main__":
    import inspect

    self = TestOneDTwoD()
    check_1d2d = self.check_1d2d()

    # Run all testfunctions
    for i in dir(self):
        if i.startswith("test_") and hasattr(inspect.getattr_static(self, i), "__call__"):
            print(i)
            getattr(self, i)(check_1d2d)
# %%
