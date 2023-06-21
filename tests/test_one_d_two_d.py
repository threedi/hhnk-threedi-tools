# %%
# -*- coding: utf-8 -*-
"""
Functional testing for oneDtwoD object
"""
# First-party imports
import os
import pathlib

# Local imports
from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.folders import Folders

# Globals
REVISION = "BWN bwn_test #6 1d2d_test"

from tests.config import FOLDER_TEST


class TestOneDTwoD:


    #Remove previous output
    FOLDER_TEST.output.one_d_two_d.unlink_contents(rmdirs=True)

    test_1d2d= OneDTwoDTest(folder=FOLDER_TEST, revision=REVISION)
    test_1d2d.output_fd.create(parents=True)
    

    def test_run_flowline_stats(self):
        """test of de hydraulische testen werken"""
        output = self.test_1d2d.run_flowline_stats()

        assert output["pump_capacity_m3_s"][1094] == 0.00116666666666667


    def test_run_node_stats(self):
        output = self.test_1d2d.run_node_stats()

        assert round(output["minimal_dem"][1], 3) == 1.54


    def test_run_depth_at_timesteps_test(self):
        """test of de 0d1d test werkt"""

        self.test_1d2d.run_wlvl_depth_at_timesteps(overwrite=True)

        assert "waterdiepte_T15.tif" in self.test_1d2d.fenv.output.one_d_two_d[self.test_1d2d.revision].content
        assert self.test_1d2d.fenv.output.one_d_two_d[self.test_1d2d.revision].waterdiepte_T1.shape == [787, 242]
        assert self.test_1d2d.fenv.output.one_d_two_d[self.test_1d2d.revision].waterdiepte_T15.sum() == 1576.087158203125


# %%
if __name__ == "__main__":
    import inspect
    selftest = TestOneDTwoD()
    self = selftest.test_1d2d

    # Run all testfunctions
    for i in dir(selftest):
        if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
            print(i)
            getattr(selftest, i)()  
# %%