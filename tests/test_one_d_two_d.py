# %%
"""Functional testing for oneDtwoD object"""

import geopandas as gpd
import pytest

from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from tests.config import FOLDER_TEST

# Globals
REVISION = "BWN bwn_test #6 1d2d_test"


class TestOneDTwoD:
    # Remove previous output.
    FOLDER_TEST.output.one_d_two_d.unlink_contents(rmdirs=True)

    @pytest.fixture(scope="class")
    def check_1d2d(self):
        check_1d2d = OneDTwoDTest(folder=FOLDER_TEST, revision=REVISION)
        check_1d2d.output_fd.mkdir(parents=True)
        return check_1d2d

    def test_run_flowline_stats(self, check_1d2d):
        """Test of de hydraulische testen werken"""
        output = check_1d2d.run_flowline_stats()

        assert output["pump_capacity_m3_s"][1094] == 0.00116666666666667

    def test_run_depth_at_timesteps(self, check_1d2d):
        """Test of de 0d1d test werkt"""
        # TODO move output to temp folder
        output_fd = check_1d2d.folder.output.one_d_two_d[check_1d2d.revision]

        check_1d2d.run_wlvl_depth_at_timesteps(overwrite=True)

        grid_gdf = gpd.read_file(output_fd.grid_nodes_2d.path)
        assert grid_gdf.loc[0, "wlvl_corr_15h"] == 0.7591500282287598
        output_fd.waterdiepte_T15.exists()

        assert output_fd.waterdiepte_T1.shape == [787, 242]
        assert output_fd.waterdiepte_T15.sum() == 1654.740478515625


# %%
if __name__ == "__main__":
    import inspect

    self = TestOneDTwoD()
    check_1d2d = self.check_1d2d()

    # %%
    # Run all testfunctions
    for i in dir(self):
        if i.startswith("test_") and hasattr(inspect.getattr_static(self, i), "__call__"):
            print(i)
            getattr(self, i)(check_1d2d)
