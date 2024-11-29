# %%
# Local imports

# from notebooks.notebook_setup import setup_notebook
# notebook_data = setup_notebook()

from hhnk_threedi_tools.core.checks.zero_d_one_d import ZeroDOneDTest
from tests.config import FOLDER_TEST, PATH_TEST_MODEL


class TestZeroDOneD:
    test_0d1d = ZeroDOneDTest.from_path(PATH_TEST_MODEL)

    def test_run_zero_d_one_d_test(self):
        """test of de 0d1d test werkt"""
        self.test_0d1d.run()
        assert self.test_0d1d.results["lvl_end"].count() == 157

    def test_run_hydraulic_test(self):
        """test of de hydraulische testen werken"""
        self.test_0d1d.run_hydraulic()
        assert self.test_0d1d.hydraulic_results["channels"]["code"].count() == 134


# %%
if __name__ == "__main__":
    import inspect

    selftest = TestZeroDOneD()
    self = selftest.test_0d1d
    # Run all testfunctions
    for i in dir(selftest):
        if i.startswith("test_") and hasattr(inspect.getattr_static(selftest, i), "__call__"):
            print(i)
            getattr(selftest, i)()
# %%
