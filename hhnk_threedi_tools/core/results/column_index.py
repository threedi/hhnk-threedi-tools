from dataclasses import dataclass

import geopandas as gpd
import hhnk_research_tools as hrt

AVAILABLE_SIMULATION_TYPES = ["0d1d_test", "1d2d_test", "klimaattoets", "breach"]

logger = hrt.logging.get_logger(__name__)


@dataclass
class ColumnIdx:
    """
    Utility class to find the index of columns in a GeoDataFrame for inserting new columns at logical positions.
    This helps group related column types together, e.g.:
    wlvl_1h wlvl_3h wlvl_1h_corr wlvl_3h_corr diff_1h diff_15h

    Attributes
    ----------
    gdf : gpd.GeoDataFrame
        The GeoDataFrame whose columns are being indexed.

    Methods
    -------
    _get_idx(search_str)
        Returns the index for inserting a column matching the search pattern.
    Properties for common column types:
        wlvl, wlvl_corr, diff, vol, infilt, incept, rain, q, u1, storage
    """

    gdf: gpd.GeoDataFrame

    def _get_idx(self, search_str) -> int:
        """Get idx based on search pattern, if not found return last index"""
        idxs = self.gdf.columns.get_indexer(
            self.gdf.columns[self.gdf.columns.str.contains(search_str, na=False)]
        ).tolist()
        return (idxs or [len(self.gdf.columns) - 1])[-1] + 1

    @property
    def wlvl(self):
        return self._get_idx(search_str="^wlvl_(?!.*corr).*")

    @property
    def wlvl_corr(self):
        return self._get_idx(search_str="^wlvl_corr_.*")

    @property
    def diff(self):
        return self._get_idx(search_str="^diff_.*")

    @property
    def vol(self):
        return self._get_idx(search_str="^vol_.*")

    @property
    def infilt(self):
        return self._get_idx(search_str="^infilt_.*")

    @property
    def incept(self):
        return self._get_idx(search_str="^incept_.*")

    @property
    def rain(self):
        return self._get_idx(search_str="^rain_.*")

    @property
    def q(self):
        return self._get_idx(search_str="^discharge_.*")

    @property
    def u1(self):
        return self._get_idx(search_str="^vel_.*")

    @property
    def storage(self):
        return self._get_idx(search_str="^storage_mm_.*")
