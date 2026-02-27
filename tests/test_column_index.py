import geopandas as gpd

from hhnk_threedi_tools.core.results.column_index import ColumnIdx


def test_column_idx_insert_and_indices():
    # Create a GeoDataFrame with columns similar to expected patterns
    columns = [
        "wlvl_1h",
        "wlvl_3h",
        "wlvl_corr_1h",
        "diff_1h",
        "vol_1h",
        "rain_1h",
        "discharge_1h",
        "vel_1h",
        "storage_mm_1h",
    ]
    gdf = gpd.GeoDataFrame(columns=columns)
    idx = ColumnIdx(gdf)

    # Assert insertion indices for a few patterns
    assert idx.wlvl == 2  # after last wlvl_*
    assert idx.wlvl_corr == 3  # after last wlvl_corr_*
    assert idx.diff == 4  # after last diff_*

    # Insert a new column at the wlvl_corr index and check position
    gdf.insert(idx.wlvl_corr, "wlvl_corr_2h", [0] * len(gdf))
    assert list(gdf.columns)[3] == "wlvl_corr_2h"
