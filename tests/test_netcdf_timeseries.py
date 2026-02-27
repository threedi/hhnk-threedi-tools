from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.results.netcdf_timeseries import AggregateNetcdfTimeSeries, NetcdfTimeSeries

folder_path = r"tests\data\model_test"
folder = Folders(folder_path)

threedi_result = folder.threedi_results.one_d_two_d[0]
user_defined_timesteps = ["max", 3600, 5400, "3600"]

self = NetcdfTimeSeries.from_folder(
    folder=folder,
    threedi_result=threedi_result,
)
assert self.typecheck

self_agg = AggregateNetcdfTimeSeries.from_folder(
    folder=folder,
    threedi_result=threedi_result,
)
assert self_agg.typecheck

assert self.get_ts_index(time_seconds=3600) == 4
assert self_agg.get_ts_index(time_seconds=3600) == 12

attribute = "s1"
element = "nodes"
subset = "2D_OPEN_WATER"
ts = self.get_ts(attribute=attribute, element=element, subset=subset)

assert ts.shape == (422, 61)

attribute = "vol_current"
assert attribute in self_agg.variables

ts_agg = self_agg.get_ts(attribute=attribute, element=element, subset=subset)

assert ts_agg.shape == (422, 181)
