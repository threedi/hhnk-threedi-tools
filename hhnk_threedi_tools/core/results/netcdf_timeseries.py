# %%
from dataclasses import dataclass

import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools.core.folders import Folders

"""Module for handling timeseries data from 2D netCDF results.

Classes
-------
NetcdfTimeSeries
    Class to manage and retrieve timeseries data from netCDF results.

Methods
-------
get_ts(attribute, element, subset)
    Retrieve timeseries data for a specific attribute, element type, and subset.    
get_ts_index(time_seconds)
    Retrieve the index of a specific time in seconds.
get_output_timesteps(user_defined_timesteps)
    Convert user defined output timesteps to timestep in ncdf indices.

"""
AVAILABLE_METHODS = ["mean", "median", "max", "max_abs", "min", "sum", "sum_pos", "sum_neg"]
logger = hrt.logging.get_logger(__name__)


@dataclass
class NetcdfTimeSeries:
    """Timeseries contained in a netcdf"""

    threedi_result: hrt.ThreediResult  # type threedigrid.admin.gridresultadmin.GridH5ResultAdmin
    user_defined_timesteps: list[int] | None = None
    use_aggregate: bool = False

    def __post_init__(self):
        self.ts = None

        # Check if result is aggregate # TODO Add test with aggregate
        self.aggregate: bool = self.typecheck_aggregate

        # Check if result has breaches
        self.breaches: bool = self.grid.has_breaches

    @classmethod
    def from_folder(cls, folder: Folders, threedi_result: hrt.ThreediResult, use_aggregate: bool = False, **kwargs):
        """Initialize from folder structure."""

        return cls(
            threedi_result=threedi_result,
            use_aggregate=use_aggregate,
        )

    @property
    def grid(self):
        """Instance of threedigrid.admin.gridresultadmin.GridH5ResultAdmin or GridH5AggregateResultAdmin"""
        if self.use_aggregate is False:
            return self.threedi_result.grid
        return self.threedi_result.aggregate_grid

    @property
    def timestamps(self):
        """Retrieve timestamps for timeseries"""
        return self.grid.nodes.timestamps

    @property
    def typecheck_aggregate(self) -> bool:
        """Check if we have a normal or aggregated netcdf"""
        return str(type(self.grid)) == "<class 'threedigrid.admin.gridresultadmin.GridH5AggregateResultAdmin'>"

    def get_ts(self, attribute: str, element: str, subset: str) -> np.ndarray:
        """
        Retrieve timeseries from netCDF result given element, subset and attribute.

        Parameters
        ----------
        attribute : str
            The attribute to retrieve (e.g. 's1', 'vol', etc.).
        element : str
            The element type to retrieve (e.g. 'nodes' or 'lines').
        subset : str
            The subset of elements to retrieve (e.g. 2D_OPEN_WATER, 1D_All, etc.).

        Returns
        -------
        np.ndarray
            A 2D numpy array with shape (number of elements, number of timestamps).
        """

        # TODO check if attribute, element and subset are valid

        ts = getattr(
            getattr(self.grid, element).subset(subset).timeseries(indexes=slice(0, len(self.timestamps))),
            attribute,
        ).T
        # Replace -9999 with nan values to prevent -9999 being used in replacing values.
        ts[ts == -9999.0] = np.nan
        logger.info(f"Retrieved {attribute} for {ts.shape[0]} {element} in {subset}")

        return ts

    def get_ts_index(self, time_seconds: int) -> int:
        """
        Retrieve indices of requested output time_seconds

        Parameters
        ----------
        time_seconds : int
            The time in seconds for which to retrieve the index.
        """
        abs_diff = np.abs(self.timestamps - time_seconds)
        # geeft 1 index voor de gevraagde timestep gelijk voor alle elementen
        ts_indx = np.argmin(abs_diff)
        if np.min(abs_diff) > 30:  # seconds diff. # TODO gebruik hier de helft van de opgegeven output time_seconds?
            raise ValueError(  # TODO toevoegen aan logging? Maar hoe omgaan met raise?
                f"""Provided time_seconds {time_seconds} not found in netcdf timeseries.
                    Closest timestep is {self.timestamps[ts_indx]} seconds at index {ts_indx}. \
                    Debug by checking available timeseries through the (.timestamps) timeseries attributes"""
            )

        return ts_indx

    def get_ts_methods(self, method: str, ts: np.ndarray) -> np.ndarray:
        """
        Get value from data using specified method.

        Parameters
        ----------
        method : str
            The method to apply to the timeseries data. Options include:
            'mean', 'median', 'max', 'max_abs', 'min', 'min_abs', 'sum', 'sum_pos', 'sum_neg'.
        ts : np.ndarray
            A 2D numpy array with shape (number of elements, number of timestamps).
        """

        if method in AVAILABLE_METHODS:
            # handle method for timeseries
            if method == "max":
                output = np.nanmax(ts, axis=1)
            if method == "max_abs":
                output = np.nanmax(abs(ts), axis=1)
            elif method == "min":
                output = np.nanmin(ts, axis=1)
            elif method == "mean":
                output = np.nanmean(abs(ts), axis=1)
            elif method == "median":
                output = np.nanmedian(abs(ts), axis=1)

            # TODO process timestep size in this step!!!!
            elif method == "sum":
                output = np.nansum(abs(ts), axis=1)
            elif method == "sum_pos":
                # filter negative values
                ts_pos = ts.copy()
                ts_pos[ts_pos < 0] = np.nan
                output = np.nansum(ts_pos, axis=1)

            elif method == "sum_neg":
                # filter positive values
                ts_neg = ts.copy()
                ts_neg[ts_neg > 0] = np.nan
                output = np.nansum(ts_neg, axis=1)
            return output

        else:
            logger.error(f"Key {method} not recognized, should be int or any of {AVAILABLE_METHODS}.")


# %%
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"tests\data\model_test"
    folder = Folders(folder_path)

    user_defined_timesteps = ["max", 3600, 5400]

    self = NetcdfTimeSeries.from_folder(
        folder=folder, threedi_result=folder.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf
    )

    assert self.get_ts_index(time_seconds=3600) == 12

    attribute = "s1"
    element = "nodes"
    subset = "2D_OPEN_WATER"
    ts = self.get_ts(attribute=attribute, element=element, subset=subset)

    assert ts.shape == (422, 577)
# %%
