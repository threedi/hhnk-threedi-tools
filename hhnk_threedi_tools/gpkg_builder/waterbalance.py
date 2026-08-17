# %%
"""
Standalone water balance calculation for 3Di simulation results.

This module is adapted from the WaterBalanceCalculation implementation
in 3Di Results Analysis:

https://github.com/nens/threedi-results-analysis/blob/master/tool_water_balance/calculation.py

The original implementation depends on QGIS. This adaptation replaces
the QGIS-specific spatial selection with threedigrid, GeoPandas, Pandas,
and NumPy so that the water balance can be calculated standalone.

The implementation has also been adapted to include
LINE_2D_OBSTACLE flowlines when calculating 2D boundary flows.

Original project:
https://github.com/nens/threedi-results-analysis
"""

from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .config import EXTERNAL_COMPONENTS, INPUT_SERIES, SERIES_INDEX, STORAGE_COMPONENTS
from .selection import select_lines_and_pumps, select_points


class WaterBalance:
    def __init__(self, threedi_result: hrt.ThreediResult, polygon_gdf: gpd.GeoDataFrame):
        self.threedi_result = threedi_result
        self.polygon_gdf = polygon_gdf

        self._polygon = polygon_gdf.to_crs(28992).geometry.union_all()
        self._grid = threedi_result.grid
        self._aggregate_grid = threedi_result.aggregate_grid
        self._admin = threedi_result.admin

        self._set_2d_line_ranges()

        # Spatial selections
        self.node_ids = select_points(
            aggregate_grid=self.aggregate_grid,
            polygon=self.polygon,
        )

        self.flowline_ids, self.pump_ids = select_lines_and_pumps(
            aggregate_grid=self.aggregate_grid,
            grid=self.grid,
            polygon=self.polygon,
            node_ids=self.node_ids,
            x2d_surf_range=self.x2d_surf_range,
            y2d_surf_range=self.y2d_surf_range,
            vert_flow_range=self.vert_flow_range,
        )

    @property
    def polygon(self):
        return self._polygon

    @property
    def grid(self):
        return self._grid

    @property
    def aggregate_grid(self):
        return self._aggregate_grid

    @property
    def admin(self):
        return self._admin

    def _set_2d_line_ranges(self):
        nr_2d_x = self.admin.get_from_meta("liutot")
        nr_2d_y = self.admin.get_from_meta("livtot")
        nr_2d = self.admin.get_from_meta("l2dtot")

        self.x2d_surf_range = range(
            1,
            nr_2d_x + 1,
        )

        self.y2d_surf_range = range(
            nr_2d_x + 1,
            nr_2d_x + nr_2d_y + 1,
        )

        self.vert_flow_range = range(
            nr_2d_x + nr_2d_y + 1,
            nr_2d_x + nr_2d_y + nr_2d + 1,
        )

    def _get_aggregated_flows(self):

        lines = self.aggregate_grid.lines
        pumps = self.aggregate_grid.pumps

        times = lines.get_timestamps("q_cum")

        # Same number/order of series as official Water Balance
        all_flows = np.zeros(
            shape=(len(times), len(INPUT_SERIES)),
            dtype=float,
        )

        # --------------------------------------------------
        # LINKS
        # --------------------------------------------------

        TYPE_1D = "1d"
        TYPE_2D = "2d"
        TYPE_2D_BOUND = "2d_bound"
        TYPE_1D_BOUND = "1d_bound"

        TYPE_1D__1D_2D_FLOW = "1d__1d_2d_flow"
        TYPE_2D__1D_2D_FLOW = "2d__1d_2d_flow"

        TYPE_1D__1D_2D_EXCH = "1d__1d_2d_exch"
        TYPE_2D__1D_2D_EXCH = "2d__1d_2d_exch"

        TYPE_2D_GROUNDWATER = "2d_groundwater"
        TYPE_2D_VERTICAL = "2d_vertical_infiltration"

        link_data = []

        # 2D
        for idx in self.flowline_ids["2d_in"]:
            link_data.append((idx, TYPE_2D, 1))

        for idx in self.flowline_ids["2d_out"]:
            link_data.append((idx, TYPE_2D, -1))

        # 2D boundaries
        for idx in self.flowline_ids["2d_bound_in"]:
            link_data.append((idx, TYPE_2D_BOUND, 1))

        for idx in self.flowline_ids["2d_bound_out"]:
            link_data.append((idx, TYPE_2D_BOUND, -1))

        # 1D
        for idx in self.flowline_ids["1d_in"]:
            link_data.append((idx, TYPE_1D, 1))

        for idx in self.flowline_ids["1d_out"]:
            link_data.append((idx, TYPE_1D, -1))

        # 1D boundaries
        for idx in self.flowline_ids["1d_bound_in"]:
            link_data.append((idx, TYPE_1D_BOUND, 1))

        for idx in self.flowline_ids["1d_bound_out"]:
            link_data.append((idx, TYPE_1D_BOUND, -1))

        # Groundwater
        for idx in self.flowline_ids["2d_groundwater_in"]:
            link_data.append((idx, TYPE_2D_GROUNDWATER, 1))

        for idx in self.flowline_ids["2d_groundwater_out"]:
            link_data.append((idx, TYPE_2D_GROUNDWATER, -1))

        # Vertical infiltration
        for idx in self.flowline_ids["2d_vertical_infiltration"]:
            link_data.append((idx, TYPE_2D_VERTICAL, 1))

        # 1D-2D crossing polygon
        for idx in self.flowline_ids["1d__1d_2d_flow"]:
            link_data.append((idx, TYPE_1D__1D_2D_FLOW, -1))

        for idx in self.flowline_ids["2d__1d_2d_flow"]:
            link_data.append((idx, TYPE_2D__1D_2D_FLOW, 1))

        # Internal domain exchange.
        # Same physical flow appears once from 1D perspective
        # and once from 2D perspective.
        for idx in self.flowline_ids["1d_2d_exch"]:
            link_data.append((idx, TYPE_1D__1D_2D_EXCH, -1))

            link_data.append((idx, TYPE_2D__1D_2D_EXCH, 1))

        link_data = np.array(
            link_data,
            dtype=[
                ("id", int),
                ("type", "U30"),
                ("direction", int),
            ],
        )

        if len(link_data) > 0:
            link_data.sort(order="id")

            ids = link_data["id"]
            directions = link_data["direction"]

            q_pos = np.ma.filled(
                lines.q_cum_positive[:, ids],
                0.0,
            )

            q_neg = np.ma.filled(
                lines.q_cum_negative[:, ids],
                0.0,
            )

            previous_pos = np.zeros(len(ids))
            previous_neg = np.zeros(len(ids))

            series_map = {
                TYPE_2D: (
                    SERIES_INDEX["2d_in"],
                    SERIES_INDEX["2d_out"],
                ),
                TYPE_1D: (
                    SERIES_INDEX["1d_in"],
                    SERIES_INDEX["1d_out"],
                ),
                TYPE_2D_BOUND: (
                    SERIES_INDEX["2d_bound_in"],
                    SERIES_INDEX["2d_bound_out"],
                ),
                TYPE_1D_BOUND: (
                    SERIES_INDEX["1d_bound_in"],
                    SERIES_INDEX["1d_bound_out"],
                ),
                TYPE_1D__1D_2D_FLOW: (
                    SERIES_INDEX["1d__1d_2d_flow_in"],
                    SERIES_INDEX["1d__1d_2d_flow_out"],
                ),
                TYPE_1D__1D_2D_EXCH: (
                    SERIES_INDEX["1d__1d_2d_exch_in"],
                    SERIES_INDEX["1d__1d_2d_exch_out"],
                ),
                TYPE_2D_GROUNDWATER: (
                    SERIES_INDEX["2d_groundwater_in"],
                    SERIES_INDEX["2d_groundwater_out"],
                ),
                TYPE_2D_VERTICAL: (
                    SERIES_INDEX["2d_vertical_infiltration_pos"],
                    SERIES_INDEX["2d_vertical_infiltration_neg"],
                ),
                TYPE_2D__1D_2D_FLOW: (
                    SERIES_INDEX["2d__1d_2d_flow_in"],
                    SERIES_INDEX["2d__1d_2d_flow_out"],
                ),
                TYPE_2D__1D_2D_EXCH: (
                    SERIES_INDEX["2d__1d_2d_exch_in"],
                    SERIES_INDEX["2d__1d_2d_exch_out"],
                ),
            }

            for ts_idx in range(len(times)):
                flow_pos = q_pos[ts_idx] * directions

                flow_neg = q_neg[ts_idx] * directions * -1

                delta_pos = flow_pos - previous_pos
                delta_neg = flow_neg - previous_neg

                previous_pos = flow_pos
                previous_neg = flow_neg

                for flow_type, (idx_in, idx_out) in series_map.items():
                    selected = link_data["type"] == flow_type

                    values = np.concatenate(
                        [
                            delta_pos[selected],
                            delta_neg[selected],
                        ]
                    )

                    all_flows[ts_idx, idx_in] = np.clip(values, 0, None).sum()

                    all_flows[ts_idx, idx_out] = np.clip(values, None, 0).sum()

        # --------------------------------------------------
        # PUMPS
        # --------------------------------------------------

        pump_data = []

        for idx in self.pump_ids["in"]:
            pump_data.append((idx, 1))

        for idx in self.pump_ids["out"]:
            pump_data.append((idx, -1))

        pump_data = np.array(
            pump_data,
            dtype=[
                ("id", int),
                ("direction", int),
            ],
        )

        if len(pump_data) > 0:
            pump_data.sort(order="id")

            ids = pump_data["id"]
            directions = pump_data["direction"]

            q_pump = np.ma.filled(
                pumps.q_pump_cum[:, ids],
                0.0,
            )

            previous = np.zeros(len(ids))

            for ts_idx in range(len(times)):
                values = q_pump[ts_idx] * directions

                delta = values - previous
                previous = values

                all_flows[
                    ts_idx,
                    SERIES_INDEX["pump_in"],
                ] = np.clip(delta, 0, None).sum()

                all_flows[
                    ts_idx,
                    SERIES_INDEX["pump_out"],
                ] = np.clip(delta, None, 0).sum()
        # --------------------------------------------------
        # NODES
        # --------------------------------------------------

        nodes = self.aggregate_grid.nodes

        node_2d = np.asarray(
            self.node_ids["2d"],
            dtype=int,
        )

        node_1d = np.asarray(
            self.node_ids["1d"],
            dtype=int,
        )

        node_2d_groundwater = np.asarray(
            self.node_ids["2d_groundwater"],
            dtype=int,
        )

        def cumulative_node_values(values, node_ids, factor=1):
            if len(node_ids) == 0:
                return np.zeros(len(times))

            total = np.ma.filled(
                values[:, node_ids],
                0.0,
            ).sum(axis=1)

            # cumulative volume -> volume during timestep
            delta = np.diff(
                total,
                prepend=0.0,
            )

            return delta * factor

        # Rain on 2D
        all_flows[:, SERIES_INDEX["rain"]] = cumulative_node_values(
            nodes.rain_cum,
            node_2d,
        )

        # Simple infiltration is a sink
        all_flows[
            :,
            SERIES_INDEX["infiltration_rate_simple"],
        ] = cumulative_node_values(
            nodes.infiltration_rate_simple_cum,
            node_2d,
            factor=-1,
        )

        # Lateral flow to 2D
        all_flows[:, SERIES_INDEX["lat_2d"]] = cumulative_node_values(
            nodes.q_lat_cum,
            node_2d,
        )

        # Lateral flow to 1D
        all_flows[:, SERIES_INDEX["lat_1d"]] = cumulative_node_values(
            nodes.q_lat_cum,
            node_1d,
        )

        # Rain on 1D
        all_flows[:, SERIES_INDEX["inflow"]] = cumulative_node_values(
            nodes.rain_cum,
            node_1d,
        )

        # --------------------------------------------------
        # Convert volume/timestep -> m3/s
        # --------------------------------------------------

        dt = np.diff(
            times,
            prepend=times[0],
        )

        dt[0] = times[1] - times[0]

        all_flows = all_flows / dt[:, None]

        # --------------------------------------------------
        # Volume change
        # --------------------------------------------------

        if len(node_2d) > 0:
            volume_2d = np.ma.filled(
                nodes.vol_current[:, node_2d],
                0.0,
            ).sum(axis=1)

            all_flows[:, SERIES_INDEX["d_2d_vol"]] = (
                np.diff(
                    volume_2d,
                    prepend=volume_2d[0],
                )
                / dt
            )

        if len(node_1d) > 0:
            volume_1d = np.ma.filled(
                nodes.vol_current[:, node_1d],
                0.0,
            ).sum(axis=1)

            all_flows[:, SERIES_INDEX["d_1d_vol"]] = (
                np.diff(
                    volume_1d,
                    prepend=volume_1d[0],
                )
                / dt
            )

        if len(node_2d_groundwater) > 0:
            volume_groundwater = np.ma.filled(
                nodes.vol_current[:, node_2d_groundwater],
                0.0,
            ).sum(axis=1)

            all_flows[:, SERIES_INDEX["d_2d_groundwater_vol"]] = (
                np.diff(
                    volume_groundwater,
                    prepend=volume_groundwater[0],
                )
                / dt
            )

        return times, all_flows

    def calculate(self):
        """Calculate water balance flow rates [m³/s]."""

        times, all_flows = self._get_aggregated_flows()

        balance = pd.DataFrame(
            data=all_flows,
            index=times,
            columns=SERIES_INDEX.keys(),
        )

        balance.index.name = "time"

        return balance

    def calculate_volumes(self, balance=None):
        """Return total volume [m³] per water balance component."""

        if balance is None:
            balance = self.calculate()

        times = balance.index.to_numpy()

        dt = np.diff(
            times,
            prepend=times[0],
        )
        dt[0] = times[1] - times[0]

        volumes = balance.mul(
            dt,
            axis=0,
        ).sum()

        return volumes

    def check_balance(self, volumes=None):
        """Check closure of the water balance."""

        if volumes is None:
            volumes = self.calculate_volumes()

        external_net = volumes[list(EXTERNAL_COMPONENTS)].sum()
        storage_change = volumes[list(STORAGE_COMPONENTS)].sum()

        balance_error = external_net - storage_change

        if storage_change != 0:
            relative_error = balance_error / abs(storage_change) * 100
        else:
            relative_error = np.nan

        return pd.Series(
            {
                "external_net": external_net,
                "storage_change": storage_change,
                "balance_error": balance_error,
                "relative_error_pct": relative_error,
            }
        )

    def plot(self, balance=None, components=None):
        """Plot water balance flow rates [m³/s]."""

        if balance is None:
            balance = self.calculate()

        if components is None:
            components = [
                "rain",
                "infiltration_rate_simple",
                "1d_in",
                "1d_out",
                "2d_in",
                "2d_out",
                "2d__1d_2d_flow_out",
                "d_2d_vol",
                "d_1d_vol",
            ]

        data = balance[components].copy()

        # Remove components that are completely zero
        data = data.loc[:, (data != 0).any(axis=0)]

        # Seconds -> hours
        time_hours = data.index.to_numpy() / 3600

        fig, ax = plt.subplots(figsize=(12, 6))

        for column in data.columns:
            ax.plot(
                time_hours,
                data[column],
                label=column,
            )

        ax.axhline(0, linewidth=0.8)

        ax.set_xlabel("Time [h]")
        ax.set_ylabel("Flow [m³/s]")
        ax.set_title("Water balance")

        ax.legend(
            loc="upper left",
            bbox_to_anchor=(1.02, 1),
        )

        fig.tight_layout()

        return fig, ax

    def export(self, output_folder):
        """Export water balance time series and total volumes to CSV."""

        output_folder = Path(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Water balance through time [m³/s]
        balance = self.calculate()

        balance.to_csv(
            output_folder / "water_balance_timeseries.csv",
            index=True,
            index_label="time_s",
        )

        # Total volume per component [m³]
        volumes = self.calculate_volumes(balance)

        volumes.rename("volume_m3").to_csv(
            output_folder / "water_balance_volumes.csv",
            index=True,
            index_label="component",
        )

        check = self.check_balance(volumes)

        check.rename("value").to_csv(
            output_folder / "water_balance_check.csv",
            index=True,
            index_label="metric",
        )

        return {
            "timeseries": output_folder / "water_balance_timeseries.csv",
            "volumes": output_folder / "water_balance_volumes.csv",
            "check": output_folder / "water_balance_check.csv",
        }


# %%
import os

from hhnk_threedi_tools import Folders

paths = {
    r"H:\02.modellen\bergen_noord_huidig_situatie_JA": "water_berging_JA.gpkg",
    r"H:\02.modellen\bergen_noord_variant_1_JA": "waterberging_v1.shp",
    r"H:\02.modellen\bergen_noord_variant_2_JA": "waterberging_v2.shp",
    r"H:\02.modellen\bergen_noord_variant_3_JA": "waterberging_v3.shp",
}

for path, waterbergin_polygon in paths.items():
    folder = Folders(path)
    polygon_gdf = gpd.read_file(folder.source_data.path / waterbergin_polygon)
    batch_path = folder.threedi_results.batch.path
    batch_folders = os.listdir(batch_path)
    for results in batch_folders:
        downloads_path = batch_path / results / "01_downloads"
        output_raster_path = batch_path / results / "02_output_rasters"
        downloads = os.listdir(downloads_path)
        for download in downloads:
            scenario_result_path = downloads_path / download
            output_path = output_raster_path / download / f"waterbalance_{download}"
            if not os.path.isdir(scenario_result_path):
                continue
            if os.path.exists(output_path):
                continue
            self = WaterBalance(threedi_result=hrt.ThreediResult(scenario_result_path), polygon_gdf=polygon_gdf)
            self.export(output_path)
            print(f"scenario {folder.name} / {download} done")
# %%
