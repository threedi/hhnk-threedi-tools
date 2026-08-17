# %%
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from threedigrid_builder.constants import LineType, NodeType

NODE_TYPES_1D = {
    NodeType.NODE_1D_NO_STORAGE,
    NodeType.NODE_1D_STORAGE,
    NodeType.NODE_1D_BOUNDARIES,
}

NODE_TYPES_2D = {
    NodeType.NODE_2D_OPEN_WATER,
    NodeType.NODE_2D_BOUNDARIES,
}

NODE_TYPES_2D_GROUNDWATER = {
    NodeType.NODE_2D_GROUNDWATER_BOUNDARIES,
    NodeType.NODE_2D_GROUNDWATER,
}

NO_ENDPOINT_ID = -9999

LINE_TYPES_1D = {
    LineType.LINE_1D_EMBEDDED,
    LineType.LINE_1D_ISOLATED,
    LineType.LINE_1D_CONNECTED,
    LineType.LINE_1D_LONG_CRESTED,
    LineType.LINE_1D_SHORT_CRESTED,
    LineType.LINE_1D_DOUBLE_CONNECTED,
}

LINE_TYPES_1D2D = {
    LineType.LINE_1D2D_SINGLE_CONNECTED_CLOSED,
    LineType.LINE_1D2D_SINGLE_CONNECTED_OPEN_WATER,
    LineType.LINE_1D2D_DOUBLE_CONNECTED_CLOSED,
    LineType.LINE_1D2D_DOUBLE_CONNECTED_OPEN_WATER,
    LineType.LINE_1D2D_POSSIBLE_BREACH,
    LineType.LINE_1D2D_ACTIVE_BREACH,
    LineType.LINE_1D2D_GROUNDWATER,
    58,
}

NODE_TYPES_BOUNDARIES = {
    NodeType.NODE_1D_BOUNDARIES,
    NodeType.NODE_2D_BOUNDARIES,
}

INPUT_SERIES = [
    ("2d_in", 0),
    ("2d_out", 1),
    ("1d_in", 2),
    ("1d_out", 3),
    ("2d_bound_in", 4),
    ("2d_bound_out", 5),
    ("1d_bound_in", 6),
    ("1d_bound_out", 7),
    ("1d__1d_2d_flow_in", 8),
    ("1d__1d_2d_flow_out", 9),
    ("1d__1d_2d_exch_in", 10),
    ("1d__1d_2d_exch_out", 11),
    ("pump_in", 12),
    ("pump_out", 13),
    ("rain", 14),
    ("infiltration_rate_simple", 15),
    ("lat_2d", 16),
    ("lat_1d", 17),
    ("d_2d_vol", 18),
    ("d_1d_vol", 19),
    ("error_2d", 20),
    ("error_1d", 21),
    ("error_1d_2d", 22),
    ("2d_groundwater_in", 23),
    ("2d_groundwater_out", 24),
    ("d_2d_groundwater_vol", 25),
    ("leak", 26),
    ("inflow", 27),
    ("2d_vertical_infiltration_pos", 28),
    ("2d_vertical_infiltration_neg", 29),
    ("2d__1d_2d_flow_in", 30),
    ("2d__1d_2d_flow_out", 31),
    ("2d__1d_2d_exch_in", 32),
    ("2d__1d_2d_exch_out", 33),
    ("intercepted_volume", 34),
    ("q_sss", 35),
]

SERIES_INDEX = dict(INPUT_SERIES)


class WaterBalance:
    def __init__(self, threedi_result: hrt.ThreediResult, polygon_gdf: gpd.GeoDataFrame):
        self.threedi_result = threedi_result
        self.polygon_gdf = polygon_gdf

        self._set_2d_line_ranges()

        # First select nodes
        self.node_ids = self._select_points()

        # Then use those nodes to select lines/pumps
        self.flowline_ids, self.pump_ids = self._select_lines_and_pumps()

    @property
    def polygon(self):
        return self.polygon_gdf.to_crs(28992).geometry.union_all()

    @property
    def grid(self):
        return self.threedi_result.grid

    @property
    def aggregate_grid(self):
        return self.threedi_result.aggregate_grid

    @property
    def admin(self):
        return self.threedi_result.admin

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

    def _select_points(self):
        """Select calculation nodes inside the water balance polygon."""

        nodes = self.aggregate_grid.nodes

        point_selection = {
            "1d": [],
            "2d": [],
            "2d_groundwater": [],
        }

        node_type_map = {}

        node_type_map.update({n.value: "1d" for n in NODE_TYPES_1D})

        node_type_map.update({n.value: "2d" for n in NODE_TYPES_2D})

        node_type_map.update({n.value: "2d_groundwater" for n in NODE_TYPES_2D_GROUNDWATER})

        nodes_gdf = gpd.GeoDataFrame(
            {
                "id": nodes.id,
                "node_type": nodes.node_type,
            },
            geometry=gpd.points_from_xy(
                nodes.coordinates[0],
                nodes.coordinates[1],
            ),
            crs=28992,
        )

        # Same node-type filter as official WaterBalanceCalculation
        nodes_gdf = nodes_gdf[nodes_gdf["node_type"].isin(node_type_map)]

        # QGIS original:
        # self.polygon.contains(point.geometry())
        #
        # GeoPandas/Shapely equivalent for points:
        nodes_gdf = nodes_gdf[nodes_gdf.geometry.within(self.polygon)]

        for node_type, category in node_type_map.items():
            ids = nodes_gdf.loc[
                nodes_gdf["node_type"] == node_type,
                "id",
            ].tolist()

            point_selection[category].extend(ids)

        return point_selection

    def _select_lines_and_pumps(self):

        lines = self.aggregate_grid.lines

        valid = lines.id != 0

        line_ids = lines.id[valid]
        line_types = lines.kcu[valid]

        node_ids_inside = np.concatenate(
            [
                np.asarray(self.node_ids["1d"], dtype=int),
                np.asarray(self.node_ids["2d"], dtype=int),
                np.asarray(self.node_ids["2d_groundwater"], dtype=int),
            ]
        )

        node_ids_inside = np.concatenate(
            [
                np.asarray(self.node_ids["1d"], dtype=int),
                np.asarray(self.node_ids["2d"], dtype=int),
                np.asarray(self.node_ids["2d_groundwater"], dtype=int),
            ]
        )

        # Nodes already selected inside polygon
        node_ids_inside = np.concatenate(
            [
                self.node_ids["1d"],
                self.node_ids["2d"],
                self.node_ids["2d_groundwater"],
            ]
        )

        start_nodes = lines.line[0, valid]
        end_nodes = lines.line[1, valid]

        start_inside = np.isin(
            start_nodes,
            node_ids_inside,
        )

        end_inside = np.isin(
            end_nodes,
            node_ids_inside,
        )

        crosses = start_inside ^ end_inside
        internal = start_inside & end_inside

        lines_df = pd.DataFrame(
            {
                "id": line_ids,
                "line_type": line_types,
                "start_inside": start_inside,
                "end_inside": end_inside,
                "crosses": crosses,
                "internal": internal,
            }
        )

        line_selection = {
            "1d_in": [],
            "1d_out": [],
            "1d_bound_in": [],
            "1d_bound_out": [],
            "2d_in": [],
            "2d_out": [],
            "2d_bound_in": [],
            "2d_bound_out": [],
            "1d__1d_2d_flow": [],
            "2d__1d_2d_flow": [],
            "1d_2d_exch": [],
            "2d_groundwater_in": [],
            "2d_groundwater_out": [],
            "2d_vertical_infiltration": [],
        }

        # Same spatial tests as the official QGIS implementation
        # Lines crossing the polygon boundary
        crossing_lines = lines_df[lines_df["crosses"]]

        # --------------------------------------------------
        # 2D vertical infiltration
        # --------------------------------------------------

        mask = (lines_df["line_type"] == LineType.LINE_2D_VERTICAL.value) & lines_df["start_inside"]

        line_selection["2d_vertical_infiltration"] = lines_df.loc[mask, "id"].tolist()

        # --------------------------------------------------
        # Lines crossing polygon
        # --------------------------------------------------

        crossing_lines = lines_df[crosses]

        is_1d = crossing_lines["line_type"].isin([line_type.value for line_type in LINE_TYPES_1D])

        is_1d2d = crossing_lines["line_type"].isin(
            [line_type.value if hasattr(line_type, "value") else line_type for line_type in LINE_TYPES_1D2D]
        )

        outgoing = crossing_lines["start_inside"]
        incoming = crossing_lines["end_inside"]

        # 1D
        line_selection["1d_out"] = crossing_lines.loc[
            outgoing & is_1d,
            "id",
        ].tolist()

        line_selection["1d_in"] = crossing_lines.loc[
            incoming & is_1d,
            "id",
        ].tolist()

        # 1D-2D crossing polygon
        line_selection["2d__1d_2d_flow"] = crossing_lines.loc[
            outgoing & is_1d2d,
            "id",
        ].tolist()

        line_selection["1d__1d_2d_flow"] = crossing_lines.loc[
            incoming & is_1d2d,
            "id",
        ].tolist()

        # --------------------------------------------------
        # 1D-2D exchange completely inside polygon
        # --------------------------------------------------

        all_is_1d2d = lines_df["line_type"].isin(
            [line_type.value if hasattr(line_type, "value") else line_type for line_type in LINE_TYPES_1D2D]
        )

        all_is_1d2d = lines_df["line_type"].isin(
            [line_type.value if hasattr(line_type, "value") else line_type for line_type in LINE_TYPES_1D2D]
        )

        line_selection["1d_2d_exch"] = lines_df.loc[
            lines_df["internal"] & all_is_1d2d,
            "id",
        ].tolist()

        # --------------------------------------------------
        # Surface 2D flow
        # --------------------------------------------------

        crossing_2d = crossing_lines[
            crossing_lines["line_type"].isin(
                [
                    LineType.LINE_2D.value,
                    LineType.LINE_2D_OBSTACLE.value,
                ]
            )
        ].copy()

        # Coordinates are needed because official 3Di code
        # distinguishes horizontal and vertical link direction.
        crossing_2d["start_x"] = lines.line_coords[0, crossing_2d["id"].to_numpy()]
        crossing_2d["start_y"] = lines.line_coords[1, crossing_2d["id"].to_numpy()]
        crossing_2d["end_x"] = lines.line_coords[2, crossing_2d["id"].to_numpy()]
        crossing_2d["end_y"] = lines.line_coords[3, crossing_2d["id"].to_numpy()]

        is_x = (crossing_2d["id"] >= self.x2d_surf_range.start) & (crossing_2d["id"] < self.x2d_surf_range.stop)

        is_y = (crossing_2d["id"] >= self.y2d_surf_range.start) & (crossing_2d["id"] < self.y2d_surf_range.stop)

        eastward = crossing_2d["end_x"] > crossing_2d["start_x"]
        northward = crossing_2d["end_y"] > crossing_2d["start_y"]

        start_inside = crossing_2d["start_inside"]
        end_inside = crossing_2d["end_inside"]

        # Horizontal 2D links
        mask = is_x & ((start_inside & ~eastward) | (end_inside & eastward))
        line_selection["2d_in"].extend(crossing_2d.loc[mask, "id"].tolist())

        mask = is_x & ((start_inside & eastward) | (end_inside & ~eastward))
        line_selection["2d_out"].extend(crossing_2d.loc[mask, "id"].tolist())

        # Vertical 2D links
        mask = is_y & ((start_inside & ~northward) | (end_inside & northward))
        line_selection["2d_in"].extend(crossing_2d.loc[mask, "id"].tolist())

        mask = is_y & ((start_inside & northward) | (end_inside & ~northward))
        line_selection["2d_out"].extend(crossing_2d.loc[mask, "id"].tolist())

        # --------------------------------------------------
        # --------------------------------------------------
        # Boundary nodes
        # --------------------------------------------------

        nodes = self.aggregate_grid.nodes

        nodes_gdf = gpd.GeoDataFrame(
            {
                "id": nodes.id,
                "node_type": nodes.node_type,
            },
            geometry=gpd.points_from_xy(
                nodes.coordinates[0],
                nodes.coordinates[1],
            ),
            crs=28992,
        )

        boundary_nodes = nodes_gdf[
            nodes_gdf["node_type"].isin([node_type.value for node_type in NODE_TYPES_BOUNDARIES])
        ]

        boundary_nodes = boundary_nodes[boundary_nodes.geometry.within(self.polygon)]

        line_start_node = lines.line[0]
        line_end_node = lines.line[1]

        for _, boundary in boundary_nodes.iterrows():
            boundary_id = boundary["id"]
            boundary_type = boundary["node_type"]

            connected_indices = np.where((line_start_node == boundary_id) | (line_end_node == boundary_id))[0]

            for line_id in connected_indices:
                # Same convention as official WaterBalanceCalculation:
                # boundary is start node -> boundary inflow
                if line_start_node[line_id] == boundary_id:
                    if boundary_type == NodeType.NODE_1D_BOUNDARIES.value:
                        line_selection["1d_bound_in"].append(int(line_id))

                    else:
                        line_selection["2d_bound_in"].append(int(line_id))

                # boundary is end node -> boundary outflow
                else:
                    if boundary_type == NodeType.NODE_1D_BOUNDARIES.value:
                        line_selection["1d_bound_out"].append(int(line_id))

                    else:
                        line_selection["2d_bound_out"].append(int(line_id))
        pump_selection = {
            "in": [],
            "out": [],
        }

        # --------------------------------------------------
        # Pumps
        # --------------------------------------------------

        pumps = self.aggregate_grid.pumps

        # Nodes inside polygon
        nodes = self.aggregate_grid.nodes

        # Ignore dummy pump id 0
        valid_pumps = pumps.id != 0

        pump_ids = pumps.id[valid_pumps]
        pump_start = pumps.node1_id[valid_pumps]
        pump_end = pumps.node2_id[valid_pumps]

        start_inside = np.isin(
            pump_start,
            node_ids_inside,
        )

        end_inside = np.isin(
            pump_end,
            node_ids_inside,
        )

        has_endpoint = pump_end != NO_ENDPOINT_ID

        # Line pump: outside -> inside
        mask = has_endpoint & ~start_inside & end_inside

        pump_selection["in"] = pump_ids[mask].tolist()

        # Line pump: inside -> outside
        mask = has_endpoint & start_inside & ~end_inside

        pump_selection["out"] = pump_ids[mask].tolist()

        # 0D pump:
        # no end node and start node inside polygon.
        # Official Water Balance always treats this as outflow.
        mask = ~has_endpoint & start_inside

        pump_selection["out"].extend(pump_ids[mask].tolist())
        return line_selection, pump_selection

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

            all_flows[SERIES_INDEX["d_2d_groundwater_vol"]] = (
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

    def calculate_volumes(self):
        """Return total volume [m³] per water balance component."""

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

    def check_balance(self):
        """Check closure of the water balance."""

        volumes = self.calculate_volumes()

        external_components = [
            "2d_in",
            "2d_out",
            "1d_in",
            "1d_out",
            "2d_bound_in",
            "2d_bound_out",
            "1d_bound_in",
            "1d_bound_out",
            "pump_in",
            "pump_out",
            "rain",
            "infiltration_rate_simple",
            "lat_2d",
            "lat_1d",
            "inflow",
            "1d__1d_2d_flow_in",
            "1d__1d_2d_flow_out",
            "2d__1d_2d_flow_in",
            "2d__1d_2d_flow_out",
            "2d_groundwater_in",
            "2d_groundwater_out",
        ]

        external_net = volumes[external_components].sum()

        storage_change = volumes["d_2d_vol"] + volumes["d_1d_vol"] + volumes["d_2d_groundwater_vol"]

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
        volumes = self.calculate_volumes()

        volumes.rename("volume_m3").to_csv(
            output_folder / "water_balance_volumes.csv",
            index=True,
            index_label="component",
        )

        check = self.check_balance()

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
            scenario_result_path =(downloads_path / download)
            output_path = output_raster_path / download /f'waterbalance_{download}'
            if not os.path.isdir(scenario_result_path):
                continue
            if os.path.exists(output_path):
                continue
            self = WaterBalance(threedi_result=hrt.ThreediResult(scenario_result_path), polygon_gdf= polygon_gdf)
            self.export(output_path)
            print(f'scenario {folder.name} / {download} done')
# %%

