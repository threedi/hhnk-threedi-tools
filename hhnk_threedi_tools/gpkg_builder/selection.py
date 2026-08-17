"""Spatial selection functions for the 3Di water balance."""

import geopandas as gpd
import numpy as np
import pandas as pd
from threedigrid_builder.constants import LineType

from .config import (
    LINE_TYPES_1D,
    LINE_TYPES_1D2D,
    NO_ENDPOINT_ID,
    NODE_TYPE_1D_BOUNDARY,
    NODE_TYPES_1D,
    NODE_TYPES_2D,
    NODE_TYPES_2D_GROUNDWATER,
    NODE_TYPES_BOUNDARIES,
)


def select_points(aggregate_grid, polygon):
    """Select calculation nodes inside the water balance polygon."""

    nodes = aggregate_grid.nodes

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
    # polygon.contains(point.geometry())
    #
    # GeoPandas/Shapely equivalent for points:
    nodes_gdf = nodes_gdf[nodes_gdf.geometry.within(polygon)]

    for node_type, category in node_type_map.items():
        ids = nodes_gdf.loc[
            nodes_gdf["node_type"] == node_type,
            "id",
        ].tolist()

        point_selection[category].extend(ids)

    return point_selection


def select_lines_and_pumps(aggregate_grid, grid, polygon, node_ids, x2d_surf_range, y2d_surf_range, vert_flow_range):

    lines = aggregate_grid.lines

    valid = lines.id != 0

    line_ids = lines.id[valid]
    line_types = lines.kcu[valid]

    node_ids_inside = np.concatenate(
        [
            np.asarray(node_ids["1d"], dtype=int),
            np.asarray(node_ids["2d"], dtype=int),
            np.asarray(node_ids["2d_groundwater"], dtype=int),
        ]
    )

    node_ids_inside = np.concatenate(
        [
            np.asarray(node_ids["1d"], dtype=int),
            np.asarray(node_ids["2d"], dtype=int),
            np.asarray(node_ids["2d_groundwater"], dtype=int),
        ]
    )

    # Nodes already selected inside polygon
    node_ids_inside = np.concatenate(
        [
            node_ids["1d"],
            node_ids["2d"],
            node_ids["2d_groundwater"],
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

    is_x = (crossing_2d["id"] >= x2d_surf_range.start) & (crossing_2d["id"] < x2d_surf_range.stop)

    is_y = (crossing_2d["id"] >= y2d_surf_range.start) & (crossing_2d["id"] < y2d_surf_range.stop)

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

    nodes = aggregate_grid.nodes

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

    boundary_nodes = nodes_gdf[nodes_gdf["node_type"].isin([node_type.value for node_type in NODE_TYPES_BOUNDARIES])]

    boundary_nodes = boundary_nodes[boundary_nodes.geometry.within(polygon)]

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
                if boundary_type == NODE_TYPE_1D_BOUNDARY.value:
                    line_selection["1d_bound_in"].append(int(line_id))

                else:
                    line_selection["2d_bound_in"].append(int(line_id))

            # boundary is end node -> boundary outflow
            else:
                if boundary_type == NODE_TYPE_1D_BOUNDARY.value:
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

    pumps = aggregate_grid.pumps

    # Nodes inside polygon
    nodes = aggregate_grid.nodes

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
