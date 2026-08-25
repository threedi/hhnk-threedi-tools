"""Constants and configuration for the standalone 3Di water balance."""

from threedigrid_builder.constants import LineType, NodeType

NO_ENDPOINT_ID: int = -9999


NODE_TYPES_1D: set[NodeType] = {
    NodeType.NODE_1D_NO_STORAGE,
    NodeType.NODE_1D_STORAGE,
    NodeType.NODE_1D_BOUNDARIES,
}

NODE_TYPES_2D: set[NodeType] = {
    NodeType.NODE_2D_OPEN_WATER,
    NodeType.NODE_2D_BOUNDARIES,
}

NODE_TYPES_2D_GROUNDWATER: set[NodeType] = {
    NodeType.NODE_2D_GROUNDWATER_BOUNDARIES,
    NodeType.NODE_2D_GROUNDWATER,
}

NODE_TYPES_BOUNDARIES: set[NodeType] = {
    NodeType.NODE_1D_BOUNDARIES,
    NodeType.NODE_2D_BOUNDARIES,
}


LINE_TYPES_1D: set[LineType] = {
    LineType.LINE_1D_EMBEDDED,
    LineType.LINE_1D_ISOLATED,
    LineType.LINE_1D_CONNECTED,
    LineType.LINE_1D_LONG_CRESTED,
    LineType.LINE_1D_SHORT_CRESTED,
    LineType.LINE_1D_DOUBLE_CONNECTED,
}

LINE_TYPES_1D2D: set[LineType | int] = {
    LineType.LINE_1D2D_SINGLE_CONNECTED_CLOSED,
    LineType.LINE_1D2D_SINGLE_CONNECTED_OPEN_WATER,
    LineType.LINE_1D2D_DOUBLE_CONNECTED_CLOSED,
    LineType.LINE_1D2D_DOUBLE_CONNECTED_OPEN_WATER,
    LineType.LINE_1D2D_POSSIBLE_BREACH,
    LineType.LINE_1D2D_ACTIVE_BREACH,
    LineType.LINE_1D2D_GROUNDWATER,
    58,
}


NODE_TYPE_1D_BOUNDARY: int = NodeType.NODE_1D_BOUNDARIES.value
NODE_TYPE_2D_BOUNDARY: int = NodeType.NODE_2D_BOUNDARIES.value


INPUT_SERIES: list[tuple[str, int]] = [
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

SERIES_INDEX: dict[str, int] = dict(INPUT_SERIES)


EXTERNAL_COMPONENTS: tuple[str, ...] = (
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
)


STORAGE_COMPONENTS: tuple[str, ...] = (
    "d_2d_vol",
    "d_1d_vol",
    "d_2d_groundwater_vol",
)