"""
constants.py
------------
Layer name mappings for the two supported 3Di schematisation types:
- RANA:   newer format used by the RANA toolchain
- THREEDI: classic format used by the 3Di schematisation builder
"""

from enum import Enum


class SchematisationType(Enum):
    RANA = "rana"
    THREEDI = "threedi"


# ---------------------------------------------------------------------------
# RANA layer names
# ---------------------------------------------------------------------------

LAYER_NAMES_RANA: dict[str, str] = {
    "connection_node": "connection_node",
    "pipe": "pipe",
    "weir": "weir",
    "orifice": "orifice",
    "culvert": "culvert",
    "cross_section_location": "cross_section_location",
    "channel": "channel",
    "pump_map": "pump_map",
    "pump": "pump",
    "boundary_condition_1d": "boundary_condition_1d",
    "boundary_condition_2d": "boundary_condition_2d",
    "lateral_1d": "lateral_1d",
    "lateral_2d": "lateral_2d",
    "surface_map": "surface_map",
    "surface": "surface",
    "obstacle": "obstacle",
    "potential_breach": "potential_breach",
    "exchange_line": "exchange_line",
    "grid_refinement_line": "grid_refinement_line",
    "grid_refinement_area": "grid_refinement_area",
}

# ---------------------------------------------------------------------------
# 3Di (classic) layer names
# ---------------------------------------------------------------------------

LAYER_NAMES_THREEDI: dict[str, str] = {
    "connection_node": "connection_node",
    "pipe": "pipe",
    "weir": "weir",
    "orifice": "orifice",
    "culvert": "culvert",
    "cross_section_location": "cross_section_location",
    "channel": "channel",
    "pump_map": "pumpstation_map",
    "pump": "pumpstation",
    "boundary_condition_1d": "1d_boundary_condition",
    "boundary_condition_2d": "2d_boundary_condition",
    "lateral_1d": "1d_lateral",
    "lateral_2d": "2d_lateral",
    "surface_map": "impervious_surface_map",
    "surface": "impervious_surface",
    "obstacle": "linear_obstacle",
    "potential_breach": "potential_breach",
    "exchange_line": "exchange_line",
    "grid_refinement_line": "grid_refinement",
    "grid_refinement_area": "grid_refinement_area",
}

# ---------------------------------------------------------------------------
# Lookup by SchematisationType
# ---------------------------------------------------------------------------

LAYER_NAMES: dict[SchematisationType, dict[str, str]] = {
    SchematisationType.RANA: LAYER_NAMES_RANA,
    SchematisationType.THREEDI: LAYER_NAMES_THREEDI,
}
