"""
Standard HHNK settings for 3Di breach simulations.

Project-specific settings, such as simulation duration,
are defined in ProjectConfig.
"""

import datetime

# General simulation settings
START_DATETIME = datetime.datetime(2000, 1, 1, 0, 0)
OUTPUT_TIMESTEP = 900  # seconds

NUMERICAL_SETTING = {
    "pump_implicit_ratio": 1,
    "cfl_strictness_factor_1d": 1,
    "cfl_strictness_factor_2d": 1,
    "convergence_eps": 1e-5,
    "convergence_cg": 1e-9,
    "flow_direction_threshold": 1e-6,
    "friction_shallow_water_depth_correction": 0,
    "general_numerical_threshold": 1e-8,
    "time_integration_method": 0,
    "limiter_waterlevel_gradient_1d": 1,
    "limiter_waterlevel_gradient_2d": 1,
    "limiter_slope_crossectional_area_2d": 0,
    "limiter_slope_friction_2d": 0,
    "max_non_linear_newton_iterations": 20,
    "max_degree_gauss_seidel": 7,
    "min_friction_velocity": 1e-5,
    "min_surface_area": 1e-8,
    "use_preconditioner_cg": 1,
    "preissmann_slot": 0,
    "limiter_slope_thin_water_layer": 0.01,
    "use_of_cg": 20,
    "use_nested_newton": True,
    "flooding_threshold": 1e-5,
}

PHYSICAL_SETTINGS = {"use_advection_1d": 1, "use_advection_2d": 1}


TIME_STEP_SETTINGS = {
    "time_step": 5,
    "min_time_step": 0.01,
    "max_time_step": 5,
    "use_time_step_stretch": False,
    "output_time_step": OUTPUT_TIMESTEP,
}


# Aggregation settings
AGGREGATION_SETTINGS = [
    # Standard HHNK aggregations
    {"name": "", "flow_variable": "discharge", "method": "avg", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "water_level", "method": "avg", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "water_level", "method": "max", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "discharge", "method": "max", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "flow_velocity", "method": "avg", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "flow_velocity", "method": "max", "interval": OUTPUT_TIMESTEP},
    # Additional aggregations for the Water Balance Tool
    {"name": "", "flow_variable": "pump_discharge", "method": "cum", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "lateral_discharge", "method": "cum", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "simple_infiltration", "method": "cum", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "rain", "method": "cum", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "leakage", "method": "cum", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "interception", "method": "current", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "discharge", "method": "cum", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "discharge", "method": "cum_negative", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "discharge", "method": "cum_positive", "interval": OUTPUT_TIMESTEP},
    {"name": "", "flow_variable": "volume", "method": "current", "interval": OUTPUT_TIMESTEP},
    {
        "name": "",
        "flow_variable": "surface_source_sink_discharge",
        "method": "cum_positive",
        "interval": OUTPUT_TIMESTEP,
    },
    {
        "name": "",
        "flow_variable": "surface_source_sink_discharge",
        "method": "cum_negative",
        "interval": OUTPUT_TIMESTEP,
    },
]
