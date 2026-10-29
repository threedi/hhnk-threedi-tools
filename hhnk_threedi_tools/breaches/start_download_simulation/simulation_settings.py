"""Default HHNK settings for 3Di breach simulations.

This module contains the standard configuration values used when setting up a
breach simulation. These defaults are intentionally centralized so that project
scripts can reuse the same solver behaviour, time stepping, and output
aggregation for comparable analyses.

The values are split into clear groups: general timing, numerical stability,
physical process activation, time-step controls, and output aggregation. The
project-specific values such as simulation duration and selected breach IDs are
kept in ProjectConfig and merged with these defaults at runtime.

Keeping this file declarative makes it easier to review, update, and explain the
simulation setup to other project members or downstream tools that depend on the
same operational defaults.
"""

import datetime

# Start time of the simulation window. This is kept fixed so all scripts use the
# same baseline reference time when preparing result files and metadata.
START_DATETIME = datetime.datetime(2000, 1, 1, 0, 0)

# Output interval for exported time series. The 3Di API expects this in seconds.
OUTPUT_TIMESTEP = 900  # seconds

# Numerical solver settings: these are the stability and convergence controls used
# for the 1D/2D flow calculation. They tune Newton iterations, CFL checks, and
# limiter behaviour to keep the breach simulation numerically stable.
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

# Physical process switches for advection in the 1D and 2D domains.
PHYSICAL_SETTINGS = {"use_advection_1d": 1, "use_advection_2d": 1}

# Adaptive time-step configuration for the flow model. The values define the
# minimum and maximum allowable step size as well as the output cadence.
TIME_STEP_SETTINGS = {
    "time_step": 5,
    "min_time_step": 0.01,
    "max_time_step": 5,
    "use_time_step_stretch": False,
    "output_time_step": OUTPUT_TIMESTEP,
}


# Aggregation definitions for model outputs. These determine which variables are
# exported and how values are summarised (average, maximum, cumulative, etc.)
# over the configured output interval.
AGGREGATION_SETTINGS = [
    # Standard HHNK aggregations used for common hydrodynamic diagnostics.
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
