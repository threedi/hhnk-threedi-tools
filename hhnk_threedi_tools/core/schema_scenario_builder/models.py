# %%
"""Models for scenario settings."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class Scenario(BaseModel):
    id: int
    schematisation_name: str
    name: str
    dem_file: str

    frict_coef_file: Optional[str] = None
    infiltration_rate_file: Optional[str] = None
    max_infiltration_capacity_file: Optional[str] = None
    initial_waterlevel_file: Optional[str] = None
    water_level_ini_type: Optional[str] = None

    display_name: Optional[str] = None
    output_time_step: int
    use_2d_rain: bool
    kmax: int
    control_group_id: Optional[int] = None
    use_2d_flow: bool
    simple_infiltration_settings_id: Optional[int] = None
    use_0d_inflow: bool
    infiltration_rate: Optional[float] = None
    infiltration_surface_option: Optional[int] = None


class ScenarioDefaults(BaseModel):
    """Default settings for schematisations."""

    advection_1d: int
    advection_2d: int
    dem_obstacle_detection: int
    dem_obstacle_height: int
    dist_calc_points: int
    embedded_cutoff_threshold: float
    epsg_code: int
    flooding_threshold: float
    frict_avg: float
    frict_coef: float
    frict_type: int
    grid_space: int
    groundwater_settings_id: Optional[int]
    guess_dams: int
    initial_groundwater_level: float
    initial_groundwater_level_file: Optional[str]
    initial_groundwater_level_type: int
    initial_waterlevel: float
    initial_waterlevel_file: Optional[str]
    interflow_settings_id: Optional[int]
    manhole_storage_area: float
    max_angle_1d_advection: float
    max_interception: float
    max_interception_file: Optional[str]
    maximum_sim_time_step: int
    minimum_sim_time_step: float
    nr_timesteps: int
    numerical_settings_id: int
    sim_time_step: int
    start_date: datetime
    start_time: Optional[str]
    table_step_size: float
    table_step_size_1d: Optional[float]
    table_step_size_volume_2d: Optional[float]
    timestep_plus: float
    use_1d_flow: int
    wind_shielding_file: Optional[str]


# %%
