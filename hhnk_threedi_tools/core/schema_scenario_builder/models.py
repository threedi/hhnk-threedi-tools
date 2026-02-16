# %%
"""Models for tables in Rana schematisation.
The scenario settings can update these values.
The models are based on the spreadsheet provided by https://docs.ranawaterintelligence.com/h_schema_300.html
"""

from pathlib import Path
from typing import ClassVar, Optional

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
from flask import json
from pydantic import BaseModel, ConfigDict, Field

logger = hrt.logging.get_logger(__name__)


class BaseSchematisationLayer(BaseModel):
    """Base class for schematisation layers. Provides methods to load from and write to a GPKG file."""

    layer_name: ClassVar[str]

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    @classmethod
    def load(cls, gpkg_path: Path) -> "BaseSchematisationLayer":
        gdf = pd.DataFrame(gpd.read_file(gpkg_path, layer=cls.layer_name))
        if gdf.empty:
            raise ValueError(f"No rows found in layer '{cls.layer_name}' of {gpkg_path}")

        row = {k: None if isinstance(v, float) and pd.isna(v) else v for k, v in gdf.iloc[0].to_dict().items()}
        return cls(**row)

    def write_to_gpkg(self, gpkg_path: Path, layer: str = "model_settings", idx: int = 0) -> None:
        gdf = gpd.read_file(gpkg_path, layer=layer)
        if gdf.empty:
            raise ValueError(f"No rows found in layer '{layer}' of {gpkg_path}")

        data = self.model_dump()  # Convert pydantic model to dict
        for key, value in data.items():
            if key in gdf.columns:
                gdf.at[idx, key] = value
        gdf.to_file(gpkg_path, layer=layer, driver="GPKG")


class InitialConditions(BaseSchematisationLayer):
    """Pydantic model for initial_conditions table schema."""

    layer_name: ClassVar[str] = "initial_conditions"

    # Layer fields
    id: Optional[int] = None
    initial_groundwater_level: Optional[float] = None
    initial_groundwater_level_aggregation: Optional[int] = None
    initial_groundwater_level_file: Optional[str] = Field(None, min_length=1, max_length=255)
    initial_water_level: Optional[float] = None
    initial_water_level_aggregation: Optional[int] = None
    initial_water_level_file: Optional[str] = Field(None, min_length=1, max_length=255)


class ModelSettings(BaseSchematisationLayer):
    """Pydantic model for model_settings table schema."""

    layer_name: ClassVar[str] = "model_settings"

    # Layer fields
    id: Optional[int] = None
    calculation_point_distance_1d: Optional[float] = None
    dem_file: Optional[str] = Field(..., min_length=1, max_length=255)
    embedded_cutoff_threshold: Optional[float] = None
    epsg_code: Optional[int] = None
    friction_averaging: Optional[int] = None
    friction_coefficient: Optional[float] = None
    friction_coefficient_file: Optional[str] = None
    friction_type: Optional[int] = None
    manhole_aboveground_storage_area: Optional[float] = None
    max_angle_1d_advection: Optional[float] = None
    maximum_table_step_size: Optional[float] = None
    minimum_cell_size: Optional[float] = None
    nr_grid_levels: Optional[int] = None
    node_open_water_detection: Optional[int] = None
    minimum_table_step_size: Optional[float] = None
    table_step_size_1d: Optional[float] = None
    use_1d_flow: Optional[bool] = None
    use_2d_flow: Optional[bool] = None
    use_2d_rain: Optional[int] = None
    use_groundwater_flow: Optional[bool] = None
    use_groundwater_storage: Optional[bool] = None
    use_interception: Optional[bool] = None
    use_interflow: Optional[bool] = None
    use_simple_infiltration: Optional[bool] = None
    use_vegetation_drag_2d: Optional[bool] = None


class SimpleInfiltration(BaseSchematisationLayer):
    """Pydantic model for simple_infiltration table schema."""

    layer_name: ClassVar[str] = "simple_infiltration"

    # Layer fields
    id: Optional[int] = None
    infiltration_rate: Optional[float] = None
    infiltration_rate_file: Optional[str] = Field(None, min_length=1, max_length=255)
    infiltration_surface_option: Optional[int] = None
    max_infiltration_volume: Optional[float] = None
    max_infiltration_volume_file: Optional[str] = None


class SimulationTemplateSettings(BaseSchematisationLayer):
    """Pydantic model for simulation_template_settings table schema."""

    layer_name: ClassVar[str] = "simulation_template_settings"

    # Layer fields
    id: Optional[int] = None
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    use_0d_inflow: Optional[bool] = None
    use_structure_control: Optional[bool] = None


class TimeStepSettings(BaseSchematisationLayer):
    """Pydantic model for time_step_settings table schema."""

    layer_name: ClassVar[str] = "time_step_settings"

    # Layer fields
    id: Optional[int] = None
    max_time_step: Optional[float] = None
    min_time_step: Optional[float] = None
    output_time_step: Optional[float] = None
    time_step: Optional[float] = None
    use_time_step_stretch: Optional[bool] = None


LAYER_MAP = {
    "initial_conditions": InitialConditions,
    "model_settings": ModelSettings,
    "simple_infiltration": SimpleInfiltration,
    "simulation_template_settings": SimulationTemplateSettings,
    "time_step_settings": TimeStepSettings,
}

# %%
if __name__ == "__main__":
    from tests.config import FOLDER_TEST

    folder = FOLDER_TEST
    gpkg_path = folder.model.schema_base.database
    model_settings = ModelSettings.load(gpkg_path)
    simple_infiltration = SimpleInfiltration.load(gpkg_path)

    with folder.model.schematisation_scenarios.open(encoding="utf-8") as f:
        scenarios = json.load(f)

    scenario_name = "0d1d_check"
    scenario = scenarios[scenario_name]

    for layer_name, values in scenario.items():
        cls = LAYER_MAP.get(layer_name)
        if not cls:
            continue

        model = cls.load(gpkg_path)  # load current values from gpkg

        # update only fields present in the JSON
        current = model.model_dump()  # pydantic v2
        for k, v in values.items():
            if k in current:
                logger.info(f"Updating {layer_name}.{k} from {current[k]} to {v}")
                setattr(model, k, v)  # validate on assignment (model_config.validate_assignment=True)
        # model.write_to_gpkg(gpkg_path, layer=layer_name) # write back to gpkg

    # model_settings.write_to_gpkg(Path("example.gpkg"), layer="model_settings")

    # loaded_settings = ModelSettings.load_from_gpkg(Path("example.gpkg"), layer="model_settings")
    # print(loaded_settings)
