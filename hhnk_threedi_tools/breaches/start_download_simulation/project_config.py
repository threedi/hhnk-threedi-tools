"""This module defines the configuration used to run a 3Di breach simulation.
It keeps project paths, model inputs, and validation logic in one place so
the same settings can be reused by scripts and workflows preparing a breach
simulation.
The ProjectConfig object stores the output directories, the selected breach
identifiers, the organisation name, and the chosen simulation settings.
Validation checks the requested duration, IDs, kering type, and closure
configuration before the run is started.
This keeps the project setup in a single place and helps prevent invalid
requests to the external 3Di API.
The module is intended for setup files and operational scripts that need a
consistent configuration object for yearly or project-specific runs.
The example at the bottom shows a typical regionale breach setup with
cmp enabled.
"""

# %%
import os
import shutil
from pathlib import Path
from typing import Any, Sequence


# %%
class ProjectConfig:
    """Configuration for a 3Di breach simulation project."""

    def __init__(
        self,
        project_folder: str | Path,
        model_folder: str | Path,
        organisation_name: str,
        filter_id: list,
        kering_type: str,
        simulation_duration: float,
        closure_method: str = "none",
        closure_time: float | None = None,
        structures_info: dict[int, str] | None = None,
        testresult_folder: str | Path | None = None,
    ) -> None:
        """Create and validate the configuration for a breach simulation."""
        # Project paths
        self.project_folder: Path = Path(project_folder)
        self.model_folder: Path = Path(model_folder)

        self.results_folder: Path = self.project_folder / "results_output"
        self.ldo_folder: Path = self.project_folder / "ldo_structuur"
        self.excel_path: Path = self.project_folder / "simulations_id.xlsx"

        # Create project folders
        self.project_folder.mkdir(parents=True, exist_ok=True)
        self.ldo_folder.mkdir(parents=True, exist_ok=True)
        self.results_folder.mkdir(parents=True, exist_ok=True)

        # Copy Excel template if it doesn't exist
        if not self.excel_path.exists():
            excel_template = Path(__file__).parent / "simulations_id.xlsx"
            shutil.copy2(excel_template, self.excel_path)

        # Simulation configuration
        self.organisation_name: str = organisation_name
        self.filter_id: list[int] = list(filter_id)
        self.kering_type: str = kering_type
        self.simulation_duration: int = simulation_duration  # days

        # Closure configuration
        self.closure_method: str = closure_method
        self.closure_time: float | None = closure_time  # hours
        self.structures_info: dict[int, str] = dict(structures_info) if structures_info is not None else {}
        self.testresult_folder: Path | None = Path(testresult_folder) if testresult_folder is not None else None

        # Validate simulation settings
        if self.simulation_duration <= 0:
            raise ValueError("Simulation duration must be greater than zero.")

        if not self.filter_id:
            raise ValueError("At least one breach ID must be selected.")

        if len(self.filter_id) != len(set(self.filter_id)):
            raise ValueError("Duplicate breach IDs found.")

        # Validate kering type
        if self.kering_type not in ["primaire", "regionale"]:
            raise ValueError(f"Invalid kering type: {self.kering_type}")

        # Validate closure method
        if self.closure_method not in ["none", "cmp", "upstream"]:
            raise ValueError(f"Invalid closure method: {self.closure_method}")

        # Closure only applies to regionale keringen
        if self.closure_method != "none":
            if self.kering_type != "regionale":
                raise ValueError("Closure is only supported for regionale keringen.")

            if self.closure_time is None:
                raise ValueError("Closure time must be specified.")

            if not 0 <= self.closure_time < self.simulation_duration * 24:
                raise ValueError("Closure time must be within simulation duration.")

        elif self.closure_time is not None:
            raise ValueError("Closure time is not needed when closure_method is none.")

        # compartimetering settings
        if self.closure_method == "cmp":
            if not self.structures_info:
                raise ValueError("cmp requires structures_info.")

        elif self.structures_info:
            raise ValueError("structures_info is only used for cmp.")

        # Upstream closure settings
        if self.closure_method == "upstream":
            if self.testresult_folder is None:
                raise ValueError("Upstream closure requires testresult_folder.")

        # Convert time units for 3Di API
        self.simulation_duration_seconds: int = round(self.simulation_duration * 86400)

        self.closure_time_seconds: int | None = (
            round(self.closure_time * 3600) if self.closure_time is not None else None
        )


# %%
if __name__ == "__main__":
    project_folder = r"H:\03.resultaten\RWS_Test_v2"
    model_folder = r"H:\02.modellen\RegionalFloodModel - deelmodel Schermer Laag Zuid"

    # options: "Hoogheemraadschap Hollands Noorderkwartier" OR "BWN HHNK"
    organisation_name = "BWN HHNK"

    # Breach id
    filter_id = [100048, 100021]

    # options: regionalee, primaire
    kering_type = "regionale"

    # days
    simulation_duration = 20

    # options: cmp, None, upstream
    closure_method = "cmp"

    # hours
    closure_time = 12

    # structure info
    structures_info = {
        632: "v2_weir",
        19: "v2_orifice",
        316: "v2_orifice",
    }

    project: ProjectConfig = ProjectConfig(
        project_folder=project_folder,
        model_folder=model_folder,
        organisation_name=organisation_name,
        filter_id=[100048, 100021],
        kering_type="regionale",
        simulation_duration=20,
        closure_method="cmp",
        closure_time=12,
        structures_info={
            632: "v2_weir",
            19: "v2_orifice",
            316: "v2_orifice",
        },
    )

# %%
