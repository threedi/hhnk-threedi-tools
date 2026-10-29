from datetime import datetime
from pathlib import Path

import pandas as pd


class SimulationRegistry:
    """Store 3Di simulations in simulations_id.xlsx."""

    def __init__(self, excel_path):
        self.excel_path = Path(excel_path)

        if not self.excel_path.exists():
            raise FileNotFoundError(f"Excel not found: {self.excel_path}")

    def add_simulation(
        self,
        scenario_name,
        schematitation_name,
        schematisation_id,
        revision_number,
        model_id,
        scenario_id_rana,
    ):
        # Read Excel
        df = pd.read_excel(self.excel_path)

        # Prevent registering the same simulation twice
        if scenario_id_rana in df["scenario_id_rana"].values:
            raise ValueError(f"Simulation {scenario_id_rana} already exists.")

        # Add new simulation
        df.loc[len(df)] = {
            "scenario_name": scenario_name,
            "schematitation_name": schematitation_name,
            "schematisation_id": schematisation_id,
            "revision_number": revision_number,
            "model_id": model_id,
            "scenario_id_rana": scenario_id_rana,
            "date_simulation_created": datetime.now(),
        }

        # Save Excel
        df.to_excel(self.excel_path, index=False)

        print(f"Simulation {scenario_name} ({scenario_id_rana}) saved.")

