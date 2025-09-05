import json
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources


class HyDAMO_Fixer:
    """Class to fix HyDAMO data."""

    def __init__(self, hydamo_file_path: Path, validation_result_path: Path, logger) -> None:
        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.hydamo_file = gpd.read_file(hydamo_file_path)
        self.validation_result = gpd.read_file(validation_result_path)

        self.config_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "FixConfig.json")
        with open(self.config_path) as f:
            self.fix_config = json.load(f)

    def _check_if_fix_needed(self, layer) -> bool:
        """Check if fixing is needed based on validation results."""
        for obj in self.fix_config["objects"]:
            if obj["object"] == layer:
                for rule in obj["validation_rule"]:
                    column = f"validate_{rule['validation_rule_id']}_{rule['result_variable']}"
                    if column in self.validation_result.columns:
                        # select rows where validation failed and save related codes in list
                        failed_rows = self.validation_result[
                            (self.validation_result["layer"] == layer) & (self.validation_result[column] == False)
                        ]
                        if not failed_rows.empty:
                            failed_codes = failed_rows["code"].tolist()
                            self.logger.info(f"Issues found for layer {layer} and rule {rule['result_variable']}")
                            return True, failed_codes
                        else:
                            failed_codes = []
                            self.logger.warning(
                                f"No issues found for layer {layer} and rule {rule['result_variable']}"
                            )
                            return False, failed_codes


if __name__ == "__main__":
    hydamo_file_path = Path("path/to/hydamo_file.geojson")
    validation_result_path = Path("path/to/validation_result.geojson")
    fixer = HyDAMO_Fixer(hydamo_file_path, validation_result_path, logger=None)

    pass
    # TODO add example usage
