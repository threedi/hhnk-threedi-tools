import json
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
from HyDAMO_validator import validate_hydamo

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.resources.schematisation_builder.db_layer_mapping import DB_LAYER_MAPPING
import functions_HyDAMO_Fixer

class HyDAMO_Fixer:
    """Class to fix HyDAMO data.
    Process:
    - read HyDAMO data and validation results
    - (make summary of total validation results? and show to user?)
    - validate specific layer before process of fixing (FUNCTION TO BE ADDED)
    - (make summary of specific layer validation results? and show to user?)
    - for each defined validation fix/rule check if fix is needed (based on validation results) (FUNCTION: _check_if_fix_needed)
    - give back rows that need to be fixed, for now in gdf format? (FUNCTION: select_features_to_fix)
    - show those row to user? (in Qgis, but will be done outside this class)
    - if needed: ask user for confirmation to fix automatically (do this step later)
    - if confirmed: apply fix (FUNCTIONS TO BE ADDED)
    - save fixed HyDAMO data
    - (validate again and show summary of results/improvements?)
    - (ask user if they want continue with next layer? Or do manual fixes? Or again automatic fixes?)

    """

    def __init__(self, hydamo_file_path: Path, 
                 validation_result_path: Path, 
                 db_layer_mapping: dict,
                 project_folder: Path = None, 
                 logger) -> None:
        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        #NOTE: maybe better to pass project folder and get paths from there?
        self.project_folder = project folder
        self.hydamo_file = gpd.read_file(hydamo_file_path)
        self.validation_result = gpd.read_file(validation_result_path)
        self.db_layer_mapping = db_layer_mapping

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

    def specific_hydamo_layer_validation(self, layer: str) -> bool:
        "validation of layer which will be used before the fixing process starts"
        # check which layers have to be included for validation, since some depend on multiple layers in the validation
        needed_layers = [layer]
        table_config = self.db_layer_mapping.get(layer)
        if table_config.get("required_sub_table"):
            needed_layers.append(table_config.get("required_sub_table"))

        if table_config.get("required_sub2_table"):
            needed_layers.append(table_config.get("required_sub2_table"))

        HyDAMO_needed_layers = self.hydamo_file[self.hydamo_file["layer"].isin(needed_layers)]

        if HyDAMO_needed_layers.empty:
            self.logger.error(f"No features found in HyDAMO for layer(s): {needed_layers}")
            pass
        else:
            self.logger.info(f"Found {len(HyDAMO_needed_layers)} features in HyDAMO for layer(s): {needed_layers}")

            # save these needed layers to temporary file
            temp_hydamo_path = self.hydamo_file_path.parent / f"temp_{layer}_for_validation.geojson"
            HyDAMO_needed_layers.to_file(temp_hydamo_path, driver="GPKG")

            # start validation of specific layer(s)

            # TODO: nu hardcoded hier, maar moet missschien nog anders
            validation_directory_path = self.project_folder / "01_source_data" / "hydamo_validation"
            validation_rules_json_path = validation_directory_path / "validationrules.json"
            coverage_location = validation_directory_path / "dtm"

            result_summary = validate_hydamo(
                hydamo_file_path=temp_hydamo_path,
                validation_rules_json_path=validation_rules_json_path,
                validation_directory_path=validation_directory_path,
                coverages_dict={"AHN": coverage_location},
                output_types=["geopackage", "csv", "geojson"],
            )

    def select_features_to_fix(self, layer: str, list_features: list) -> gpd.GeoDataFrame:
        """Select features from HyDAMO that need to be fixed.

        Args:
            layer (str): The layer from which to select features.
            list_features (list): List of feature IDs to select.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the selected features.
        """
        selected_features = self.hydamo_file[
            (self.hydamo_file["layer"] == layer) & (self.hydamo_file["id"].isin(list_features))
        ]
        return selected_features
    
    def fix_features(self, layer: str, list_features: list, validatiefix_name: str) -> None:
        """Apply fixes to the selected features based on predefined rules.

        Args:
            layer (str): The layer of the features to be fixed.
            list_features (list): List of feature IDs to be fixed.
            validatiefix_name (str): The name of the validation fix to apply.
        """
        func = getattr(functions_HyDAMO_Fixer, validatiefix_name, None)

        if not func:
            self.logger.error(f"Validation fix function '{validatiefix_name}' not found.")
            return

        # Apply the fix function to the selected features
        self.logger.info(f"Applying the fix {validatiefix_name} to {len(list_features)} features in layer {layer}.")
        self.hydamo_file = func(self.hydamo_file, layer, list_features, self.logger)
        
    def run(self) -> None:
        """Run the HyDAMO fixing process."""
        for obj in self.fix_config["objects"]:
            layer = obj["object"]
            self.logger.info(f"Processing layer: {layer}")

            fix_needed, list_features = self._check_if_fix_needed(layer)
            if fix_needed:
                self.logger.info(f"Fix needed for layer {layer}. Features to fix: {list_features}")

                # Select features to fix
                features_to_fix = self.select_features_to_fix(layer, list_features)
                if features_to_fix.empty:
                    self.logger.warning(f"No features found to fix in layer {layer}.")
                    continue

                # Show features to user (outside this class, e.g., in QGIS)

                # For now, we assume user confirms automatic fixing
                for rule in obj["validation_rule"]:
                    fixes = rule['fixes']
                    if len(fixes) > 2:
                        #TODO: give choice to user. Which feature to fix with which function?
                        self.logger.warning(f"Multiple fixes found for rule {rule['validation_rule_id']}. For now using the first one.")
                    
                    validation_fix_name = fixes[0]['validationfix_name']
                    self.fix_features(layer, list_features, validation_fix_name)
            else:
                self.logger.info(f"No fix needed for layer {layer}.")

        # Save the fixed HyDAMO data
        fixed_hydamo_path = self.project_folder / "01_source_data" / "fixed_hydamo.geojson"
        self.hydamo_file.to_file(fixed_hydamo_path, driver="GPKG")
        self.logger.info(f"Fixed HyDAMO data saved to {fixed_hydamo_path}")


if __name__ == "__main__":
    hydamo_file_path = Path("path/to/hydamo_file.geojson")
    validation_result_path = Path("path/to/validation_result.geojson")
    fixer = HyDAMO_Fixer(hydamo_file_path, validation_result_path, logger=None)

    pass
    # TODO add example usage
